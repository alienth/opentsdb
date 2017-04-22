// This file is part of OpenTSDB.
// Copyright (C) 2011-2012  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicLong;

import com.stumbleupon.async.Deferred;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.hbase.async.KeyValue;

import net.opentsdb.stats.StatsCollector;

/**
 * "Queue" of rows to compact.
 * <p>
 * Whenever we write a data point to HBase, the row key we write to is added
 * to this queue, which is effectively a sorted set.  There is a separate
 * thread that periodically goes through the queue and look for "old rows" to
 * compact.  A row is considered "old" if the timestamp in the row key is
 * older than a certain threshold.
 * <p>
 * The compaction process consists in reading all the cells within a given row
 * and writing them back out as a single big cell.  Once that writes succeeds,
 * we delete all the individual little cells.
 * <p>
 * This process is effective because in HBase the row key is repeated for
 * every single cell.  And because there is no way to efficiently append bytes
 * at the end of a cell, we have to do this instead.
 */
final class CompactionQueue  {

  private static final Logger LOG = LoggerFactory.getLogger(CompactionQueue.class);

  private final AtomicLong duplicates_different = new AtomicLong();
  private final AtomicLong duplicates_same = new AtomicLong();
  private final AtomicLong compaction_count = new AtomicLong();

  /** The {@code TSDB} instance we belong to. */
  private final TSDB tsdb;


  /**
   * Constructor.
   * @param tsdb The TSDB we belong to.
   */
  public CompactionQueue(final TSDB tsdb) {
    // super(new Cmp(tsdb));
    this.tsdb = tsdb;
  }

  /**
   * Collects the stats and metrics tracked by this instance.
   * @param collector The collector to use.
   */
  void collectStats(final StatsCollector collector) {
    collector.record("compaction.count", compaction_count);
    collector.record("compaction.duplicates", duplicates_same, "type=identical");
    collector.record("compaction.duplicates", duplicates_different, "type=variant");
    if (!tsdb.config.enable_compactions()) {
      return;
    }
  }

  /**
   * Compacts a row into a single {@link KeyValue}.
   * @param row The row containing all the KVs to compact.
   * Must contain at least one element.
   * @return A compacted version of this row.
   */
  KeyValue compact(final ArrayList<KeyValue> row) {
    final KeyValue[] compacted = { null };
    compact(row, compacted);
    return compacted[0];
  }

  /**
   * Maintains state for a single compaction; exists to break the steps down into manageable
   * pieces without having to worry about returning multiple values and passing many parameters
   * around.
   * 
   * @since 2.1
   */
  private class Compaction {

    // The row passed in for compaction.
    private final ArrayList<KeyValue> row;

    // The compacted result, which gets returned back to the caller in some cases.
    private final KeyValue[] compacted;

    private final int nkvs;

    // keeps a list of KeyValues to be deleted
    private final List<KeyValue> to_delete;

    // heap of columns, ordered by increasing timestamp
    private PriorityQueue<ColumnDatapointIterator> heap;

    // KeyValue containing the longest qualifier for the datapoint, used to optimize
    // checking if the compacted qualifier already exists.
    private KeyValue longest;
    
    // the latest append column. If set then we don't want to re-write the row
    // and if we only had a single column with a single value, we return this.
    private KeyValue last_append_column;

    public Compaction(ArrayList<KeyValue> row, KeyValue[] compacted) {
      nkvs = row.size();
      this.row = row;
      this.compacted = compacted;
      to_delete = new ArrayList<KeyValue>(nkvs);
    }

    /**
     * Check if there are no merges required.  This will be the case when:
     * <ul>
     *  <li>there are no columns in the heap</li>
     *  <li>there is only one single-valued column</li>
     * </ul>
     *
     * @return true if we know no additional work is required
     */
    private boolean noMerges() {
      switch (heap.size()) {
        case 0:
          // no data points, nothing to do
          return true;
        case 1:
          return true;
        default:
          // more than one column, need to merge
          return false;
      }
    }

    /**
     * Perform the compaction.
     *
     * @return A {@link Deferred} if the compaction processed required a write
     * to HBase, otherwise {@code null}.
     */
    public Deferred<Object> compact() {
      // no columns in row, nothing to do
      if (nkvs == 0) {
        return null;
      }

      // go through all the columns, process annotations, and
      heap = new PriorityQueue<ColumnDatapointIterator>(nkvs);
      int tot_values = buildHeapProcessAnnotations();

      // if there are no datapoints or only one, we are done
      if (noMerges()) {
        // return the single non-annotation entry if requested
        if (compacted != null && heap.size() == 1) {
          compacted[0] = findFirstDatapointColumn();
        }
        return null;
      }

      // merge the datapoints, ordered by timestamp and removing duplicates
      final ByteBufferList compacted_qual = new ByteBufferList(tot_values);
      final ByteBufferList compacted_val = new ByteBufferList(tot_values);
      compaction_count.incrementAndGet();
      mergeDatapoints(compacted_qual, compacted_val);

      // if we wound up with no data in the compacted column, we are done
      if (compacted_qual.segmentCount() == 0) {
        return null;
      }

      // build the compacted columns
      final KeyValue compact = buildCompactedColumn(compacted_qual, compacted_val);

      if (compacted != null) {  // Caller is interested in the compacted form.
        compacted[0] = compact;
      }
      return null;
    }

    /**
     * Find the first datapoint column in a row. It may be an appended column
     *
     * @return the first found datapoint column in the row, or null if none
     */
    private KeyValue findFirstDatapointColumn() {
      if (last_append_column != null) {
        return last_append_column;
      }
      for (final KeyValue kv : row) {
        if (isDatapoint(kv)) {
          return kv;
        }
      }
      return null;
    }

    /**
     * Build a heap of columns containing datapoints.  Assumes that non-datapoint columns are
     * never merged.  Adds datapoint columns to the list of rows to be deleted.
     *
     * @return an estimate of the number of total values present, which may be high
     */
    private int buildHeapProcessAnnotations() {
      int tot_values = 0;
      for (final KeyValue kv : row) {
        byte[] qual = kv.qualifier();
        int len = qual.length;
        final int entry_size = 4;
        tot_values += (len + entry_size - 1) / entry_size;
        if (longest == null || longest.qualifier().length < kv.qualifier().length) {
          longest = kv;
        }
        ColumnDatapointIterator col = new ColumnDatapointIterator(kv);
        if (col.hasMoreData()) {
          heap.add(col);
        }
        to_delete.add(kv);
      }
      return tot_values;
    }

    /**
     * Process datapoints from the heap in order, merging into a sorted list.  Handles duplicates
     * by keeping the most recent (based on HBase column timestamps; if duplicates in the )
     *
     * @param compacted_qual qualifiers for sorted datapoints
     * @param compacted_val values for sorted datapoints
     */
    private void mergeDatapoints(ByteBufferList compacted_qual, 
        ByteBufferList compacted_val) {
      long prevTs = -1;
      while (!heap.isEmpty()) {
        final ColumnDatapointIterator col = heap.remove();
        final long ts = col.getTimestampOffsetMs();
        if (ts == prevTs) {
          // check to see if it is a complete duplicate, or if the value changed
          // TODO - Address duplicates with varying data types.
          final byte[] existingVal = compacted_val.getLastSegment();
          final byte[] discardedVal = col.getCopyOfCurrentValue();
          if (!Arrays.equals(existingVal, discardedVal)) {
            duplicates_different.incrementAndGet();
            if (!tsdb.config.fix_duplicates()) {
              throw new IllegalDataException("Duplicate timestamp for key="
                  + Arrays.toString(row.get(0).key()) + ", ms_offset=" + ts + ", older="
                  + Arrays.toString(existingVal) + ", newer=" + Arrays.toString(discardedVal)
                  + "; set tsd.storage.fix_duplicates=true to fix automatically or run Fsck");
            }
            LOG.warn("Duplicate timestamp for key=" + Arrays.toString(row.get(0).key())
                + ", ms_offset=" + ts + ", kept=" + Arrays.toString(existingVal) + ", discarded="
                + Arrays.toString(discardedVal));
          } else {
            duplicates_same.incrementAndGet();
          }
        } else {
          prevTs = ts;
          col.writeToBuffers(compacted_qual, compacted_val);
        }
        if (col.advance()) {
          // there is still more data in this column, so add it back to the heap
          heap.add(col);
        }
      }
    }

    /**
     * Build the compacted column from the list of byte buffers that were
     * merged together.
     *
     * @param compacted_qual list of merged qualifiers
     * @param compacted_val list of merged values
     *
     * @return {@link KeyValue} instance for the compacted column
     */
    private KeyValue buildCompactedColumn(ByteBufferList compacted_qual,
        ByteBufferList compacted_val) {
      final byte[] cq = compacted_qual.toBytes(0);
      final byte[] cv = compacted_val.toBytes(0);

      final KeyValue first = row.get(0);
      return new KeyValue(first.key(), first.family(), cq, cv);
    }
  }

  /**
   * Check if a particular column is a datapoint column (as opposed to annotation or other
   * extended formats).
   *
   * @param kv column to check
   * @return true if the column represents one or more datapoint
   */
  protected static boolean isDatapoint(KeyValue kv) {
    return (kv.qualifier().length & 1) == 0;
  }

  /**
   * Compacts a row into a single {@link KeyValue}.
   * <p>
   * If the {@code row} is empty, this function does literally nothing.
   * If {@code compacted} is not {@code null}, then the compacted form of this
   * {@code row} will be stored in {@code compacted[0]}.  Obviously, if the
   * {@code row} contains a single cell, then that cell is the compacted form.
   * Otherwise the compaction process takes places.
   * @param row The row containing all the KVs to compact.  Must be non-null.
   * @param compacted If non-null, the first item in the array will be set to
   * a {@link KeyValue} containing the compacted form of this row.
   * If non-null, we will also not write the compacted form back to HBase
   * unless the timestamp in the row key is old enough.
   * @param annotations supplied list which will have all encountered
   * annotations added to it.
   * @return A {@link Deferred} if the compaction processed required a write
   * to HBase, otherwise {@code null}.
   */
  Deferred<Object> compact(final ArrayList<KeyValue> row,
      final KeyValue[] compacted) {
    return new Compaction(row, compacted).compact();
  }

  static final long serialVersionUID = 1307386642;

  // /**
  //  * Helper to sort the byte arrays in the compaction queue.
  //  * <p>
  //  * This comparator sorts things by timestamp first, this way we can find
  //  * all rows of the same age at once.
  //  */
  // private static final class Cmp implements Comparator<byte[]> {

  //   /** The position with which the timestamp of metric starts.  */
  //   private final short timestamp_pos;

  //   public Cmp(final TSDB tsdb) {
  //     timestamp_pos = (short) (Const.SALT_WIDTH() + tsdb.metrics.width());
  //   }

  //   @Override
  //   public int compare(final byte[] a, final byte[] b) {
  //     final int c = Bytes.memcmp(a, b, timestamp_pos, Const.TIMESTAMP_BYTES);
  //     // If the timestamps are equal, sort according to the entire row key.
  //     return c != 0 ? c : Bytes.memcmp(a, b);
  //   }
  // }
}

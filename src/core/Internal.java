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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.hbase.async.Bytes;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;

import com.stumbleupon.async.Callback;

/**
 * <strong>This class is not part of the public API.</strong>
 * <p><pre>
 * ,____________________________,
 * | This class is reserved for |
 * | OpenTSDB's internal usage! |
 * `----------------------------'
 *       \                   / \  //\
 *        \    |\___/|      /   \//  \\
 *             /0  0  \__  /    //  | \ \
 *            /     /  \/_/    //   |  \  \
 *            @_^_@'/   \/_   //    |   \   \
 *            //_^_/     \/_ //     |    \    \
 *         ( //) |        \///      |     \     \
 *       ( / /) _|_ /   )  //       |      \     _\
 *     ( // /) '/,_ _ _/  ( ; -.    |    _ _\.-~        .-~~~^-.
 *   (( / / )) ,-{        _      `-.|.-~-.           .~         `.
 *  (( // / ))  '/\      /                 ~-. _ .-~      .-~^-.  \
 *  (( /// ))      `.   {            }                   /      \  \
 *   (( / ))     .----~-.\        \-'                 .~         \  `. \^-.
 *              ///.----../        \             _ -~             `.  ^-`  ^-_
 *                ///-._ _ _ _ _ _ _}^ - - - - ~                     ~-- ,.-~
 *                                                                   /.-~
 *              You've been warned by the dragon!
 * </pre><p>
 * This class is reserved for OpenTSDB's own internal usage only.  If you use
 * anything from this package outside of OpenTSDB, a dragon will spontaneously
 * appear and eat you.  You've been warned.
 * <p>
 * This class only exists because Java's packaging system is annoying as the
 * "package-private" accessibility level only applies to the current package
 * but not its sub-packages,  and because Java doesn't have fine-grained API
 * visibility mechanism such as that of Scala or C++.
 * <p>
 * This package provides access into internal methods for higher-level
 * packages, for the sake of reducing code duplication and (ab)use of
 * reflection.
 */
public final class Internal {

  /** @see Const#FLAG_BITS  */
  public static final short FLAG_BITS = Const.FLAG_BITS;

  /** @see Const#LENGTH_MASK  */
  public static final short LENGTH_MASK = Const.LENGTH_MASK;

  /** @see Const#FLAGS_MASK  */
  public static final short FLAGS_MASK = Const.FLAGS_MASK;

  private Internal() {
    // Can't instantiate.
  }

  /** @see TsdbQuery#getScanner */
  public static Scanner getScanner(final Query query) {
    return ((TsdbQuery) query).getScanner();
  }

  /** Returns a set of scanners, one for each bucket if salted, or one scanner
   * if salting is disabled. 
   * @see TsdbQuery#getScanner() */
  // TODO - don't call this
  public static List<Scanner> getScanners(final Query query) {
    final List<Scanner> scanners = new ArrayList<Scanner>(1);
    scanners.add(((TsdbQuery) query).getScanner());
    return scanners;
  }
  
  /** @see RowKey#metricName */
  public static String metricName(final byte[] key) {
    return RowKey.metricName(key);
  }

  /** Extracts the timestamp from a row key.  */
  public static long baseTime(final byte[] row) {
    return RowKey.baseTime(row);
  }
  
  /** @return the time normalized to an hour boundary in epoch seconds */
  public static long baseTime(final long timestamp) {
    if ((timestamp & Const.SECOND_MASK) != 0) {
      // drop the ms timestamp to seconds to calculate the base timestamp
      return ((timestamp / 1000) - 
          ((timestamp / 1000) % Const.MAX_TIMESPAN));
    } else {
      return (timestamp - (timestamp % Const.MAX_TIMESPAN));
    }
  }
  
  /** @see Tags#getTags */
  public static Map<String, String> getTags(final TSDB tsdb, final byte[] row) {
    return Tags.getTags(tsdb, row);
  }

  /** @see RowSeq#extractIntegerValue */
  public static long extractIntegerValue(final byte[] values,
                                         final int value_idx,
                                         final byte flags) {
    return RowSeq.extractIntegerValue(values, value_idx, flags);
  }

  /** @see RowSeq#extractFloatingPointValue */
  public static double extractFloatingPointValue(final byte[] values,
                                                 final int value_idx,
                                                 final byte flags) {
    return RowSeq.extractFloatingPointValue(values, value_idx, flags);
  }

  /**
   * Extracts a Cell from a single data point, fixing potential errors with
   * the qualifier flags
   * @param column The column to parse
   * @return A Cell if successful, null if the column did not contain a data
   * point (i.e. it was meta data) or failed to parse
   * @throws IllegalDataException  if the qualifier was not 2 bytes long or
   * it wasn't a millisecond qualifier
   * @since 2.0
   */
  public static Cell parseSingleValue(final KeyValue column) {
    if (column.qualifier().length == 4) {
      final ArrayList<KeyValue> row = new ArrayList<KeyValue>(1);
      row.add(column);
      final ArrayList<Cell> cells = extractDataPoints(row, 1);
      if (cells.isEmpty()) {
        return null;
      }
      return cells.get(0);
    }
    throw new IllegalDataException (
        "Qualifier does not appear to be a single data point: " + column);
  }
  
  /**
   * Extracts the data points from a single column.
   * While it's meant for use on a compacted column, you can pass any other type
   * of column and it will be returned. If the column represents a data point, 
   * a single cell will be returned. If the column contains an annotation or
   * other object, the result will be an empty array list. Compacted columns
   * will be split into individual data points.
   * <b>Note:</b> This method does not account for duplicate timestamps in
   * qualifiers.
   * @param column The column to parse
   * @return An array list of data point {@link Cell} objects. The list may be 
   * empty if the column did not contain a data point.
   * @throws IllegalDataException if one of the cells cannot be read because
   * it's corrupted or in a format we don't understand.
   * @since 2.0
   */
  public static ArrayList<Cell> extractDataPoints(final KeyValue column) {
    final ArrayList<KeyValue> row = new ArrayList<KeyValue>(1);
    row.add(column);
    return extractDataPoints(row, column.qualifier().length / 2);
  }
  
  /**
   * Breaks down all the values in a row into individual {@link Cell}s sorted on
   * the qualifier. Columns with non data-point data will be discarded.
   * <b>Note:</b> This method does not account for duplicate timestamps in
   * qualifiers.
   * @param row An array of data row columns to parse
   * @param estimated_nvalues Estimate of the number of values to compact.
   * Used to pre-allocate a collection of the right size, so it's better to
   * overshoot a bit to avoid re-allocations.
   * @return An array list of data point {@link Cell} objects. The list may be 
   * empty if the row did not contain a data point.
   * @throws IllegalDataException if one of the cells cannot be read because
   * it's corrupted or in a format we don't understand.
   * @since 2.0
   */
  public static ArrayList<Cell> extractDataPoints(final ArrayList<KeyValue> row,
      final int estimated_nvalues) {
    final ArrayList<Cell> cells = new ArrayList<Cell>(estimated_nvalues);
    for (final KeyValue kv : row) {
      final byte[] qual = kv.qualifier();
      final int len = qual.length;
      final byte[] val = kv.value();
      
      if (len % 2 != 0) {
        // skip a non data point column
        continue;
      } else if (len == 4) {  // Single-value cell.
        final Cell cell = new Cell(qual, val);
        cells.add(cell);
        continue;
      }
      
      // Now break it down into Cells.
      int val_idx = 0;
      try {
        for (int i = 0; i < len; i += 4) {
          final byte[] q = extractQualifier(qual, i);
          final int vlen = getValueLengthFromQualifier(qual, i);
          
          final byte[] v = new byte[vlen];
          System.arraycopy(val, val_idx, v, 0, vlen);
          val_idx += vlen;
          final Cell cell = new Cell(q, v);
          cells.add(cell);
        }
      } catch (ArrayIndexOutOfBoundsException e) {
        throw new IllegalDataException("Corrupted value: couldn't break down"
            + " into individual values (consumed " + val_idx + " bytes, but was"
            + " expecting to consume " + (val.length - 1) + "): " + kv
            + ", cells so far: " + cells);
      }
      
      // Check we consumed all the bytes of the value.  Remember the last byte
      // is metadata, so it's normal that we didn't consume it.
      if (val_idx != val.length - 1) {
        throw new IllegalDataException("Corrupted value: couldn't break down"
          + " into individual values (consumed " + val_idx + " bytes, but was"
          + " expecting to consume " + (val.length - 1) + "): " + kv
          + ", cells so far: " + cells);
      }
    }
    
    Collections.sort(cells);
    return cells;
  }
  
  /**
   * Represents a single data point in a row. Compacted columns may not be
   * stored in a cell.
   * <p>
   * This is simply a glorified pair of (qualifier, value) that's comparable.
   * Only the qualifier is used to make comparisons.
   * @since 2.0
   */
  public static final class Cell implements Comparable<Cell> {
    /** Tombstone used as a helper during the complex compaction.  */
    public static final Cell SKIP = new Cell(null, null);

    final byte[] qualifier;
    final byte[] value;

    /**
     * Constructor that sets the cell
     * @param qualifier Qualifier to store
     * @param value Value to store
     */
    public Cell(final byte[] qualifier, final byte[] value) {
      this.qualifier = qualifier;
      this.value = value;
    }

    /** Compares the qualifiers of two cells */
    public int compareTo(final Cell other) {
      return compareQualifiers(qualifier, 0, other.qualifier, 0);
    }

    /** Determines if the cells are equal based on their qualifier */
    @Override
    public boolean equals(final Object o) {
      return o != null && o instanceof Cell && compareTo((Cell) o) == 0;
    }

    /** @return a hash code based on the qualifier bytes */
    @Override
    public int hashCode() {
      return Arrays.hashCode(qualifier);
    }

    /** Prints the raw data of the qualifier and value */
    @Override
    public String toString() {
      return "Cell(" + Arrays.toString(qualifier)
        + ", " + Arrays.toString(value) + ')';
    }
  
    /** @return the qualifier byte array */
    public byte[] qualifier() {
      return qualifier;
    }
    
    /** @return the value byte array */
    public byte[] value() {
      return value;
    }
    
    /**
     * Returns the value of the cell as a Number for passing to a StringBuffer
     * @return The numeric value of the cell
     * @throws IllegalDataException if the value is invalid
     */
    public Number parseValue() {
      if (isInteger()) {
        return extractIntegerValue(value, 0, 
            (byte)getFlagsFromQualifier(qualifier));
      } else {
        return extractFloatingPointValue(value, 0, 
            (byte)getFlagsFromQualifier(qualifier));
      }      
    }

    /**
     * Returns the Unix epoch timestamp in milliseconds
     * @param base_time Row key base time to add the offset to
     * @return Unix epoch timestamp in milliseconds
     */
    public long timestamp(final long base_time) {
      return getTimestampFromQualifier(qualifier, base_time);
    }
    
    /**
     * Returns the timestamp as stored in HBase for the cell, i.e. in seconds
     * or milliseconds
     * @param base_time Row key base time to add the offset to
     * @return Unix epoch timestamp
     */
    public long absoluteTimestamp(final long base_time) {
      final long timestamp = getTimestampFromQualifier(qualifier, base_time);
      return timestamp / 1000;
    }
    
    /** @return Whether or not the value is an integer */
    public boolean isInteger() {
      return (Internal.getFlagsFromQualifier(qualifier) & 
          Const.FLAG_FLOAT) == 0x0;
    }
  }
  
  /**
   * Helper to sort a row with a mixture of millisecond and second data points.
   * In such a case, we convert all of the seconds into millisecond timestamps,
   * then perform the comparison.
   * <b>Note:</b> You must filter out all but the second, millisecond and
   * compacted rows
   * @since 2.0
   */
  public static final class KeyValueComparator implements Comparator<KeyValue> {
    
    /**
     * Compares the qualifiers from two key values
     * @param a The first kv
     * @param b The second kv
     * @return 0 if they have the same timestamp, -1 if a is less than b, 1 
     * otherwise.
     */
    public int compare(final KeyValue a, final KeyValue b) {
      return compareQualifiers(a.qualifier(), 0, b.qualifier(), 0);
    }
    
  }

  /**
   * Callback used to fetch only the last data point from a row returned as the
   * result of a GetRequest. Non data points will be parsed out and the
   * resulting time and value stored in an IncomingDataPoint if found. If no 
   * valid data was found, a null is returned.
   * @since 2.0
   */
  public static class GetLastDataPointCB implements Callback<IncomingDataPoint, 
    ArrayList<KeyValue>> {
    final TSDB tsdb;
    
    public GetLastDataPointCB(final TSDB tsdb) {
      this.tsdb = tsdb;
    }
    
    /**
     * Returns the last data point from a data row in the TSDB table.
     * @param row The row from HBase
     * @return null if no data was found, a data point if one was
     */
    public IncomingDataPoint call(final ArrayList<KeyValue> row) 
      throws Exception {
      if (row == null || row.size() < 1) {
        return null;
      }
      
      // check to see if the cells array is empty as it will flush out all
      // non-data points.
      final ArrayList<Cell> cells = extractDataPoints(row, row.size());
      if (cells.isEmpty()) {
        return null;
      }
      final Cell cell = cells.get(cells.size() - 1);
      final IncomingDataPoint dp = new IncomingDataPoint();
      final long base_time = baseTime(row.get(0).key());
      dp.setTimestamp(getTimestampFromQualifier(cell.qualifier(), base_time));
      dp.setValue(cell.parseValue().toString());
      return dp;
    }
  }
  
  /**
   * Compares two data point byte arrays with offsets.
   * Can be used on:
   * <ul><li>Single data point columns</li>
   * <li>Compacted columns</li></ul>
   * <b>Warning:</b> Does not work on Annotation or other columns
   * @param a The first byte array to compare
   * @param offset_a An offset for a
   * @param b The second byte array
   * @param offset_b An offset for b
   * @return 0 if they have the same timestamp, -1 if a is less than b, 1 
   * otherwise.
   * @since 2.0
   */
  public static int compareQualifiers(final byte[] a, final int offset_a, 
      final byte[] b, final int offset_b) {
    final long left = Internal.getOffsetFromQualifier(a, offset_a);
    final long right = Internal.getOffsetFromQualifier(b, offset_b);
    if (left == right) {
      return 0;
    }
    return (left < right) ? -1 : 1;
  }
  
  /**
   * Returns the offset in milliseconds from the row base timestamp from a data
   * point qualifier
   * @param qualifier The qualifier to parse
   * @return The offset in milliseconds from the base time
   * @throws IllegalArgumentException if the qualifier is null or empty
   * @since 2.0
   */
  public static int getOffsetFromQualifier(final byte[] qualifier) {
    return getOffsetFromQualifier(qualifier, 0);
  }
  
  /**
   * Returns the offset in milliseconds from the row base timestamp from a data
   * point qualifier at the given offset (for compacted columns)
   * @param qualifier The qualifier to parse
   * @param offset An offset within the byte array
   * @return The offset in milliseconds from the base time
   * @throws IllegalDataException if the qualifier is null or the offset falls 
   * outside of the qualifier array
   * @since 2.0
   */
  // TODO this needs to become offset in seconds, since a 28-day ms offset won't fit in an int.
  public static int getOffsetFromQualifier(final byte[] qualifier, 
      final int offset) {
    validateQualifier(qualifier, offset);
    final int seconds = (Bytes.getUnsignedShort(qualifier, offset) & 0xFFFFFFC0) 
      >>> Const.FLAG_BITS;
    return seconds * 1000;
  }
  
  /**
   * Returns the length of the value, in bytes, parsed from the qualifier
   * @param qualifier The qualifier to parse
   * @return The length of the value in bytes, from 1 to 8.
   * @throws IllegalArgumentException if the qualifier is null or empty
   * @since 2.0
   */
  public static byte getValueLengthFromQualifier(final byte[] qualifier) {
    return getValueLengthFromQualifier(qualifier, 0);
  }
  
  /**
   * Returns the length of the value, in bytes, parsed from the qualifier
   * @param qualifier The qualifier to parse
   * @param offset An offset within the byte array
   * @return The length of the value in bytes, from 1 to 8.
   * @throws IllegalArgumentException if the qualifier is null or the offset falls
   * outside of the qualifier array
   * @since 2.0
   */
  public static byte getValueLengthFromQualifier(final byte[] qualifier, 
      final int offset) {
    validateQualifier(qualifier, offset);    
    short length;
    length = (short) (qualifier[offset + 3] & Internal.LENGTH_MASK); 
    return (byte) (length + 1);
  }

  /**
   * Returns the length, in bytes, of the qualifier: 2 or 4 bytes
   * @param qualifier The qualifier to parse
   * @return The length of the qualifier in bytes
   * @throws IllegalArgumentException if the qualifier is null or empty
   * @since 2.0
   */
  public static short getQualifierLength(final byte[] qualifier) {
    return getQualifierLength(qualifier, 0);
  }
  
  /**
   * Returns the length, in bytes, of the qualifier: 2 or 4 bytes
   * @param qualifier The qualifier to parse
   * @param offset An offset within the byte array
   * @return The length of the qualifier in bytes
   * @throws IllegalArgumentException if the qualifier is null or the offset falls
   * outside of the qualifier array
   * @since 2.0
   */
  public static short getQualifierLength(final byte[] qualifier, 
      final int offset) {
    validateQualifier(qualifier, offset);    
    return Const.QUALIFIER_BYTES;
  }
  
  /**
   * Returns the absolute timestamp of a data point qualifier in milliseconds
   * @param qualifier The qualifier to parse
   * @param base_time The base time, in seconds, from the row key
   * @return The absolute timestamp in milliseconds
   * @throws IllegalArgumentException if the qualifier is null or empty
   * @since 2.0
   */
  public static long getTimestampFromQualifier(final byte[] qualifier, 
      final long base_time) {
    return (base_time * 1000) + getOffsetFromQualifier(qualifier);
  }
  
  /**
   * Returns the absolute timestamp of a data point qualifier in milliseconds
   * @param qualifier The qualifier to parse
   * @param base_time The base time, in seconds, from the row key
   * @param offset An offset within the byte array
   * @return The absolute timestamp in milliseconds
   * @throws IllegalArgumentException if the qualifier is null or the offset falls
   * outside of the qualifier array
   * @since 2.0
   */
  public static long getTimestampFromQualifier(final byte[] qualifier, 
      final long base_time, final int offset) {
    return (base_time * 1000) + getOffsetFromQualifier(qualifier, offset);
  }

  /**
   * Parses the flag bits from the qualifier
   * @param qualifier The qualifier to parse
   * @return A short representing the last 4 bits of the qualifier
   * @throws IllegalArgumentException if the qualifier is null or empty
   * @since 2.0
   */
  public static short getFlagsFromQualifier(final byte[] qualifier) {
    return getFlagsFromQualifier(qualifier, 0);
  }
  
  /**
   * Parses the flag bits from the qualifier
   * @param qualifier The qualifier to parse
   * @param offset An offset within the byte array
   * @return A short representing the last 4 bits of the qualifier
   * @throws IllegalArgumentException if the qualifier is null or the offset falls
   * outside of the qualifier array
   * @since 2.0
   */
  public static short getFlagsFromQualifier(final byte[] qualifier, 
      final int offset) {
    validateQualifier(qualifier, offset);
    return (short) (qualifier[offset + 3] & Internal.FLAGS_MASK); 
  }

  /**
   * Parses the qualifier to determine if the data is a floating point value.
   * 4 bytes == Float, 8 bytes == Double
   * @param qualifier The qualifier to parse
   * @return True if the encoded data is a floating point value
   * @throws IllegalArgumentException if the qualifier is null or the offset falls
   * outside of the qualifier array
   * @since 2.1
   */
  public static boolean isFloat(final byte[] qualifier) {
    return isFloat(qualifier, 0);
  }

  /**
   * Parses the qualifier to determine if the data is a floating point value.
   * 4 bytes == Float, 8 bytes == Double
   * @param qualifier The qualifier to parse
   * @param offset An offset within the byte array
   * @return True if the encoded data is a floating point value
   * @throws IllegalArgumentException if the qualifier is null or the offset falls
   * outside of the qualifier array
   * @since 2.1
   */
  public static boolean isFloat(final byte[] qualifier, final int offset) {
    validateQualifier(qualifier, offset);
    return (qualifier[offset + 3] & Const.FLAG_FLOAT) == Const.FLAG_FLOAT; 
  }
  
  /**
   * Extracts the 4 byte qualifier from a compacted byte array
   * @param qualifier The qualifier to parse
   * @param offset An offset within the byte array
   * @return A byte array with only the requested qualifier
   * @throws IllegalArgumentException if the qualifier is null or the offset falls
   * outside of the qualifier array
   * @since 2.0
   */
  public static byte[] extractQualifier(final byte[] qualifier, 
      final int offset) {
    validateQualifier(qualifier, offset);
    return new byte[] { qualifier[offset], qualifier[offset + 1],
       qualifier[offset + 2], qualifier[offset + 3] };
  }
  
  /**
   * Returns a 4 byte qualifier based on the timestamp and the flags.
   * @param timestamp A Unix epoch timestamp in seconds or milliseconds
   * @param flags Flags to set on the qualifier (length &| float)
   * @return A 2 or 4 byte qualifier for storage in column or compacted column
   * @since 2.0
   */
  public static byte[] buildQualifier(long timestamp, final short flags) {
    final long base_time;
    if ((timestamp & Const.SECOND_MASK) != 0) {
      timestamp = timestamp / 1000;
    }
    base_time = (timestamp - (timestamp % Const.MAX_TIMESPAN));
    final int qual = (int) ((timestamp - base_time) << Const.FLAG_BITS
        | flags);
    return Bytes.fromInt(qual);
  }

  /**
   * Checks the qualifier to verify that it has data and that the offset is
   * within bounds
   * @param qualifier The qualifier to validate
   * @param offset An optional offset
   * @throws IllegalDataException if the qualifier is null or the offset falls 
   * outside of the qualifier array
   * @since 2.0
   */
  private static void validateQualifier(final byte[] qualifier, 
      final int offset) {
    if (offset < 0 || offset >= qualifier.length - 1) {
      throw new IllegalDataException("Offset of [" + offset + 
          "] is out of bounds for the qualifier length of [" + 
          qualifier.length + "]");
    }
  }

  /**
   * Simple helper to calculate the max value for any width of long from 0 to 8
   * bytes. 
   * @param width The width of the byte array we're comparing
   * @return The maximum unsigned integer value on {@link width} bytes. Note:
   * If you ask for 8 bytes, it will return the max signed value. This is due
   * to Java lacking unsigned integers... *sigh*.
   * @since 2.2
   */
  public static long getMaxUnsignedValueOnBytes(final int width) {
    if (width < 0 || width > 8) {
      throw new IllegalArgumentException("Width must be from 1 to 8 bytes: " 
          + width);
    }
    if (width < 8) {
      return ((long) 1 << width * Byte.SIZE) - 1;
    } else {
      return Long.MAX_VALUE;
    }
  }
}

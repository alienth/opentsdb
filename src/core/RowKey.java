// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
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

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.hbase.async.Bytes;

/** Helper functions to deal with the row key. */
final public class RowKey {

  private RowKey() {
    // Can't create instances of this utility class.
  }

  /**
   * Extracts the name of the metric ID contained in a row key.
   * @param tsdb The TSDB to use.
   * @param row The actual row key.
   * @return The name of the metric.
   * @throws NoSuchUniqueId if the UID could not resolve to a string
   */
  static String metricName(final byte[] key) {
    return getKeyField(key, metric_field);
  }

  // TODO Const this
  static short metric_field = 0;
  static short ts_field = 1;
  static short tags_field = 2;

  /** Extracts the timestamp from a row key.  */
  static long baseTime(final byte[] row) {
    return Bytes.getUnsignedInt(getKeyFieldBytes(row, ts_field));
  }
 
  private static String getKeyField(final byte[] key, int field) {
    String keyStr = new String(key, CHARSET);
    return keyStr.split(field_delim_str)[field];
  }

  private static byte[] getKeyFieldBytes(final byte[] key, int field) {
    short delimCount = 0;
    int fieldStart = 0;              // inclusive start index of the field
    int fieldStop = key.length - 1;  // exclusive end index of the field

    for (short i = 0; i < key.length; i++) {
      if (key[i] == field_delim) {
        delimCount++;
        if (delimCount == field) {
          fieldStop = i;
          break;
        } else {
          fieldStart = i;
        }
      }
    }

    return Arrays.copyOfRange(key, fieldStart, fieldStop);
  }


  /**
   * Checks a row key to determine if it contains the metric UID. If salting is
   * enabled, we skip the salt bytes.
   * @param metric The metric UID to match
   * @param row_key The row key to match on
   * @return 0 if the two arrays are identical, otherwise the difference
   * between the first two different bytes (treated as unsigned), otherwise
   * the different between their lengths.
   * @throws IndexOutOfBoundsException if either array isn't large enough.
   */
  public static int rowKeyContainsMetric(final byte[] metric, 
      final byte[] row_key) {
    for (int i = 0; i < metric.length; i++) {
      if (metric[i] != row_key[i]) {
        return (metric[i] & 0xFF) - (row_key[i] & 0xFF);  // "promote" to unsigned.
      }
    }
    return 0;
  }
  
  final static Charset CHARSET = TSDB.CHARSET;
  final static String tag_delim_str = ":";
  final static byte[] tag_delim = tag_delim_str.getBytes(CHARSET);
  final static String tag_equals_str = "=";
  final static byte[] tag_equals = tag_equals_str.getBytes(CHARSET);
  final static String field_delim_str = "\0";
  final static byte field_delim = 0x00;


  static byte[] tagsToBytes(Map<String, String> tagm) {
    int tag_size = 0;
    for (final String key : tagm.keySet()) {
      tag_size += key.getBytes(CHARSET).length + tagm.get(key).getBytes(CHARSET).length + tag_delim.length + tag_equals.length;
    }
    tag_size--; // We remove the last delimiter.

    byte[] tags = new byte[tag_size];
    short pos = 0;

    for (final String key : tagm.keySet()) {
      final byte[] tagKey = key.getBytes(CHARSET);
      final byte[] tagVal = tagm.get(key).getBytes(CHARSET);
      copyInRowKey(tags, pos, tagKey);
      pos += tagKey.length;
      copyInRowKey(tags, pos, tag_equals);
      pos += tag_equals.length;
      copyInRowKey(tags, pos, tagVal);
      pos += tagVal.length;
      if (pos < tag_size) {
        // Only put the delim if this isn't the end.
        copyInRowKey(tags, pos, tag_delim);
        pos += tagVal.length;
      }
    }
    return tags;
  }

  // TODO - Move this tags stuff to Tags.java.
  static Map<String, String> getTags(byte[] key) {
    final String tagStr = getTagString(key);
    return getTags(tagStr);
  }

  private static Map<String, String> getTags(String tagStr) {
    final Map<String, String> tagm = new HashMap<String, String>();

    for (String tag : tagStr.split(tag_delim_str)) {
      String[] kv = tag.split(tag_equals_str, 1);
      tagm.put(kv[0], kv[1]);
    }
    return tagm;
  }

  static String getTagString(byte[] key) {
    return getKeyField(key, tags_field);
  }

  static byte[] rowKeyTemplate(final TSDB tsdb, final String metricStr,
      final Map<String, String> tagm) {
    byte[] tags = tagsToBytes(tagm);
    byte[] metric = metricStr.getBytes(CHARSET);
    return rowKeyTemplate(tsdb, metric, tags);
  }

  /**
   * Returns a partially initialized row key for this metric and these tags. The
   * only thing left to fill in is the base timestamp.
   */
  static byte[] rowKeyTemplate(final TSDB tsdb, final byte[] metric,
      final byte[] tags) {
    int row_size = (metric.length +
                    1 +
                    Const.TIMESTAMP_BYTES +
                    1 +
                    tags.length);
    final byte[] row = new byte[row_size];

    short pos = (short) 0;

    // final byte[] delim = new byte[]{field_delim};

    copyInRowKey(row, pos, metric);
    pos += metric.length;
    // copyInRowKey(row, pos, delim); // delim is 00, which is what is initialized by default.
    pos += 1;

    pos += Const.TIMESTAMP_BYTES;
    // copyInRowKey(row, pos, delim); // delim is 00, which is what is initialized by default.
    pos += 1;

    copyInRowKey(row, pos, tags);

    return row;
  }

  /**
   * Copies the specified byte array at the specified offset in the row key.
   * 
   * @param row
   *          The row key into which to copy the bytes.
   * @param offset
   *          The offset in the row key to start writing at.
   * @param bytes
   *          The bytes to copy.
   */
  private static void copyInRowKey(final byte[] row, final short offset,
      final byte[] bytes) {
    System.arraycopy(bytes, 0, row, offset, bytes.length);
  }

  /**
   * Validates the given metric and tags.
   * 
   * @throws IllegalArgumentException
   *           if any of the arguments aren't valid.
   */
  static void checkMetricAndTags(final String metric,
      final Map<String, String> tags) {
    if (tags.size() <= 0) {
      throw new IllegalArgumentException("Need at least one tag (metric="
          + metric + ", tags=" + tags + ')');
    } else if (tags.size() > Const.MAX_NUM_TAGS()) {
      throw new IllegalArgumentException("Too many tags: " + tags.size()
          + " maximum allowed: " + Const.MAX_NUM_TAGS() + ", tags: " + tags);
    }

    Tags.validateString("metric name", metric);
    for (final Map.Entry<String, String> tag : tags.entrySet()) {
      Tags.validateString("tag name", tag.getKey());
      Tags.validateString("tag value", tag.getValue());
    }
  }



}

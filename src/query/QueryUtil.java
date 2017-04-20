// This file is part of OpenTSDB.
// Copyright (C) 2010-2015  The OpenTSDB Authors.
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
package net.opentsdb.query;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import net.opentsdb.core.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.query.filter.TagVWildcardFilter;

import org.hbase.async.Bytes;
import org.hbase.async.KeyRegexpFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.hbase.async.Scanner;

/**
 * A simple class with utility methods for executing queries against the storage
 * layer.
 * @since 2.2
 */
public class QueryUtil {
  private static final Logger LOG = LoggerFactory.getLogger(QueryUtil.class);
  
  /**
   * Crafts a regular expression for scanning over data table rows and filtering
   * time series that the user doesn't want.
   * @param group_bys An optional list of tag keys that we want to group on. May
   * be null.
   * @param row_key_literals An optional list of key value pairs to filter on.
   * May be null.
   * @param explicit_tags Whether or not explicit tags are enabled so that the
   * regex only picks out series with the specified tags
   * @return A regular expression string to pass to the storage layer.
   */
  public static String getRowKeyRegex(
      final String metric,
      final List<String> group_bys,
      final Map<String, String[]> row_key_literals, 
      final boolean explicit_tags,
      final List<TagVFilter> filters) {
    if (group_bys != null) {
      Collections.sort(group_bys);
    }
    for (final TagVFilter filter : filters) {
      if (filter instanceof TagVWildcardFilter) {
        TagVWildcardFilter wildcard = (TagVWildcardFilter) filter;
      }
    }

    final int metric_width = metric.getBytes(TSDB.CHARSET).length;
    final int prefix_width = metric_width + Const.TIMESTAMP_BYTES;
    final StringBuilder buf = new StringBuilder(200);

    buf.append("(?s)"  // Ensure we use the DOTALL flag.
               + "^.{")
       // ... start by skipping metric ID and timestamp.
       //  + 1 for delimiter
       .append(metric_width + 1 + Const.TIMESTAMP_BYTES)
       .append("}");

    final Iterator<Entry<String, String[]>> it = row_key_literals == null ?
        new HashMap<String, String[]>().entrySet().iterator() : row_key_literals.entrySet().iterator();
    // final Iterator<Entry<String, String[]>> it = row_key_literals == null ? 
    //     new TreeMap<String, String[]>().iterator() : row_key_literals.iterator();
    // int fuzzy_offset = Const.SALT_WIDTH() + TSDB.metrics_width();
    // if (fuzzy_mask != null) {
    //   // make sure to skip the timestamp when scanning
    //   while (fuzzy_offset < prefix_width) {
    //     fuzzy_mask[fuzzy_offset++] = 1;
    //   }
    // }
 

    // If we want to get host=ny-jharvey02
    // ^.{metric_width + timestamp_width}(?:.*?\:*)host=ny-jharvey02(?::|$).*
    //


    // Tags are delimited by ":" - see RowKey.tags_delim_str
    while(it.hasNext()) {
      Entry<String, String[]> entry = it.hasNext() ? it.next() : null;
      final boolean not_key = 
          entry.getValue() != null && entry.getValue().length == 0;
      
      // Skip any number of tags.
      if (!explicit_tags) {
        buf.append("(?:.*?:*)");
      }

      if (not_key) {
        // start the lookahead as we have a key we explicitly do not want in the
        // results
        buf.append("(?!");
      }
      // buf.append("\\Q");
      
      buf.append(entry.getKey() + "=");
      if (entry.getValue() != null && entry.getValue().length > 0) {  // Add a group_by.
        // We want specific IDs.  List them: /(AAA|BBB|CCC|..)/
        buf.append("(?:");
        for (final String value : entry.getValue()) {
          if (value == "") {
            continue;
          }
          buf.append(value);
          buf.append('|');
        }
        // Replace the pipe of the last iteration.
        buf.setCharAt(buf.length() - 1, ')');
      } else {
        buf.append(".*");  // Any value ID.
      }
      
      if (not_key) {
        // be sure to close off the look ahead
        buf.append(")");
      }
    }
    // Skip any number of tags before the end.
    if (!explicit_tags) {
      buf.append("(?::|$).*");
    }
    return buf.toString();
  }
  /**
   * Crafts a regular expression for scanning over data table rows and filtering
   * time series that the user doesn't want. Also fills in an optional fuzzy
   * mask and key as it builds the regex if configured to do so.
   * @param group_bys An optional list of tag keys that we want to group on. May
   * be null.
   * @param row_key_literals An optional list of key value pairs to filter on.
   * May be null.
   * @param explicit_tags Whether or not explicit tags are enabled so that the
   * regex only picks out series with the specified tags
   * @param fuzzy_key An optional fuzzy filter row key
   * @param fuzzy_mask An optional fuzzy filter mask
   * @return A regular expression string to pass to the storage layer.
   * @since 2.3
   */
  // public static String getRowKeyUIDRegex(
  //     final List<byte[]> group_bys, 
  //     final ByteMap<byte[][]> row_key_literals, 
  //     final boolean explicit_tags,
  //     final byte[] fuzzy_key, 
  //     final byte[] fuzzy_mask,
  //     final List<TagVFilter> filters) {
  //   if (group_bys != null) {
  //     Collections.sort(group_bys, Bytes.MEMCMP);
  //   }
  //   for (final TagVFilter filter : filters) {
  //     if (filter instanceof TagVWildcardFilter) {
  //       TagVWildcardFilter wildcard = (TagVWildcardFilter) filter;
  //     }
  //   }
  //   final int prefix_width = Const.SALT_WIDTH() + TSDB.metrics_width() + 
  //       Const.TIMESTAMP_BYTES;
  //   final short name_width = TSDB.tagk_width();
  //   final short value_width = TSDB.tagv_width();
  //   final short tagsize = (short) (name_width + value_width);
  //   // Generate a regexp for our tags.  Say we have 2 tags: { 0 0 1 0 0 2 }
  //   // and { 4 5 6 9 8 7 }, the regexp will be:
  //   // "^.{7}(?:.{6})*\\Q\000\000\001\000\000\002\\E(?:.{6})*\\Q\004\005\006\011\010\007\\E(?:.{6})*$"
  //   final StringBuilder buf = new StringBuilder(
  //       15  // "^.{N}" + "(?:.{M})*" + "$"
  //       + ((13 + tagsize) // "(?:.{M})*\\Q" + tagsize bytes + "\\E"
  //          * ((row_key_literals == null ? 0 : row_key_literals.size()) + 
  //              (group_bys == null ? 0 : group_bys.size() * 3))));
  //   // In order to avoid re-allocations, reserve a bit more w/ groups ^^^

  //   // Alright, let's build this regexp.  From the beginning...
  //   buf.append("(?s)"  // Ensure we use the DOTALL flag.
  //              + "^.{")
  //      // ... start by skipping metric ID and timestamp.
  //      .append(TSDB.metrics_width() + Const.TIMESTAMP_BYTES)
  //      .append("}");

  //   final Iterator<Entry<byte[], byte[][]>> it = row_key_literals == null ? 
  //       new ByteMap<byte[][]>().iterator() : row_key_literals.iterator();
  //   int fuzzy_offset = Const.SALT_WIDTH() + TSDB.metrics_width();
  //   if (fuzzy_mask != null) {
  //     // make sure to skip the timestamp when scanning
  //     while (fuzzy_offset < prefix_width) {
  //       fuzzy_mask[fuzzy_offset++] = 1;
  //     }
  //   }
    
  //   while(it.hasNext()) {
  //     Entry<byte[], byte[][]> entry = it.hasNext() ? it.next() : null;
  //     // TODO - This look ahead may be expensive. We need to get some data around
  //     // whether it's faster for HBase to scan with a look ahead or simply pass
  //     // the rows back to the TSD for filtering.
  //     final boolean not_key = 
  //         entry.getValue() != null && entry.getValue().length == 0;
      
  //     // Skip any number of tags.
  //     if (!explicit_tags) {
  //       buf.append("(?:.{").append(tagsize).append("})*");
  //     } else if (fuzzy_mask != null) {
  //       // TODO - see if we can figure out how to improve the fuzzy filter by
  //       // setting explicit tag values whenever we can. In testing there was
  //       // a conflict between the row key regex and fuzzy filter that prevented
  //       // results from returning properly.
  //       System.arraycopy(entry.getKey(), 0, fuzzy_key, fuzzy_offset, name_width);
  //       fuzzy_offset += name_width;
  //       for (int i = 0; i < value_width; i++) {
  //         fuzzy_mask[fuzzy_offset++] = 1;
  //       }
  //     }
  //     if (not_key) {
  //       // start the lookahead as we have a key we explicitly do not want in the
  //       // results
  //       buf.append("(?!");
  //     }
  //     buf.append("\\Q");
      
  //     addId(buf, entry.getKey(), true);
  //     if (entry.getValue() != null && entry.getValue().length > 0) {  // Add a group_by.
  //       // We want specific IDs.  List them: /(AAA|BBB|CCC|..)/
  //       buf.append("(?:");
  //       for (final byte[] value_id : entry.getValue()) {
  //         if (value_id == null) {
  //           continue;
  //         }
  //         buf.append("\\Q");
  //         addId(buf, value_id, true);
  //         buf.append('|');
  //       }
  //       // Replace the pipe of the last iteration.
  //       buf.setCharAt(buf.length() - 1, ')');
  //     } else {
  //       buf.append(".{").append(value_width).append('}');  // Any value ID.
  //     }
      
  //     if (not_key) {
  //       // be sure to close off the look ahead
  //       buf.append(")");
  //     }
  //   }
  //   // Skip any number of tags before the end.
  //   if (!explicit_tags) {
  //     buf.append("(?:.{").append(tagsize).append("})*");
  //   }
  //   buf.append("$");
  //   return buf.toString();
  // }
  
  /**
   * Sets a filter or filter list on the scanner based on whether or not the
   * query had tags it needed to match.
   * @param scanner The scanner to modify.
   * @param group_bys An optional list of tag keys that we want to group on. May
   * be null.
   * @param row_key_literals An optional list of key value pairs to filter on.
   * May be null.
   * @param end_time The end of the query time so the fuzzy filter knows when
   * to stop scanning.
   */
  public static void setDataTableScanFilter(
      final Scanner scanner, 
      final String metric,
      final List<String> group_bys, 
      final Map<String, String[]> row_key_literals,
      final boolean explicit_tags,
      final List<TagVFilter> filters,
      final int end_time) {

    // no-op
    if ((group_bys == null || group_bys.isEmpty()) 
        && (row_key_literals == null || row_key_literals.isEmpty())) {
      return;
    }

    final String regex = getRowKeyRegex(metric, group_bys, row_key_literals, explicit_tags, filters);
    final KeyRegexpFilter regex_filter = new KeyRegexpFilter(
        regex.toString(), Const.ASCII_CHARSET);
    // if (LOG.isDebugEnabled()) {
    //   LOG.debug("Regex for scanner: " + scanner + ": " + 
    //       byteRegexToString(regex));
    // }

    scanner.setFilter(regex_filter);
    return;

  }
  
  /**
   * Compiles an HBase scanner against the main data table
   * @param tsdb The TSDB with a configured HBaseClient
   * @param salt_bucket An optional salt bucket ID for salting the start/stop
   * keys.
   * @param metric The metric to scan for
   * @param start The start time stamp in seconds
   * @param stop The stop timestamp in seconds
   * @param table The table name to scan over
   * @param family The table family to scan over
   * @return A scanner ready for processing.
   */
  public static Scanner getMetricScanner(final TSDB tsdb, final int salt_bucket, 
      final String metricStr, final int start, final int stop, 
      final byte[] table, final byte[] family) {
    final byte[] metric = metricStr.getBytes(TSDB.CHARSET);
    final byte[] start_row = new byte[metric.length + 1 + Const.TIMESTAMP_BYTES];
    final byte[] end_row = new byte[metric.length + 1 + Const.TIMESTAMP_BYTES];
    
    Bytes.setInt(start_row, start, metric.length + 1);
    Bytes.setInt(end_row, stop, metric.length + 1);
    
    System.arraycopy(metric, 0, start_row, 0, metric.length);
    System.arraycopy(metric, 0, end_row, 0, metric.length);
    
    final Scanner scanner = tsdb.getClient().newScanner(table);
    scanner.setMaxNumRows(tsdb.getConfig().scanner_maxNumRows());
    scanner.setStartKey(start_row);
    scanner.setStopKey(end_row);
    scanner.setFamily(family);
    return scanner;
  }
  
  /**
   * Little helper to print out the regular expression by converting the UID
   * bytes to an array.
   * @param regexp The regex string to print to the debug log
   */
  // public static String byteRegexToString(final String regexp) {
  //   final StringBuilder buf = new StringBuilder();
  //   for (int i = 0; i < regexp.length(); i++) {
  //     if (i > 0 && regexp.charAt(i - 1) == 'Q') {
  //       if (regexp.charAt(i - 3) == '*') {
  //         // tagk
  //         byte[] tagk = new byte[TSDB.tagk_width()];
  //         for (int x = 0; x < TSDB.tagk_width(); x++) {
  //           tagk[x] = (byte)regexp.charAt(i + x);
  //         }
  //         i += TSDB.tagk_width();
  //         buf.append(Arrays.toString(tagk));
  //       } else {
  //         // tagv
  //         byte[] tagv = new byte[TSDB.tagv_width()];
  //         for (int x = 0; x < TSDB.tagv_width(); x++) {
  //           tagv[x] = (byte)regexp.charAt(i + x);
  //         }
  //         i += TSDB.tagv_width();
  //         buf.append(Arrays.toString(tagv));
  //       }
  //     } else {
  //       buf.append(regexp.charAt(i));
  //     }
  //   }
  //   return buf.toString();
  // }
}

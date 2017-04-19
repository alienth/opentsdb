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
import java.util.TimeZone;

/** Constants used in various places.  */
public final class Const {

  /** Number of bytes on which a timestamp is encoded.  */
  public static final short TIMESTAMP_BYTES = 4;

  /** Number of bytes on which a qualifier is encoded.  */
  public static final short QUALIFIER_BYTES = 4;

  /** Maximum number of tags allowed per data point.  */
  private static short MAX_NUM_TAGS = 8;
  public static short MAX_NUM_TAGS() {
    return MAX_NUM_TAGS;
  }

  /**
   * -------------- WARNING ----------------
   * Package private method to override the maximum number of tags.
   * 8 is an aggressive limit on purpose to avoid performance issues.
   * @param tags The number of tags to allow
   * @throws IllegalArgumentException if the number of tags is less
   * than 1 (OpenTSDB requires at least one tag per metric).
   */
  static void setMaxNumTags(final short tags) {
    if (tags < 1) {
      throw new IllegalArgumentException("tsd.storage.max_tags must be greater than 0");
    }
    MAX_NUM_TAGS = tags;
  }
  
  /** The default ASCII character set for encoding tables and qualifiers that
   * don't depend on user input that may be encoded with UTF.
   * Charset to use with our server-side row-filter.
   * We use this one because it preserves every possible byte unchanged.
   */
  public static final Charset ASCII_CHARSET = Charset.forName("ISO-8859-1");
  
  /** Used for metrics, tags names and tag values */
  public static final Charset UTF8_CHARSET = Charset.forName("UTF8");
  
  /** The UTC timezone used for rollup and calendar conversions */
  public static final TimeZone UTC_TZ = TimeZone.getTimeZone("UTC");

  /** Number of LSBs in time_deltas reserved for flags.  */
  public static final short FLAG_BITS = 10;
  
  /**
   * When this bit is set, the value is a floating point value.
   * Otherwise it's an integer value.
   */
  public static final short FLAG_FLOAT = 0x8;

  /** Mask to select the size of a value from the qualifier.  */
  public static final short LENGTH_MASK = 0x7;

  /** Mask to select all the FLAG_BITS.  */
  public static final short FLAGS_MASK = FLAG_FLOAT | LENGTH_MASK;
  
  /** Mask to verify a timestamp on 4 bytes in seconds */
  public static final long SECOND_MASK = 0xFFFFFFFF00000000L;
  
  /** Max time delta (in seconds) we can store in a column qualifier.  */
  public static final int MAX_TIMESPAN = 2419200;

  /**
   * Array containing the hexadecimal characters (0 to 9, A to F).
   * This array is read-only, changing its contents leads to an undefined
   * behavior.
   */
  public static final byte[] HEX = {
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
    'A', 'B', 'C', 'D', 'E', 'F'
  };

  /** 
   * Necessary for rate calculations where we may be trying to convert a 
   * large Long value to a double. Doubles can only take integers up to 2^53
   * before losing precision.
   */
  public static final long MAX_INT_IN_DOUBLE = 0xFFE0000000000000L;

  /**
   * Mnemonics for FileSystem.checkDirectory()
   */
  public static final boolean DONT_CREATE = false;
  public static final boolean CREATE_IF_NEEDED = true;
  public static final boolean MUST_BE_WRITEABLE = true;
  
}

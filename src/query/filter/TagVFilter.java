// This file is part of OpenTSDB.
// Copyright (C) 2015  The OpenTSDB Authors.
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
package net.opentsdb.query.filter;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.core.TSDB;
import net.opentsdb.utils.Pair;
import net.opentsdb.utils.PluginLoader;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.stumbleupon.async.Deferred;

/**
 * A base class for tag value filters that may execute against rows that
 * come out of a scanner to determine if we should include them in the results
 * or not. The filters should be prefixed with something to differentiate them
 * from literal values.
 * 
 * Every filter must be associated with a tag key. During scanning, each time
 * a new TSUID is encountered, the map will be passed to {@link match} for
 * matching.
 * 
 * Plugins implementing the filter must include the following:
 * 
 * - {@code public static final String FILTER_NAME;} 
 *   A short, unique name without spaces or odd characters that is used to 
 *   invoke the filter.
 * - {@code public static String description();} 
 *   A method that returns a description of what the filter does.
 * - {@code public static String examples();}
 *   A method that returns a string with some examples of how to use the filter.
 * 
 * This class also contains the list of configured filters as well as a method
 * to load filters from plugin Jars.
 * @since 2.2
 */
@JsonDeserialize(builder = TagVFilter.Builder.class)
public abstract class TagVFilter implements Comparable<TagVFilter> {
  private static final Logger LOG = LoggerFactory.getLogger(TagVFilter.class);
  
  /** A map of configured filters for use in querying */
  private static Map<String, Pair<Class<?>, Constructor<? extends TagVFilter>>>
    tagv_filter_map = new HashMap<String, 
                          Pair<Class<?>, Constructor<? extends TagVFilter>>>();
  static {
    try {
      tagv_filter_map.put(TagVLiteralOrFilter.FILTER_NAME, 
          new Pair<Class<?>, Constructor<? extends TagVFilter>>(TagVLiteralOrFilter.class,
              TagVLiteralOrFilter.class.getDeclaredConstructor(String.class, String.class)));
      tagv_filter_map.put(TagVLiteralOrFilter.TagVILiteralOrFilter.FILTER_NAME, 
          new Pair<Class<?>, Constructor<? extends TagVFilter>>(TagVLiteralOrFilter.TagVILiteralOrFilter.class,
              TagVLiteralOrFilter.TagVILiteralOrFilter.class.getDeclaredConstructor(String.class, String.class)));
      tagv_filter_map.put(TagVNotLiteralOrFilter.FILTER_NAME, 
          new Pair<Class<?>, Constructor<? extends TagVFilter>>(TagVNotLiteralOrFilter.class,
              TagVNotLiteralOrFilter.class.getDeclaredConstructor(String.class, String.class)));
      tagv_filter_map.put(TagVNotLiteralOrFilter.TagVNotILiteralOrFilter.FILTER_NAME, 
          new Pair<Class<?>, Constructor<? extends TagVFilter>>(TagVNotLiteralOrFilter.TagVNotILiteralOrFilter.class,
              TagVNotLiteralOrFilter.TagVNotILiteralOrFilter.class.getDeclaredConstructor(String.class, String.class)));
      tagv_filter_map.put(TagVRegexFilter.FILTER_NAME, 
          new Pair<Class<?>, Constructor<? extends TagVFilter>>(TagVRegexFilter.class,
              TagVRegexFilter.class.getDeclaredConstructor(String.class, String.class)));
      tagv_filter_map.put(TagVWildcardFilter.FILTER_NAME, 
          new Pair<Class<?>, Constructor<? extends TagVFilter>>(TagVWildcardFilter.class,
              TagVWildcardFilter.class.getDeclaredConstructor(String.class, String.class)));
      tagv_filter_map.put(TagVWildcardFilter.TagVIWildcardFilter.FILTER_NAME, 
          new Pair<Class<?>, Constructor<? extends TagVFilter>>(TagVWildcardFilter.TagVIWildcardFilter.class,
              TagVWildcardFilter.TagVIWildcardFilter.class.getDeclaredConstructor(String.class, String.class)));
      /* TODO - this requires either a better HBase filter or more logic on our side
      tagv_filter_map.put(TagVNotKeyFilter.FILTER_NAME, 
          new Pair<Class<?>, Constructor<? extends TagVFilter>>(TagVNotKeyFilter.class,
              TagVNotKeyFilter.class.getDeclaredConstructor(String.class, String.class)));
       */
    } catch (SecurityException e) {
      throw new RuntimeException("Failed to load a tag value filter", e);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException("Failed to load a tag value filter", e);
    }
  }

  /** The tag key this filter is associated with */
  final protected String tagk;
  
  /** The raw, unparsed filter */
  final protected String filter;
  
  protected List<String> tagvs;
  
  /** Whether or not to also group by this filter */
  @JsonProperty
  protected boolean group_by;
  
  /** A flag to indicate whether or not we need to execute a post-scan lookup */
  protected boolean post_scan = true;
  
  /**
   * Default Ctor needed for the service loader. Implementations must override
   * and set the filterName().
   */
  public TagVFilter() {
    this.tagk = null;
    this.filter = null;
  }
  
  /**
   * The ctor that validates we have a good tag key to work with
   * @param tagk The tag key to associate with this filter
   * @param filter The unparsed filter
   * @throws IlleglArgumentException if the tag was empty or null.
   */
  public TagVFilter(final String tagk, final String filter) {
    this.tagk = tagk;
    this.filter = filter;
    if (tagk == null || tagk.isEmpty()) {
      throw new IllegalArgumentException("Filter must have a tagk");
    }
  }
  
  /**
   * Looks up the tag key in the given map and determines if the filter matches
   * or not. If the tag key doesn't exist in the tag map, then the match fails.
   * @param tags The tag map to use for looking up the value for the tagk 
   * @return True if the tag value matches, false if it doesn't.
   */
  public abstract Deferred<Boolean> match(final Map<String, String> tags);
  
  /**
   * The name of this filter as used in queries. When used in URL queries the
   * value will be in parentheses, e.g. filter(<exp>)
   * The name will also be lowercased before storing it in the lookup map.
   * @return The name of the filter.
   */
  public abstract String getType();

  /**
   * A simple string of the filter settings for printing in toString() calls.
   * @return A string with the format "{settings=<val>, ...}"
   */
  @JsonIgnore
  public abstract String debugInfo();
  
  @Override
  public String toString() {
    final StringBuilder buf = new StringBuilder();
    buf.append("filter_name=")
       .append(getType())
       .append(", tagk=").append(tagk)
       .append(", group_by=").append(group_by)
       .append(", config=")
       .append(debugInfo());
    return buf.toString();
  }
  
  /**
   * Parses the tag value and determines if it's a group by, a literal or a filter.
   * @param tagk The tag key associated with this value
   * @param filter The tag value, possibly a filter
   * @return Null if the value was a group by or a literal, a valid filter object
   * if it looked to be a filter.
   * @throws IllegalArgumentException if the tag key or filter was null, empty
   * or if the filter was malformed, e.g. a bad regular expression.
   */
  public static TagVFilter getFilter(final String tagk, final String filter) {
    if (tagk == null || tagk.isEmpty()) {
      throw new IllegalArgumentException("Tagk cannot be null or empty");
    }
    if (filter == null || filter.isEmpty()) {
      throw new IllegalArgumentException("Filter cannot be null or empty");
    }
    if (filter.length() == 1 && filter.charAt(0) == '*') {
      return null; // group by filter
    }
    
    final int paren = filter.indexOf('(');
    if (paren > -1) {
      final String prefix = filter.substring(0, paren).toLowerCase();
      return new Builder().setTagk(tagk)
                          .setFilter(stripParentheses(filter))
                          .setType(prefix)
                          .build();
    } else if (filter.contains("*")) {
      // a shortcut for wildcards since we don't allow asterisks to be stored
      // in strings at this time.
      return new TagVWildcardFilter(tagk, filter, true);
    } else {
      return null; // likely a literal or unknown
    }
  }
  
  /**
   * Helper to strip parentheses from a filter name passed in over a URL
   * or JSON. E.g. "regexp(foo.*)" returns "foo.*".
   * @param filter The filter string to parse
   * @return The filter value minus the surrounding name and parens.
   */
  public static String stripParentheses(final String filter) {
    if (filter == null || filter.isEmpty()) {
      throw new IllegalArgumentException("Filter string cannot be null or empty");
    }
    if (filter.charAt(filter.length() - 1) != ')') {
      throw new IllegalArgumentException("Filter must end with a ')': " + filter);
    }
    final int start_pos = filter.indexOf('(');
    if (start_pos < 0) {
      throw new IllegalArgumentException("Filter must include a '(': " + filter);
    }
    return filter.substring(start_pos + 1, filter.length() - 1);
  }
  
  /**
   * Loads plugins from the plugin directory and loads them into the map.
   * Built-in filters don't need to go through this process.
   * @param tsdb A TSDB to use to initialize plugins
   * @throws ClassNotFoundException If we found a class that we didn't... find?
   * @throws NoSuchMethodException If the discovered plugin didn't have the
   *         proper (tagk, filter) ctor 
   * @throws InvocationTargetException if the static "initialize(tsdb)" method
   *         doesn't exist. 
   * @throws IllegalAccessException if something went really pear shaped 
   * @throws SecurityException if the JVM is really unhappy with the user
   * @throws IllegalArgumentException really shouldn't happen but you know,
   *         checked exceptions...
   */
  public static void initializeFilterMap(final TSDB tsdb) 
      throws ClassNotFoundException, NoSuchMethodException, NoSuchFieldException, 
      IllegalArgumentException, SecurityException, IllegalAccessException, 
      InvocationTargetException {
    final List<TagVFilter> filter_plugins = 
        PluginLoader.loadPlugins(TagVFilter.class);
    if (filter_plugins != null) {
      for (final TagVFilter filter : filter_plugins) {
        // validate required fields and methods
        filter.getClass().getDeclaredMethod("description");
        filter.getClass().getDeclaredMethod("examples");
        filter.getClass().getDeclaredField("FILTER_NAME");
        
        final Method initialize = filter.getClass()
            .getDeclaredMethod("initialize", TSDB.class);
        initialize.invoke(null, tsdb);
        
        final Constructor<? extends TagVFilter> ctor = 
            filter.getClass().getDeclaredConstructor(String.class, String.class);
        
        final Pair<Class<?>, Constructor<? extends TagVFilter>> existing = 
            tagv_filter_map.get(filter.getType());
        if (existing != null) {
          LOG.warn("Overloading existing filter " + 
              existing.getClass().getCanonicalName() + 
              " with new filter " + filter.getClass().getCanonicalName());
        }
        tagv_filter_map.put(filter.getType().toLowerCase(), 
            new Pair<Class<?>, Constructor<? extends TagVFilter>>(
                filter.getClass(), ctor));
        LOG.info("Successfully loaded TagVFilter plugin: " + 
            filter.getClass().getCanonicalName());
      }
      LOG.info("Loaded " + tagv_filter_map.size() + " filters");
    }
  }
  
  /**
   * Converts the tag map to a filter list. If a filter already exists for a
   * tag group by, then the duplicate is skipped. 
   * @param tags A set of tag keys and values. May be null or empty.
   * @param filters A set of filters to add the converted filters to. This may
   * not be null.
   */
  public static void tagsToFilters(final Map<String, String> tags, 
      final List<TagVFilter> filters) {
    mapToFilters(tags, filters, true);
  }

  /**
   * Converts the  map to a filter list. If a filter already exists for a
   * tag group by and we're told to process group bys, then the duplicate 
   * is skipped. 
   * @param map A set of tag keys and values. May be null or empty.
   * @param filters A set of filters to add the converted filters to. This may
   * not be null.
   * @param group_by Whether or not to set the group by flag and kick dupes
   */
  public static void mapToFilters(final Map<String, String> map, 
      final List<TagVFilter> filters, final boolean group_by) {
    if (map == null || map.isEmpty()) {
      return;
    }

    for (final Map.Entry<String, String> entry : map.entrySet()) {
      TagVFilter filter = getFilter(entry.getKey(), entry.getValue());

      if (filter == null && entry.getValue().equals("*")) {
        filter = new TagVWildcardFilter(entry.getKey(), "*", true);
      } else if (filter == null) {
        filter = new TagVLiteralOrFilter(entry.getKey(), entry.getValue());
      }
      
      if (group_by) {
        filter.setGroupBy(true);
        boolean duplicate = false;
        for (final TagVFilter existing : filters) {
          if (filter.equals(existing)) {
            LOG.debug("Skipping duplicate filter: " + existing);
            existing.setGroupBy(true);
            duplicate = true;
            break;
          }
        }
        
        if (!duplicate) {
          filters.add(filter);
        }
      } else {
        filters.add(filter);
      }
    }
  }
  
  /**
   * Runs through the loaded plugin map and dumps the names, description and
   * examples into a map to serialize via the API.
   * @return A map of filter meta data.
   */
  public static Map<String, Map<String, String>> loadedFilters() {
    final Map<String, Map<String, String>> filters = 
        new HashMap<String, Map<String, String>>(tagv_filter_map.size());
    for (final Pair<Class<?>, Constructor<? extends TagVFilter>> pair : 
        tagv_filter_map.values()) {
      final Map<String, String> filter_meta = new HashMap<String, String>(1);
      try {
        Method method = pair.getKey().getDeclaredMethod("description");
        filter_meta.put("description", (String)method.invoke(null));
        
        method = pair.getKey().getDeclaredMethod("examples");
        filter_meta.put("examples", (String)method.invoke(null));
        
        final Field filter_name = pair.getKey().getDeclaredField("FILTER_NAME");
        filters.put((String)filter_name.get(null), filter_meta);
      } catch (SecurityException e) {
        throw new RuntimeException("Unexpected security exception", e);
      } catch (NoSuchMethodException e) {
        LOG.error("Filter plugin " + pair.getClass().getCanonicalName() + 
            " did not implement one of the \"description\" or \"examples\" methods");
      } catch (NoSuchFieldException e) {
        LOG.error("Filter plugin " + pair.getClass().getCanonicalName() + 
            " did not have the \"FILTER_NAME\" field");
      } catch (IllegalArgumentException e) {
        throw new RuntimeException("Unexpected exception", e);
      } catch (IllegalAccessException e) {
        throw new RuntimeException("Unexpected security exception", e);
      } catch (InvocationTargetException e) {
        throw new RuntimeException("Unexpected security exception", e);
      }
      
    }
    return filters;
  }
  
  
  /** @return the tag key associated with this filter */
  public String getTagk() {
    return tagk;
  }

  @JsonIgnore
  public List<String> getTagVs() {
    return tagvs;
  }
  
  /** @return A copy of this filter BEFORE tag resolution, as a new object. */
  @JsonIgnore
  public TagVFilter getCopy() {
    return Builder()
        .setFilter(filter)
        .setTagk(tagk)
        .setType(getType())
        .setGroupBy(group_by)
        .build();
  }
  
  /** @return whether or not to group by the results of this filter */
  @JsonIgnore
  public boolean isGroupBy() {
    return group_by;
  }

  /** @param group_by Wether or not to group by the results of this filter */
  public void setGroupBy(final boolean group_by) {
    this.group_by = group_by;
  }
  
  public String getFilter() {
    return filter;
  }
  
  /** @return the simple class name of this filter */
  @JsonIgnore
  public String getName() {
    return this.getClass().getSimpleName();
  }

  /** @return Whether or not this filter should be executed against scan results */
  public boolean postScan() {
    return post_scan;
  }
  
  /** @param post_scan Whether or not this filter should be executed against 
   * scan results */
  public void setPostScan(final boolean post_scan) {
    this.post_scan = post_scan;
  }
  
  @Override
  public int compareTo(final TagVFilter filter) {
    return filter.tagk.compareTo(tagk);
  }

  /** @return a TagVFilter builder for constructing filters */
  public static Builder Builder() {
    return new Builder();
  }
  
  /**
   * Builder class used for deserializing filters from JSON queries via Jackson
   * since we don't want the user to worry about the class name. The type,
   * tagk and filter must be configured or the build will fail.
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "set")
  public static class Builder {
    private String type;
    private String tagk;
    private String filter;
    @JsonProperty
    private boolean group_by;
    
    /** @param type The type of filter matching a valid filter name */
    public Builder setType(final String type) {
      this.type = type;
      return this;
    }
    
    /** @param tagk The tag key to match on for this filter */
    public Builder setTagk(final String tagk) {
      this.tagk = tagk;
      return this;
    }

    /** @param filter The filter expression to use for matching */
    public Builder setFilter(final String filter) {
      this.filter = filter;
      return this;
    }
    
    /** @param group_by Whether or not the filter should group results */
    public Builder setGroupBy(final boolean group_by) {
      this.group_by = group_by;
      return this;
    }
    
    /**
     * Searches the filter map for the given type and returns an instantiated
     * filter if found. The caller must set the type, tagk and filter values.
     * @return A filter if instantiation was successful
     * @throws IllegalArgumentException if one of the required parameters was
     * not set or the filter couldn't be found.
     * @throws RuntimeException if the filter couldn't be instantiated. Check
     * the implementation if it's a plugin.
     */
    public TagVFilter build() { 
      if (type == null || type.isEmpty()) {
        throw new IllegalArgumentException(
            "The filter type cannot be null or empty");
      }
      if (tagk == null || tagk.isEmpty()) {
        throw new IllegalArgumentException(
            "The tagk cannot be null or empty");
      }
      
      final Pair<Class<?>, Constructor<? extends TagVFilter>> filter_meta = 
          tagv_filter_map.get(type);
      if (filter_meta == null) {
        throw new IllegalArgumentException(
            "Could not find a tag value filter of the type: " + type);
      }
      final Constructor<? extends TagVFilter> ctor = filter_meta.getValue();
      final TagVFilter tagv_filter;
      try {
        tagv_filter = ctor.newInstance(tagk, filter);
      } catch (IllegalArgumentException e) {
        throw e;
      } catch (InstantiationException e) {
        throw new RuntimeException("Failed to instantiate filter: " + type, e);
      } catch (IllegalAccessException e) {
        throw new RuntimeException("Failed to instantiate filter: " + type, e);
      } catch (InvocationTargetException e) {
        if (e.getCause() != null) {
          throw (RuntimeException)e.getCause();
        }
        throw new RuntimeException("Failed to instantiate filter: " + type, e);
      }
      
      tagv_filter.setGroupBy(group_by);
      return tagv_filter;
    }
  }

  public void populateTagvs() {
  }

}

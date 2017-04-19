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

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.DeferredGroupException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.hbase.async.Bytes;
import org.hbase.async.ClientStats;
import org.hbase.async.DeleteRequest;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.HBaseException;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.Timer;

import net.opentsdb.tsd.StorageExceptionHandler;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.DateTime;
import net.opentsdb.utils.PluginLoader;
import net.opentsdb.utils.Threads;
import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.tools.StartupPlugin;
import net.opentsdb.stats.Histogram;
import net.opentsdb.stats.QueryStats;
import net.opentsdb.stats.StatsCollector;

/**
 * Thread-safe implementation of the TSDB client.
 * <p>
 * This class is the central class of OpenTSDB.  You use it to add new data
 * points or query the database.
 */
public final class TSDB {
  private static final Logger LOG = LoggerFactory.getLogger(TSDB.class);
  
  static final byte[] FAMILY = { 't' };

  /** Charset used to convert Strings to byte arrays and back. */
  public static final Charset CHARSET = Charset.forName("ISO-8859-1");
  // private static short TAG_VALUE_WIDTH = 3;

  /** Client for the HBase cluster to use.  */
  final HBaseClient client;

  /** Name of the table in which timeseries are stored.  */
  final byte[] table;
  /** Name of the table where tree data is stored. */
  final byte[] treetable;
  /** Name of the table where meta data is stored. */
  final byte[] meta_table;

  /** Configuration object for all TSDB components */
  final Config config;

  /** Timer used for various tasks such as idle timeouts or query timeouts */
  private final HashedWheelTimer timer;
  
  private final CompactionQueue compactionq;
  /** Optional Startup Plugin to use if configured */
  private StartupPlugin startup = null;

  /** Plugin for dealing with data points that can't be stored */
  private StorageExceptionHandler storage_exception_handler = null;

  /** A filter plugin for allowing or blocking time series */
  private WriteableDataPointFilterPlugin ts_filter;
  
  /** Writes rejected by the filter */ 
  private final AtomicLong rejected_dps = new AtomicLong();
  
  /** Datapoints Added */
  private static final AtomicLong datapoints_added = new AtomicLong();

  /**
   * Constructor
   * @param client An initialized HBase client object
   * @param config An initialized configuration object
   * @since 2.1
   */
  public TSDB(final HBaseClient client, final Config config) {
    this.config = config;
    if (client == null) {
      final org.hbase.async.Config async_config;
      if (config.configLocation() != null && !config.configLocation().isEmpty()) {
        try {
          async_config = new org.hbase.async.Config(config.configLocation());
        } catch (final IOException e) {
          throw new RuntimeException("Failed to read the config file: " + 
              config.configLocation(), e);
        }
      } else {
        async_config = new org.hbase.async.Config();
      }
      async_config.overrideConfig("hbase.zookeeper.znode.parent", 
          config.getString("tsd.storage.hbase.zk_basedir"));
      async_config.overrideConfig("hbase.zookeeper.quorum", 
          config.getString("tsd.storage.hbase.zk_quorum"));
      this.client = new HBaseClient(async_config);
    } else {
      this.client = client;
    }
    
   if (config.hasProperty("tsd.storage.max_tags")) {
      Const.setMaxNumTags(config.getShort("tsd.storage.max_tags"));
    }
    if (config.hasProperty("tsd.storage.salt.buckets")) {
      Const.setSaltBuckets(config.getInt("tsd.storage.salt.buckets"));
    }
    if (config.hasProperty("tsd.storage.salt.width")) {
      Const.setSaltWidth(config.getInt("tsd.storage.salt.width"));
    }
    
    table = config.getString("tsd.storage.hbase.data_table").getBytes(CHARSET);
    treetable = config.getString("tsd.storage.hbase.tree_table").getBytes(CHARSET);
    meta_table = config.getString("tsd.storage.hbase.meta_table").getBytes(CHARSET);

    // Since we don't compact stuff anymore, this is used mostly used in a static fashion.
    // TODO static-ize this
    compactionq = new CompactionQueue(this);
   
    if (config.hasProperty("tsd.core.timezone")) {
      DateTime.setDefaultTimezone(config.getString("tsd.core.timezone"));
    }
    
    timer = Threads.newTimer("TSDB Timer");
    
    QueryStats.setEnableDuplicates(
        config.getBoolean("tsd.query.allow_simultaneous_duplicates"));
    
   
    if (config.getString("tsd.core.tag.allow_specialchars") != null) {
      Tags.setAllowSpecialChars(config.getString("tsd.core.tag.allow_specialchars"));
    }
    
    // set any extra tags from the config for stats
    StatsCollector.setGlobalTags(config);
    
    LOG.debug(config.dumpConfiguration());
  }
  
  /**
   * Constructor
   * @param config An initialized configuration object
   * @since 2.0
   */
  public TSDB(final Config config) {
    this(null, config);
  }
  
  /** @return The data point column family name */
  public static byte[] FAMILY() {
    return FAMILY;
  }

  /**
   * Called by initializePlugins, also used to load startup plugins.
   * @since 2.3
   */
  public static void loadPluginPath(final String plugin_path) {
    if (plugin_path != null && !plugin_path.isEmpty()) {
      try {
        PluginLoader.loadJARs(plugin_path);
      } catch (Exception e) {
        LOG.error("Error loading plugins from plugin path: " + plugin_path, e);
        throw new RuntimeException("Error loading plugins from plugin path: " +
                plugin_path, e);
      }
    }
  }

  /**
   * Should be called immediately after construction to initialize plugins and
   * objects that rely on such. It also moves most of the potential exception
   * throwing code out of the constructor so TSDMain can shutdown clients and
   * such properly.
   * @param init_rpcs Whether or not to initialize RPC plugins as well
   * @throws RuntimeException if the plugin path could not be processed
   * @throws IllegalArgumentException if a plugin could not be initialized
   * @since 2.0
   */
  public void initializePlugins(final boolean init_rpcs) {
    final String plugin_path = config.getString("tsd.core.plugin_path");
    loadPluginPath(plugin_path);

    try {
      TagVFilter.initializeFilterMap(this);
      // @#$@%$%#$ing typed exceptions
    } catch (SecurityException e) {
      throw new RuntimeException("Failed to instantiate filters", e);
    } catch (IllegalArgumentException e) {
      throw new RuntimeException("Failed to instantiate filters", e);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Failed to instantiate filters", e);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException("Failed to instantiate filters", e);
    } catch (NoSuchFieldException e) {
      throw new RuntimeException("Failed to instantiate filters", e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Failed to instantiate filters", e);
    } catch (InvocationTargetException e) {
      throw new RuntimeException("Failed to instantiate filters", e);
    }

    
    // load the storage exception plugin if enabled
    if (config.getBoolean("tsd.core.storage_exception_handler.enable")) {
      storage_exception_handler = PluginLoader.loadSpecificPlugin(
          config.getString("tsd.core.storage_exception_handler.plugin"), 
          StorageExceptionHandler.class);
      if (storage_exception_handler == null) {
        throw new IllegalArgumentException(
            "Unable to locate storage exception handler plugin: " + 
            config.getString("tsd.core.storage_exception_handler.plugin"));
      }
      try {
        storage_exception_handler.initialize(this);
      } catch (Exception e) {
        throw new RuntimeException(
            "Failed to initialize storage exception handler plugin", e);
      }
      LOG.info("Successfully initialized storage exception handler plugin [" + 
          storage_exception_handler.getClass().getCanonicalName() + "] version: " 
          + storage_exception_handler.version());
    }
    
    // Writeable Data Point Filter
    if (config.getBoolean("tsd.timeseriesfilter.enable")) {
      ts_filter = PluginLoader.loadSpecificPlugin(
          config.getString("tsd.timeseriesfilter.plugin"), 
          WriteableDataPointFilterPlugin.class);
      if (ts_filter == null) {
        throw new IllegalArgumentException(
            "Unable to locate time series filter plugin plugin: " + 
            config.getString("tsd.timeseriesfilter.plugin"));
      }
      try {
        ts_filter.initialize(this);
      } catch (Exception e) {
        throw new RuntimeException(
            "Failed to initialize time series filter plugin", e);
      }
      LOG.info("Successfully initialized time series filter plugin [" + 
          ts_filter.getClass().getCanonicalName() + "] version: " 
          + ts_filter.version());
    }
    
  }
  
  /** 
   * Returns the configured HBase client 
   * @return The HBase client
   * @since 2.0 
   */
  public final HBaseClient getClient() {
    return this.client;
  }

  /**
   * Sets the startup plugin so that it can be shutdown properly. 
   * Note that this method will not initialize or call any other methods 
   * belonging to the plugin's implementation.
   * @param plugin The startup plugin that was used. 
   * @since 2.3
   */
  public final void setStartupPlugin(final StartupPlugin plugin) { 
    startup = plugin; 
  }
  
  /**
   * Getter that returns the startup plugin object.
   * @return The StartupPlugin object or null if the plugin was not set.
   * @since 2.3
   */
  public final StartupPlugin getStartupPlugin() { 
    return startup; 
  }

  /**
   * Getter that returns the configuration object
   * @return The configuration object
   * @since 2.0 
   */
  public final Config getConfig() {
    return this.config;
  }
  
  /**
   * Returns the storage exception handler. May be null if not enabled
   * @return The storage exception handler
   * @since 2.2
   */
  public final StorageExceptionHandler getStorageExceptionHandler() {
    return storage_exception_handler;
  }

  /**
   * @return the TS filter object, may be null
   * @since 2.3
   */
  public WriteableDataPointFilterPlugin getTSfilter() {
    return ts_filter;
  }
  
  /**
   * Verifies that the data and UID tables exist in HBase and optionally the
   * tree and meta data tables if the user has enabled meta tracking or tree
   * building
   * @return An ArrayList of objects to wait for
   * @throws TableNotFoundException
   * @since 2.0
   */
  public Deferred<ArrayList<Object>> checkNecessaryTablesExist() {
    final ArrayList<Deferred<Object>> checks = 
      new ArrayList<Deferred<Object>>(2);
    checks.add(client.ensureTableExists(
        config.getString("tsd.storage.hbase.data_table")));
    checks.add(client.ensureTableExists(
        config.getString("tsd.storage.hbase.uid_table")));
    if (config.enable_tree_processing()) {
      checks.add(client.ensureTableExists(
          config.getString("tsd.storage.hbase.tree_table")));
    }
    if (config.enable_realtime_ts() || config.enable_realtime_uid() || 
        config.enable_tsuid_incrementing()) {
      checks.add(client.ensureTableExists(
          config.getString("tsd.storage.hbase.meta_table")));
    }
    return Deferred.group(checks);
  }
  
  /**
   * Collects the stats and metrics tracked by this instance.
   * @param collector The collector to use.
   */
  public void collectStats(final StatsCollector collector) {
    {
      final Runtime runtime = Runtime.getRuntime();
      collector.record("jvm.ramfree", runtime.freeMemory());
      collector.record("jvm.ramused", runtime.totalMemory());
    }

    collector.addExtraTag("class", "TSDB");
    try {
      collector.record("datapoints.added", datapoints_added, "type=all");
    } finally {
      collector.clearExtraTag("class");
    }

    collector.addExtraTag("class", "TsdbQuery");
    try {
      collector.record("hbase.latency", TsdbQuery.scanlatency, "method=scan");
    } finally {
      collector.clearExtraTag("class");
    }
    final ClientStats stats = client.stats();
    collector.record("hbase.root_lookups", stats.rootLookups());
    collector.record("hbase.meta_lookups",
                     stats.uncontendedMetaLookups(), "type=uncontended");
    collector.record("hbase.meta_lookups",
                     stats.contendedMetaLookups(), "type=contended");
    collector.record("hbase.rpcs",
                     stats.atomicIncrements(), "type=increment");
    collector.record("hbase.rpcs", stats.deletes(), "type=delete");
    collector.record("hbase.rpcs", stats.gets(), "type=get");
    collector.record("hbase.rpcs", stats.puts(), "type=put");
    collector.record("hbase.rpcs", stats.appends(), "type=append");
    collector.record("hbase.rpcs", stats.rowLocks(), "type=rowLock");
    collector.record("hbase.rpcs", stats.scannersOpened(), "type=openScanner");
    collector.record("hbase.rpcs", stats.scans(), "type=scan");
    collector.record("hbase.rpcs.batched", stats.numBatchedRpcSent());
    collector.record("hbase.flushes", stats.flushes());
    collector.record("hbase.connections.created", stats.connectionsCreated());
    collector.record("hbase.connections.idle_closed", stats.idleConnectionsClosed());
    collector.record("hbase.nsre", stats.noSuchRegionExceptions());
    collector.record("hbase.nsre.rpcs_delayed",
                     stats.numRpcDelayedDueToNSRE());
    collector.record("hbase.region_clients.open",
        stats.regionClients());
    collector.record("hbase.region_clients.idle_closed",
        stats.idleConnectionsClosed());

    // Collect Stats from Plugins
    if (startup != null) {
      try {
        collector.addExtraTag("plugin", "startup");
        startup.collectStats(collector);
      } finally {
        collector.clearExtraTag("plugin");
      }
    }
    if (storage_exception_handler != null) {
      try {
        collector.addExtraTag("plugin", "storageExceptionHandler");
        storage_exception_handler.collectStats(collector);
      } finally {
        collector.clearExtraTag("plugin");
      }
    }
    if (ts_filter != null) {
      try {
        collector.addExtraTag("plugin", "timeseriesFilter");
        ts_filter.collectStats(collector);
      } finally {
        collector.clearExtraTag("plugin");
      }
    }

  }

  /** Returns a latency histogram for Scan RPCs used to fetch data points.  */
  public Histogram getScanLatencyHistogram() {
    return TsdbQuery.scanlatency;
  }

  /**
   * Returns a new {@link Query} instance suitable for this TSDB.
   */
  public Query newQuery() {
    return new TsdbQuery(this);
  }

  /**
   * Adds a single integer value data point in the TSDB.
   * @param metric A non-empty string.
   * @param timestamp The timestamp associated with the value.
   * @param value The value of the data point.
   * @param tags The tags on this series.  This map must be non-empty.
   * @return A deferred object that indicates the completion of the request.
   * The {@link Object} has not special meaning and can be {@code null} (think
   * of it as {@code Deferred<Void>}). But you probably want to attach at
   * least an errback to this {@code Deferred} to handle failures.
   * @throws IllegalArgumentException if the timestamp is less than or equal
   * to the previous timestamp added or 0 for the first timestamp, or if the
   * difference with the previous timestamp is too large.
   * @throws IllegalArgumentException if the metric name is empty or contains
   * illegal characters.
   * @throws IllegalArgumentException if the tags list is empty or one of the
   * elements contains illegal characters.
   * @throws HBaseException (deferred) if there was a problem while persisting
   * data.
   */
  public Deferred<Object> addPoint(final String metric,
                                   final long timestamp,
                                   final long value,
                                   final Map<String, String> tags) {
    final byte[] v;
    if (Byte.MIN_VALUE <= value && value <= Byte.MAX_VALUE) {
      v = new byte[] { (byte) value };
    } else if (Short.MIN_VALUE <= value && value <= Short.MAX_VALUE) {
      v = Bytes.fromShort((short) value);
    } else if (Integer.MIN_VALUE <= value && value <= Integer.MAX_VALUE) {
      v = Bytes.fromInt((int) value);
    } else {
      v = Bytes.fromLong(value);
    }

    final short flags = (short) (v.length - 1);  // Just the length.
    return addPointInternal(metric, timestamp, v, tags, flags);
  }

  /**
   * Adds a double precision floating-point value data point in the TSDB.
   * @param metric A non-empty string.
   * @param timestamp The timestamp associated with the value.
   * @param value The value of the data point.
   * @param tags The tags on this series.  This map must be non-empty.
   * @return A deferred object that indicates the completion of the request.
   * The {@link Object} has not special meaning and can be {@code null} (think
   * of it as {@code Deferred<Void>}). But you probably want to attach at
   * least an errback to this {@code Deferred} to handle failures.
   * @throws IllegalArgumentException if the timestamp is less than or equal
   * to the previous timestamp added or 0 for the first timestamp, or if the
   * difference with the previous timestamp is too large.
   * @throws IllegalArgumentException if the metric name is empty or contains
   * illegal characters.
   * @throws IllegalArgumentException if the value is NaN or infinite.
   * @throws IllegalArgumentException if the tags list is empty or one of the
   * elements contains illegal characters.
   * @throws HBaseException (deferred) if there was a problem while persisting
   * data.
   * @since 1.2
   */
  public Deferred<Object> addPoint(final String metric,
                                   final long timestamp,
                                   final double value,
                                   final Map<String, String> tags) {
    if (Double.isNaN(value) || Double.isInfinite(value)) {
      throw new IllegalArgumentException("value is NaN or Infinite: " + value
                                         + " for metric=" + metric
                                         + " timestamp=" + timestamp);
    }
    final short flags = Const.FLAG_FLOAT | 0x7;  // A float stored on 8 bytes.
    return addPointInternal(metric, timestamp,
                            Bytes.fromLong(Double.doubleToRawLongBits(value)),
                            tags, flags);
  }

  /**
   * Adds a single floating-point value data point in the TSDB.
   * @param metric A non-empty string.
   * @param timestamp The timestamp associated with the value.
   * @param value The value of the data point.
   * @param tags The tags on this series.  This map must be non-empty.
   * @return A deferred object that indicates the completion of the request.
   * The {@link Object} has not special meaning and can be {@code null} (think
   * of it as {@code Deferred<Void>}). But you probably want to attach at
   * least an errback to this {@code Deferred} to handle failures.
   * @throws IllegalArgumentException if the timestamp is less than or equal
   * to the previous timestamp added or 0 for the first timestamp, or if the
   * difference with the previous timestamp is too large.
   * @throws IllegalArgumentException if the metric name is empty or contains
   * illegal characters.
   * @throws IllegalArgumentException if the value is NaN or infinite.
   * @throws IllegalArgumentException if the tags list is empty or one of the
   * elements contains illegal characters.
   * @throws HBaseException (deferred) if there was a problem while persisting
   * data.
   */
  public Deferred<Object> addPoint(final String metric,
                                   final long timestamp,
                                   final float value,
                                   final Map<String, String> tags) {
    if (Float.isNaN(value) || Float.isInfinite(value)) {
      throw new IllegalArgumentException("value is NaN or Infinite: " + value
                                         + " for metric=" + metric
                                         + " timestamp=" + timestamp);
    }
    final short flags = Const.FLAG_FLOAT | 0x3;  // A float stored on 4 bytes.
    return addPointInternal(metric, timestamp,
                            Bytes.fromInt(Float.floatToRawIntBits(value)),
                            tags, flags);
  }

  private Deferred<Object> addPointInternal(final String metricStr,
                                            final long timestamp,
                                            final byte[] value,
                                            final Map<String, String> tagm,
                                            final short flags) {
    // we only accept positive unix epoch timestamps in seconds or milliseconds
    if (timestamp < 0 || ((timestamp & Const.SECOND_MASK) != 0 && 
        timestamp > 9999999999999L)) {
      throw new IllegalArgumentException((timestamp < 0 ? "negative " : "bad")
          + " timestamp=" + timestamp
          + " when trying to add value=" + Arrays.toString(value) + '/' + flags
          + " to metric=" + metricStr + ", tags=" + tagm);
    }
    byte[] metric = metricStr.getBytes(CHARSET);
    byte[] tags = RowKey.tagsToBytes(tagm);
    RowKey.checkMetricAndTags(metricStr, tagm);
    final byte[] row = RowKey.rowKeyTemplate(this, metric, tags);
    final long base_time;
    final byte[] qualifier = Internal.buildQualifier(timestamp, flags);
    
    if ((timestamp & Const.SECOND_MASK) != 0) {
      // drop the ms timestamp to seconds to calculate the base timestamp
      base_time = ((timestamp / 1000) - 
          ((timestamp / 1000) % Const.MAX_TIMESPAN));
    } else {
      base_time = (timestamp - (timestamp % Const.MAX_TIMESPAN));
    }
    
    /** Callback executed for chaining filter calls to see if the value
     * should be written or not. */
    final class WriteCB implements Callback<Deferred<Object>, Boolean> {
      @Override
      public Deferred<Object> call(final Boolean allowed) throws Exception {
        if (!allowed) {
          rejected_dps.incrementAndGet();
          return Deferred.fromResult(null);
        }
        
        Bytes.setInt(row, (int) base_time, metric.length + Const.SALT_WIDTH());
        RowKey.prefixKeyWithSalt(row, metric, tags);

        Deferred<Object> result = null;
        final PutRequest point = new PutRequest(table, row, metric, Bytes.fromInt((int) base_time), tags, FAMILY, qualifier, value);
        result = client.put(point);

        // Count all added datapoints, not just those that came in through PUT rpc
        // Will there be others? Well, something could call addPoint programatically right?
        datapoints_added.incrementAndGet();

        return result;
      }
      @Override
      public String toString() {
        return "addPointInternal Write Callback";
      }
    }
    
    if (ts_filter != null && ts_filter.filterDataPoints()) {
      return ts_filter.allowDataPoint(metricStr, timestamp, value, tagm, flags)
          .addCallbackDeferring(new WriteCB());
    }
    return Deferred.fromResult(true).addCallbackDeferring(new WriteCB());
  }

  /**
   * Forces a flush of any un-committed in memory data including left over 
   * compactions.
   * <p>
   * For instance, any data point not persisted will be sent to HBase.
   * @return A {@link Deferred} that will be called once all the un-committed
   * data has been successfully and durably stored.  The value of the deferred
   * object return is meaningless and unspecified, and can be {@code null}.
   * @throws HBaseException (deferred) if there was a problem sending
   * un-committed data to HBase.  Please refer to the {@link HBaseException}
   * hierarchy to handle the possible failures.  Some of them are easily
   * recoverable by retrying, some are not.
   */
  public Deferred<Object> flush() throws HBaseException {
    return client.flush();
  }

  /**
   * Gracefully shuts down this TSD instance.
   * <p>
   * The method must call {@code shutdown()} on all plugins as well as flush the
   * compaction queue.
   * @return A {@link Deferred} that will be called once all the un-committed
   * data has been successfully and durably stored, and all resources used by
   * this instance have been released.  The value of the deferred object
   * return is meaningless and unspecified, and can be {@code null}.
   * @throws HBaseException (deferred) if there was a problem sending
   * un-committed data to HBase.  Please refer to the {@link HBaseException}
   * hierarchy to handle the possible failures.  Some of them are easily
   * recoverable by retrying, some are not.
   */
  public Deferred<Object> shutdown() {
    final ArrayList<Deferred<Object>> deferreds = 
      new ArrayList<Deferred<Object>>();
    
    final class FinalShutdown implements Callback<Object, Object> {
      @Override
      public Object call(Object result) throws Exception {
        if (result instanceof Exception) {
          LOG.error("A previous shutdown failed", (Exception)result);
        }
        final Set<Timeout> timeouts = timer.stop();
        // TODO - at some point we should clean these up.
        if (timeouts.size() > 0) {
          LOG.warn("There were " + timeouts.size() + " timer tasks queued");
        }
        LOG.info("Completed shutting down the TSDB");
        return Deferred.fromResult(null);
      }
    }
    
    final class SEHShutdown implements Callback<Object, Object> {
      @Override
      public Object call(Object result) throws Exception {
        if (result instanceof Exception) {
          LOG.error("Shutdown of the HBase client failed", (Exception)result);
        }
        LOG.info("Shutting down storage exception handler plugin: " + 
            storage_exception_handler.getClass().getCanonicalName());
        return storage_exception_handler.shutdown().addBoth(new FinalShutdown());
      }
      @Override
      public String toString() {
        return "SEHShutdown";
      }
    }
    
    final class HClientShutdown implements Callback<Deferred<Object>, ArrayList<Object>> {
      public Deferred<Object> call(final ArrayList<Object> args) {
        if (storage_exception_handler != null) {
          return client.shutdown().addBoth(new SEHShutdown());
        }
        return client.shutdown().addBoth(new FinalShutdown());
      }
      public String toString() {
        return "shutdown HBase client";
      }
    }
    
    final class ShutdownErrback implements Callback<Object, Exception> {
      public Object call(final Exception e) {
        final Logger LOG = LoggerFactory.getLogger(ShutdownErrback.class);
        if (e instanceof DeferredGroupException) {
          final DeferredGroupException ge = (DeferredGroupException) e;
          for (final Object r : ge.results()) {
            if (r instanceof Exception) {
              LOG.error("Failed to shutdown the TSD", (Exception) r);
            }
          }
        } else {
          LOG.error("Failed to shutdown the TSD", e);
        }
        return new HClientShutdown().call(null);
      }
      public String toString() {
        return "shutdown HBase client after error";
      }
    }
    
    if (startup != null) {
      LOG.info("Shutting down startup plugin: " +
              startup.getClass().getCanonicalName());
      deferreds.add(startup.shutdown());
    }
    if (storage_exception_handler != null) {
      LOG.info("Shutting down storage exception handler plugin: " + 
          storage_exception_handler.getClass().getCanonicalName());
      deferreds.add(storage_exception_handler.shutdown());
    }
    if (ts_filter != null) {
      LOG.info("Shutting down time series filter plugin: " + 
          ts_filter.getClass().getCanonicalName());
      deferreds.add(ts_filter.shutdown());
    }
    
    // wait for plugins to shutdown before we close the client
    return deferreds.size() > 0
      ? Deferred.group(deferreds).addCallbackDeferring(new HClientShutdown())
          .addErrback(new ShutdownErrback())
      : new HClientShutdown().call(null);
  }

 
  /** @return the name of the data table as a byte array for client requests */
  public byte[] dataTable() {
    return this.table;
  }
  
  /** @return the name of the tree table as a byte array for client requests */
  public byte[] treeTable() {
    return this.treetable;
  }
  
  /** @return the name of the meta table as a byte array for client requests */
  public byte[] metaTable() {
    return this.meta_table;
  }

  /**
   * Simply logs plugin errors when they're thrown by attaching as an errorback. 
   * Without this, exceptions will just disappear (unless logged by the plugin) 
   * since we don't wait for a result.
   */
  final class PluginError implements Callback<Object, Exception> {
    @Override
    public Object call(final Exception e) throws Exception {
      LOG.error("Exception from Search plugin indexer", e);
      return null;
    }
  }
  
 
  /** @return the timer used for various house keeping functions */
  public Timer getTimer() {
    return timer;
  }
  
  // ------------------ //
  // Compaction helpers //
  // ------------------ //

  final KeyValue compact(final ArrayList<KeyValue> row) {
    return compactionq.compact(row);
  }

  // ------------------------ //
  // HBase operations helpers //
  // ------------------------ //

  /** Gets the entire given row from the data table. */
  final Deferred<ArrayList<KeyValue>> get(final byte[] key) {
    return client.get(new GetRequest(table, key, FAMILY));
  }

  /** Puts the given value into the data table. */
  final Deferred<Object> put(final byte[] key,
                             final byte[] qualifier,
                             final byte[] value) {
    return client.put(new PutRequest(table, key, FAMILY, qualifier, value));
  }

  /** Deletes the given cells from the data table. */
  final Deferred<Object> delete(final byte[] key, final byte[][] qualifiers) {
    return client.delete(new DeleteRequest(table, key, FAMILY, qualifiers));
  }

}

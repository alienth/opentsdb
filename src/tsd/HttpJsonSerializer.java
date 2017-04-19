// This file is part of OpenTSDB.
// Copyright (C) 2013  The OpenTSDB Authors.
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
package net.opentsdb.tsd;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.type.TypeReference;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.DataPoint;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.FillPolicy;
import net.opentsdb.core.IncomingDataPoint;
import net.opentsdb.core.QueryException;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSQuery;
import net.opentsdb.core.TSSubQuery;
import net.opentsdb.stats.QueryStats;
import net.opentsdb.stats.QueryStats.QueryStat;
import net.opentsdb.tsd.QueryRpc.LastPointQuery;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.DateTime;
import net.opentsdb.utils.JSON;

/**
 * Implementation of the base serializer class with JSON as the format
 * <p>
 * <b>Note:</b> This class is not final and the implementations are not either
 * so that we can extend this default class with slightly different methods
 * when needed and retain everything else.
 * @since 2.0
 */
class HttpJsonSerializer extends HttpSerializer {

  /** Type reference for incoming data points */
  private static TypeReference<ArrayList<IncomingDataPoint>> TR_INCOMING =
    new TypeReference<ArrayList<IncomingDataPoint>>() {};
  
  /** Type reference for uid assignments */
  private static TypeReference<HashMap<String, List<String>>> UID_ASSIGN =
    new TypeReference<HashMap<String, List<String>>>() {};
   
  /**
   * Default constructor necessary for plugin implementation
   */
  public HttpJsonSerializer() {
    super();
  }
  
  /**
   * Constructor that sets the query object
   * @param query Request/resposne object
   */
  public HttpJsonSerializer(final HttpQuery query) {
    super(query);
  }
  
  /** Initializer, nothing to do for the JSON serializer */
  @Override
  public void initialize(final TSDB tsdb) {
    // nothing to see here
  }
  
  /** Nothing to do on shutdown */
  public Deferred<Object> shutdown() {
    return new Deferred<Object>();
  }
  
  /** @return the version */
  @Override
  public String version() {
    return "2.0.0";
  }

  /** @return the shortname */
  @Override
  public String shortName() {
    return "json";
  }
  
  /**
   * Parses one or more data points for storage
   * @return an array of data points to process for storage
   * @throws JSONException if parsing failed
   * @throws BadRequestException if the content was missing or parsing failed
   */
  @Override
  public List<IncomingDataPoint> parsePutV1() {
    if (!query.hasContent()) {
      throw new BadRequestException("Missing request content");
    }

    // convert to a string so we can handle character encoding properly
    final String content = query.getContent().trim();
    final int firstbyte = content.charAt(0);
    try {
      if (firstbyte == '{') {
        final IncomingDataPoint dp = 
          JSON.parseToObject(content, IncomingDataPoint.class);
        final ArrayList<IncomingDataPoint> dps = 
          new ArrayList<IncomingDataPoint>(1);
        dps.add(dp);
        return dps;
      } else {
        return JSON.parseToObject(content, TR_INCOMING);
      }
    } catch (IllegalArgumentException iae) {
      throw new BadRequestException("Unable to parse the given JSON", iae);
    }
  }
  
  /**
   * Parses a suggestion query
   * @return a hash map of key/value pairs
   * @throws JSONException if parsing failed
   * @throws BadRequestException if the content was missing or parsing failed
   */
  @Override
  public HashMap<String, String> parseSuggestV1() {
    final String json = query.getContent();
    if (json == null || json.isEmpty()) {
      throw new BadRequestException(HttpResponseStatus.BAD_REQUEST,
          "Missing message content",
          "Supply valid JSON formatted data in the body of your request");
    }
    try {
      return JSON.parseToObject(query.getContent(), 
          new TypeReference<HashMap<String, String>>(){});
    } catch (IllegalArgumentException iae) {
      throw new BadRequestException("Unable to parse the given JSON", iae);
    }
  }
  
  /**
   * Parses a list of metrics, tagk and/or tagvs to assign UIDs to
   * @return as hash map of lists for the different types
   * @throws JSONException if parsing failed
   * @throws BadRequestException if the content was missing or parsing failed
   */
  public HashMap<String, List<String>> parseUidAssignV1() {
    final String json = query.getContent();
    if (json == null || json.isEmpty()) {
      throw new BadRequestException(HttpResponseStatus.BAD_REQUEST,
          "Missing message content",
          "Supply valid JSON formatted data in the body of your request");
    }
    try {
      return JSON.parseToObject(json, UID_ASSIGN);
    } catch (IllegalArgumentException iae) {
      throw new BadRequestException("Unable to parse the given JSON", iae);
    }
  }
  
  /**
   * Parses a timeseries data query
   * @return A TSQuery with data ready to validate
   * @throws JSONException if parsing failed
   * @throws BadRequestException if the content was missing or parsing failed
   */
  public TSQuery parseQueryV1() {
    final String json = query.getContent();
    if (json == null || json.isEmpty()) {
      throw new BadRequestException(HttpResponseStatus.BAD_REQUEST,
          "Missing message content",
          "Supply valid JSON formatted data in the body of your request");
    }
    try {
      return JSON.parseToObject(json, TSQuery.class);
    } catch (IllegalArgumentException iae) {
      throw new BadRequestException("Unable to parse the given JSON", iae);
    }
  }
  
  /**
   * Parses a last data point query
   * @return A LastPointQuery to work with
   * @throws JSONException if parsing failed
   * @throws BadRequestException if the content was missing or parsing failed
   */
  public LastPointQuery parseLastPointQueryV1() {
    final String json = query.getContent();
    if (json == null || json.isEmpty()) {
      throw new BadRequestException(HttpResponseStatus.BAD_REQUEST,
          "Missing message content",
          "Supply valid JSON formatted data in the body of your request");
    }
    try {
      return JSON.parseToObject(json, LastPointQuery.class);
    } catch (IllegalArgumentException iae) {
      throw new BadRequestException("Unable to parse the given JSON", iae);
    }
  }
  
  
  /**
   * Formats the results of an HTTP data point storage request
   * @param results A map of results. The map will consist of:
   * <ul><li>success - (long) the number of successfully parsed datapoints</li>
   * <li>failed - (long) the number of datapoint parsing failures</li>
   * <li>errors - (ArrayList<HashMap<String, Object>>) an optional list of 
   * datapoints that had errors. The nested map has these fields:
   * <ul><li>error - (String) the error that occurred</li>
   * <li>datapoint - (IncomingDatapoint) the datapoint that generated the error
   * </li></ul></li></ul>
   * @return A JSON formatted byte array
   * @throws JSONException if serialization failed
   */
  public ChannelBuffer formatPutV1(final Map<String, Object> results) {
    return this.serializeJSON(results);
  }
  
  /**
   * Formats a suggestion response
   * @param suggestions List of suggestions for the given type
   * @return A JSON formatted byte array
   * @throws JSONException if serialization failed
   */
  @Override
  public ChannelBuffer formatSuggestV1(final List<String> suggestions) {
    return this.serializeJSON(suggestions);
  }
  
  /**
   * Format the serializer status map
   * @return A JSON structure
   * @throws JSONException if serialization failed
   */
  public ChannelBuffer formatSerializersV1() {
    return serializeJSON(HttpQuery.getSerializerStatus());
  }
  
  /**
   * Format the list of implemented aggregators
   * @param aggregators The list of aggregation functions
   * @return A JSON structure
   * @throws JSONException if serialization failed
   */
  public ChannelBuffer formatAggregatorsV1(final Set<String> aggregators) {
    return this.serializeJSON(aggregators);
  }
  
  /**
   * Format a hash map of information about the OpenTSDB version
   * @param version A hash map with version information
   * @return A JSON structure
   * @throws JSONException if serialization failed
   */
  public ChannelBuffer formatVersionV1(final Map<String, String> version) {
    return this.serializeJSON(version);
  }
  
  /**
   * Format a response from the DropCaches call
   * @param response A hash map with a response
   * @return A JSON structure
   * @throws JSONException if serialization failed
   */
  public ChannelBuffer formatDropCachesV1(final Map<String, String> response) {
    return this.serializeJSON(response);
  }
  
  /**
   * Format the results from a timeseries data query
   * @param data_query The TSQuery object used to fetch the results
   * @param results The data fetched from storage
   * @param globals An optional list of global annotation objects
   * @return A ChannelBuffer object to pass on to the caller
   */
  public ChannelBuffer formatQueryV1(final TSQuery data_query, 
      final List<DataPoints[]> results) {
    try {
      return formatQueryAsyncV1(data_query, results)
          .joinUninterruptibly();
    } catch (QueryException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Shouldn't be here", e);
    }
  }
  
  /**
   * Format the results from a timeseries data query
   * @param data_query The TSQuery object used to fetch the results
   * @param results The data fetched from storage
   * @return A Deferred<ChannelBuffer> object to pass on to the caller
   * @throws IOException if serialization failed
   * @since 2.2
   */
  public Deferred<ChannelBuffer> formatQueryAsyncV1(final TSQuery data_query, 
      final List<DataPoints[]> results) 
          throws IOException {
    
    final long start = DateTime.currentTimeMillis();
    final boolean as_arrays = this.query.hasQueryStringParam("arrays");
    final String jsonp = this.query.getQueryStringParam("jsonp");
    
    // buffers and an array list to stored the deferreds
    final ChannelBuffer response = ChannelBuffers.dynamicBuffer();
    final OutputStream output = new ChannelBufferOutputStream(response);
    // too bad an inner class can't modify a primitive. This is a work around 
    final List<Boolean> timeout_flag = new ArrayList<Boolean>(1);
    timeout_flag.add(false);
    
    // start with JSONp if we're told to
    if (jsonp != null && !jsonp.isEmpty()) {
      output.write((jsonp + "(").getBytes(query.getCharset()));
    }
    
    // start the JSON generator and write the opening array
    final JsonGenerator json = JSON.getFactory().createGenerator(output);
    json.writeStartArray();
 
    /**
     * Every individual data point set (the result of a query and possibly a
     * group by) will initiate an asynchronous metric/tag UID to name resolution
     * and then print to the buffer.
     * NOTE that because this is asynchronous, the order of results is
     * indeterminate.
     */
    class DPsResolver implements Callback<Deferred<Object>, Object> {
      /** Has to be final to be shared with the nested classes */
      final StringBuilder metric = new StringBuilder(256);
      /** Resolved tags */
      final Map<String, String> tags = new HashMap<String, String>();
      /** Resolved aggregated tags */
      final List<String> agg_tags = new ArrayList<String>();
      /** A list storing the metric and tag resolve calls */
      final List<Deferred<Object>> resolve_deferreds = 
          new ArrayList<Deferred<Object>>();
      /** The data points to serialize */
      final DataPoints dps;
      /** Starting time in nanos when we sent the UID resolution queries off */
      long uid_start;
      
      public DPsResolver(final DataPoints dps) {
        this.dps = dps;
      }
      
      /** Resolves the metric UID to a name*/
      class MetricResolver implements Callback<Object, String> {
        public Object call(final String metric) throws Exception {
          DPsResolver.this.metric.append(metric);
          return null;
        }
      }
      
      /** Resolves the tag UIDs to a key/value string set */
      class TagResolver implements Callback<Object, Map<String, String>> {
        public Object call(final Map<String, String> tags) throws Exception {
          DPsResolver.this.tags.putAll(tags);
          return null;
        }
      }
      
      /** Resolves aggregated tags */
      class AggTagResolver implements Callback<Object, Set<String>> {
        public Object call(final Set<String> tags) throws Exception {
          DPsResolver.this.agg_tags.addAll(tags);
          return null;
        }
      }
      
      /** After the metric and tags have been resolved, this will print the
       * results to the output buffer in the proper format.
       */
      class WriteToBuffer implements Callback<Object, ArrayList<Object>> {
        final DataPoints dps;
        
        /**
         * Default ctor that takes a data point set
         * @param dps Datapoints to print
         */
        public WriteToBuffer(final DataPoints dps) {
          this.dps = dps;
        }
        
        /**
         * Handles writing the data to the output buffer. The results of the
         * deferreds don't matter as they will be stored in the class final
         * variables.
         */
        public Object call(final ArrayList<Object> deferreds) throws Exception {
          data_query.getQueryStats().addStat(dps.getQueryIndex(), 
              QueryStat.UID_TO_STRING_TIME, (DateTime.nanoTime() - uid_start));
          final long local_serialization_start = DateTime.nanoTime();
          final TSSubQuery orig_query = data_query.getQueries()
              .get(dps.getQueryIndex());
          
          json.writeStartObject();
          json.writeStringField("metric", metric.toString());
          
          json.writeFieldName("tags");
          json.writeStartObject();
          if (dps.getTags() != null) {
            for (Map.Entry<String, String> tag : tags.entrySet()) {
              json.writeStringField(tag.getKey(), tag.getValue());
            }
          }
          json.writeEndObject();
          
          json.writeFieldName("aggregateTags");
          json.writeStartArray();
          if (dps.getAggregatedTags() != null) {
            for (String atag : agg_tags) {
              json.writeString(atag);
            }
          }
          json.writeEndArray();
          
          if (data_query.getShowQuery()) {
            json.writeObjectField("query", orig_query);
          }
          
          // now the fun stuff, dump the data and time just the iteration over
          // the data points
          final long dps_start = DateTime.nanoTime();
          json.writeFieldName("dps");
          long counter = 0;
          
          // default is to write a map, otherwise write arrays
          if (!timeout_flag.get(0) && as_arrays) {
            json.writeStartArray();
            for (final DataPoint dp : dps) {
              if (dp.timestamp() < data_query.startTime() || 
                  dp.timestamp() > data_query.endTime()) {
                continue;
              }
              final long timestamp = data_query.getMsResolution() ? 
                  dp.timestamp() : dp.timestamp() / 1000;
              json.writeStartArray();
              json.writeNumber(timestamp);
              if (dp.isInteger()) {
                json.writeNumber(dp.longValue());
              } else { 
                // Report missing intervals as null or NaN.
                final double value = dp.doubleValue();
                if (Double.isNaN(value) && 
                    orig_query.fillPolicy() == FillPolicy.NULL) {
                  json.writeNull();
                } else {
                  json.writeNumber(dp.doubleValue());
                }
              }
              json.writeEndArray();
              ++counter;
            }
            json.writeEndArray();
          } else if (!timeout_flag.get(0)) {
            json.writeStartObject();
            for (final DataPoint dp : dps) {
              if (dp.timestamp() < (data_query.startTime()) || 
                  dp.timestamp() > (data_query.endTime())) {
                continue;
              }
              final long timestamp = data_query.getMsResolution() ? 
                  dp.timestamp() : dp.timestamp() / 1000;
              if (dp.isInteger()) {
                json.writeNumberField(Long.toString(timestamp), dp.longValue());
              } else {
                // Report missing intervals as null or NaN.
                final double value = dp.doubleValue();
                if (Double.isNaN(value) && 
                    orig_query.fillPolicy() == FillPolicy.NULL) {
                  json.writeNumberField(Long.toString(timestamp), null);
                } else {
                  json.writeNumberField(Long.toString(timestamp), dp.doubleValue());
                }
              }
              ++counter;
            }
            json.writeEndObject();
            
          } else {
            // skipping data points all together due to timeout
            json.writeStartObject();
            json.writeEndObject();
          }
          
          final long agg_time = DateTime.nanoTime() - dps_start;
          data_query.getQueryStats().addStat(dps.getQueryIndex(), 
              QueryStat.AGGREGATION_TIME, agg_time);
          data_query.getQueryStats().addStat(dps.getQueryIndex(), 
              QueryStat.AGGREGATED_SIZE, counter);
          
          // yeah, it's a little early but we need to dump it out with the results.
          data_query.getQueryStats().addStat(dps.getQueryIndex(), 
              QueryStat.SERIALIZATION_TIME, 
              DateTime.nanoTime() - local_serialization_start);
          if (!timeout_flag.get(0) && data_query.getShowStats()) {
            int query_index = (dps == null) ? -1 : dps.getQueryIndex();
            QueryStats stats = data_query.getQueryStats();
            
            if (query_index >= 0) {
              json.writeFieldName("stats");
              final Map<String, Object> s = stats.getQueryStats(query_index, false);
              if (s != null) {
                json.writeObject(s);
              } else {
                json.writeStringField("ERROR", "NO STATS FOUND");
              }
            }
          }

          // close the results for this particular query
          json.writeEndObject();
          return null;
        }
      }
      
      /**
       * When called, initiates a resolution of metric and tag UIDs to names, 
       * then prints to the output buffer once they are completed.
       */
      public Deferred<Object> call(final Object obj) throws Exception {
        this.uid_start = DateTime.nanoTime();
        
        resolve_deferreds.add(Deferred.fromResult(dps.metricName())
            .addCallback(new MetricResolver()));
        resolve_deferreds.add(Deferred.fromResult(dps.getTags())
            .addCallback(new TagResolver()));
        resolve_deferreds.add(Deferred.fromResult(dps.getAggregatedTags())
            .addCallback(new AggTagResolver()));
        return Deferred.group(resolve_deferreds)
            .addCallback(new WriteToBuffer(dps));
      }

    }
    
    // We want the serializer to execute serially so we need to create a callback
    // chain so that when one DPsResolver is finished, it triggers the next to
    // start serializing.
    final Deferred<Object> cb_chain = new Deferred<Object>();

    for (DataPoints[] separate_dps : results) {
      for (DataPoints dps : separate_dps) {
        try {
          cb_chain.addCallback(new DPsResolver(dps));
        } catch (Exception e) {
          throw new RuntimeException("Unexpected error durring resolution", e);
        }
      }
    }
  
    /** Final callback to close out the JSON array and return our results */
    class FinalCB implements Callback<ChannelBuffer, Object> {
      public ChannelBuffer call(final Object obj)
          throws Exception {
        
        // Call this here so we rollup sub metrics into a summary. It's not
        // completely accurate, of course, because we still have to write the
        // summary and close the writer. But it's close.
        data_query.getQueryStats().markSerializationSuccessful();

        // dump overall stats as an extra object in the array
        // TODO - yeah, I've heard this sucks, we need to figure out a better way.
        if (data_query.getShowSummary()) {
          final QueryStats stats = data_query.getQueryStats();
          json.writeStartObject();
          json.writeFieldName("statsSummary");
          json.writeObject(stats.getStats(true, true));
          json.writeEndObject();
        }
        
        // IMPORTANT Make sure the close the JSON array and the generator
        json.writeEndArray();
        json.close();
        
        if (jsonp != null && !jsonp.isEmpty()) {
          output.write(")".getBytes());
        }
        return response;
      }
    }

    // trigger the callback chain here
    cb_chain.callback(null);
    return cb_chain.addCallback(new FinalCB());
  }
  
  /**
   * Format a list of last data points
   * @param data_points The results of the query
   * @return A JSON structure
   * @throws JSONException if serialization failed
   */
  public ChannelBuffer formatLastPointQueryV1(
      final List<IncomingDataPoint> data_points) {
    return this.serializeJSON(data_points);
  }
  
  /**
   * Format a list of statistics
   * @param note The statistics list to format
   * @return A ChannelBuffer object to pass on to the caller
   * @throws JSONException if serialization failed
   */
  public ChannelBuffer formatStatsV1(final List<IncomingDataPoint> stats) {
    return serializeJSON(stats);
  }
  
  /**
   * Format a list of thread statistics
   * @param stats The thread statistics list to format
   * @return A ChannelBuffer object to pass on to the caller
   * @throws JSONException if serialization failed
   * @since 2.2
   */
  public ChannelBuffer formatThreadStatsV1(final List<Map<String, Object>> stats) {
    return serializeJSON(stats);
  }
  
  /**
   * format a list of region client statistics
   * @param stats The list of region client stats to format
   * @return A ChannelBuffer object to pass on to the caller
   * @throws JSONException if serialization failed
   * @since 2.2
   */
  public ChannelBuffer formatRegionStatsV1(final List<Map<String, Object>> stats) {
    return serializeJSON(stats);
  }

  /**
   * Format a list of JVM statistics
   * @param stats The JVM stats map to format
   * @return A ChannelBuffer object to pass on to the caller
   * @throws JSONException if serialization failed
   * @since 2.2
   */
  public ChannelBuffer formatJVMStatsV1(final Map<String, Map<String, Object>> stats) {
    return serializeJSON(stats);
  }
  
  /**
   * Format the query stats
   * @param query_stats Map of query statistics
   * @return A ChannelBuffer object to pass on to the caller
   * @throws BadRequestException if the plugin has not implemented this method
   * @since 2.2
   */
  public ChannelBuffer formatQueryStatsV1(final Map<String, Object> query_stats) {
    return serializeJSON(query_stats);
  }
  
  
  /**
   * Format the running configuration
   * @param config The running config to serialize
   * @return A ChannelBuffer object to pass on to the caller
   * @throws JSONException if serialization failed
   */
  public ChannelBuffer formatConfigV1(final Config config) {
    TreeMap<String, String> map = new TreeMap<String, String>(config.getMap());
    for (Map.Entry<String, String> entry : map.entrySet()) {
      if (entry.getKey().toUpperCase().contains("PASS")) {
        map.put(entry.getKey(), "********");
      }
    }
    return serializeJSON(map);
  }
  
  /**
   * Format the loaded filter configurations
   * @param config The filters to serialize
   * @return A ChannelBuffer object to pass on to the caller
   * @throws JSONException if serialization failed
   */
  public ChannelBuffer formatFilterConfigV1(
      final Map<String, Map<String, String>> config) {
    return serializeJSON(config);
  }
  
  /**
   * Helper object for the format calls to wrap the JSON response in a JSONP
   * function if requested. Used for code dedupe.
   * @param obj The object to serialize
   * @return A ChannelBuffer to pass on to the query
   * @throws JSONException if serialization failed
   */
  private ChannelBuffer serializeJSON(final Object obj) {
    if (query.hasQueryStringParam("jsonp")) {
      return ChannelBuffers.wrappedBuffer(
          JSON.serializeToJSONPBytes(query.getQueryStringParam("jsonp"), 
          obj));
    }
    return ChannelBuffers.wrappedBuffer(JSON.serializeToBytes(obj));
  }
}

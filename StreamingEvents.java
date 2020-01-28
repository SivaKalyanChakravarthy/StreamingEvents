package com.siemens.pd;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Iterator;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsWithStateFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.GroupState;
import org.apache.spark.sql.streaming.GroupStateTimeout;
import org.apache.spark.sql.streaming.StreamingQuery;

/**
 * You are receiving a stream of event data that contains a session identifier; explain how you
 * would build a service or process to sessionize data, i.e. summarize the given events down to a
 * single record per session. Assume that every session sends a start event and some arbitrary,
 * variable number of intermediate events, but that only ~90% send an end event.
 * 
 * <hostname> and <port> describe the TCP server that this process would connect to receive data.
 * 
 * @author Siva Kalyan Chakravarthy
 *
 */
public class StreamingEvents {

  private static final String SESSION_TIME_OUT = "30 minutes";
  private static final String LATE_ARRIVAL_TIME_OUT = "10 minutes";

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: StreamingEvents <hostname> <port>");
      System.exit(1);
    }

    String host = args[0];
    int port = Integer.parseInt(args[1]);

    // Create the spark session object
    SparkSession sparkSession =
        SparkSession.builder().master("local").appName("StreamingEvents").getOrCreate();

    sparkSession.sparkContext().setLogLevel("ERROR");

    // Create Dataframe out of the lines read from the connection
    Dataset<Row> dsRow =
        sparkSession.readStream().format("socket").option("host", host).option("port", port).load();

    // Split the lines and create Event Dataset
    Dataset<Event> events = dsRow.as(Encoders.STRING()).map(r -> {
      String[] cols = r.split(",");
      Double val = StringUtils.isNotBlank(cols[2]) ? Double.valueOf(cols[2]) : 0.0;
      Timestamp ts = Timestamp.valueOf(cols[0]);
      return new Event(ts, cols[1], val, cols[3]);
    }, Encoders.bean(Event.class));

    // Sessionize the events and calculate the total value to report a summarized session update.
    // Defining the State update function
    MapGroupsWithStateFunction<String, Event, SessionInfo, SessionUpdate> funcForStateUpdate =
        new MapGroupsWithStateFunction<String, Event, SessionInfo, SessionUpdate>() {
          @Override
          public SessionUpdate call(String sessionId, Iterator<Event> events,
              GroupState<SessionInfo> state) throws Exception {

            // If timed out, then remove session and send final update
            // This will help to close the session if endEvent flag is not received.
            // 10% of sessions with out endEvent will be handled here.
            if (state.hasTimedOut()) {
              SessionUpdate lastUpdate =
                  new SessionUpdate(sessionId, state.get().getTotalValue(), true);
              state.remove();
              return lastUpdate;

            } else {
              // Aggregate the values from the events
              double totalValue = 0;
              boolean isLastEvent = false;
              while (events.hasNext()) {
                Event e = events.next();
                totalValue += e.getValue();
                isLastEvent = StringUtils.isNotBlank(e.getEndingEvent());
              }
              SessionInfo updatedSession = new SessionInfo();

              // Update the total value in session info
              if (state.exists()) {
                SessionInfo oldSession = state.get();
                updatedSession.setTotalValue(oldSession.getTotalValue() + totalValue);
              } else {
                updatedSession.setTotalValue(totalValue);
              }
              state.update(updatedSession);
              
              // Setting timeout to end the session incase endEvent flag is not received.
              // poor session time out will end the session even though end flag comes after delayed
              // time.
              state.setTimeoutDuration(SESSION_TIME_OUT);
              
              // close the session if endEvent flag is received
              // 90% of sessions with endEvent flag is handled here
              if (isLastEvent) {
                state.remove();
                return new SessionUpdate(sessionId, updatedSession.getTotalValue(), true);
              } else
                return new SessionUpdate(sessionId, updatedSession.getTotalValue(), false);
            }
          }

        };

    // Apply the state update function to the events dataset grouped by sessionId
    // using withWatermark, late arriving events with in the threshold time of LATE_ARRIVAL_TIME_OUT
    // are aggregated. but data later than the threshold will start getting dropped.
    //
    Dataset<SessionUpdate> sessionUpdates = events.withWatermark("eventTime", LATE_ARRIVAL_TIME_OUT)
        .groupByKey(new MapFunction<Event, String>() {
          @Override
          public String call(Event event) {
            return event.getSessionId();
          }
        }, Encoders.STRING())
        .mapGroupsWithState(funcForStateUpdate, Encoders.bean(SessionInfo.class),
            Encoders.bean(SessionUpdate.class), GroupStateTimeout.ProcessingTimeTimeout());

    // Start running the query that prints the session updates to the console
    StreamingQuery query =
        sessionUpdates.writeStream().outputMode("update").format("console").start();

    query.awaitTermination();

  }

  /**
   * User-defined data type representing the input events
   */
  public static class Event implements Serializable {
    private Timestamp eventTime;
    private String sessionId;
    private Double value;
    private String endingEvent;

    public Event() {}

    public Event(Timestamp eventTime, String sessionId, Double value, String endingEvent) {
      this.sessionId = sessionId;
      this.value = value;
      this.endingEvent = endingEvent;
      this.eventTime = eventTime;
    }

    public String getSessionId() {
      return sessionId;
    }

    public void setSessionId(String sessionId) {
      this.sessionId = sessionId;
    }

    public Double getValue() {
      return value;
    }

    public void setValue(Double value) {
      this.value = value;
    }

    public String getEndingEvent() {
      return endingEvent;
    }

    public void setEndingEvent(String endingEvent) {
      this.endingEvent = endingEvent;
    }

    public Timestamp getTimestamp() {
      return eventTime;
    }

    public void setTimestamp(Timestamp timestamp) {
      this.eventTime = timestamp;
    }
  }

  /**
   * User-defined data type for storing a session information as state in mapGroupsWithState.
   */
  public static class SessionInfo implements Serializable {
    private double totalValue = 0;

    public SessionInfo() {
      super();
    }

    public double getTotalValue() {
      return totalValue;
    }

    public void setTotalValue(double totalValue) {
      this.totalValue = totalValue;
    }

    public SessionInfo(int totalValue) {
      super();
      this.totalValue = totalValue;
    }

    @Override
    public String toString() {
      return "SessionInfo [totalValue=" + totalValue + "]";
    }

  }

  /**
   * User-defined data type representing the update information returned by mapGroupsWithState.
   */
  public static class SessionUpdate implements Serializable {
    private String id;
    private double totalValue;
    private boolean expired;

    public SessionUpdate(String id, double totalValue, boolean expired) {
      super();
      this.id = id;
      this.totalValue = totalValue;
      this.expired = expired;
    }

    public double getTotalValue() {
      return totalValue;
    }

    public void setTotalValue(double totalValue) {
      this.totalValue = totalValue;
    }

    public SessionUpdate() {}

    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }

    public boolean isExpired() {
      return expired;
    }

    public void setExpired(boolean expired) {
      this.expired = expired;
    }
  }

}

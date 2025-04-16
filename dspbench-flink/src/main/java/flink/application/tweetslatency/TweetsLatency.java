package flink.application.tweetslatency;

import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import flink.application.AbstractApplication;
import flink.constants.TweetsLatencyConstants;
import flink.parsers.TweetsLatencyParser;


public class TweetsLatency extends AbstractApplication {

    public class MetadataGenerator implements FlatMapFunction<TweetsLatencyEvent, TweetsLatencyMetadata> {

        private final Configuration config;

        public MetadataGenerator(Configuration config) {
            this.config = config;
        }

        @Override
        public void flatMap(TweetsLatencyEvent event, Collector<TweetsLatencyMetadata> out) throws Exception {
            List<Integer> durations = event.getDurations();  
            for (int i = 0; i < durations.size(); i++) {
                int duration = durations.get(i);
                // Create a metadata object with the event id, duration and initially null content.
                TweetsLatencyMetadata metadata = new TweetsLatencyMetadata(event.getId(), duration, null);
                out.collect(metadata);
            }
        }
    }

    public class MetadataCollector implements MapFunction<TweetsLatencyMetadata, TweetsLatencyMetadata> {

        private final Configuration config;

        public MetadataCollector(Configuration config) {
            this.config = config;
        }

        @Override
        public TweetsLatencyMetadata map(TweetsLatencyMetadata metadata) throws Exception {
            int delay = metadata.getDuration();
            // Simulate delay (for demonstration only; avoid blocking calls like Thread.sleep in production Flink jobs)
            Thread.sleep(delay);
            // Generate a string content (you could use any logic for generating content)
            String content = "Collected metadata for event " + metadata.getEventId() + " with delay " + delay;
            // Return a new metadata instance with the content populated
            return new TweetsLatencyMetadata(metadata.getEventId(), delay, content);
        }
    }

    public class MetadataWrapper implements MapFunction<TweetsLatencyMetadata, TweetsLatencyEvent> {
        
        // A lookup map that holds the original TweetsLatencyEvent keyed by eventId.
        private final Map<Integer, TweetsLatencyEvent> eventLookup;
        
        public MetadataWrapper(Map<Integer, TweetsLatencyEvent> eventLookup) {
            this.eventLookup = eventLookup;
        }
        
        @Override
        public TweetsLatencyEvent map(TweetsLatencyMetadata metadata) throws Exception {
            // Retrieve the original event using the event ID
            TweetsLatencyEvent originalEvent = this.eventLookup.get(metadata.getEventId());
            if (originalEvent == null) {
                // If no matching event is found, use default values
                originalEvent = new TweetsLatencyEvent(
                        metadata.getEventId(),
                        "unknown",
                        "unknown"
                );
            }

            // Create a new TweetsLatencyEvent carrying the original fields (authorName and content)
            TweetsLatencyEvent wrappedEvent = new TweetsLatencyEvent(
                    originalEvent.getId(),
                    originalEvent.getAuthorName(),
                    originalEvent.getContent()
            );
            
            // Add the metadata into the event's list of metadata elements
            wrappedEvent.getMetadatas().add(metadata);
            return wrappedEvent;
        }
    }

    public class MetadataReducer implements ReduceFunction<TweetsLatencyEvent> {

        private final Configuration config;

        public MetadataReducer(Configuration config) {
            this.config = config;
        }

        @Override
        public TweetsLatencyEvent reduce(TweetsLatencyEvent event1, TweetsLatencyEvent event2) throws Exception {
            // Combine the metadata lists from both events.
            event1.getMetadatas().addAll(event2.getMetadatas());
            return event1;
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(TweetsLatency.class);

    private int parserThreads;
    private int splitterThreads;
    private int reducerThreads;

    public TweetsLatency(String appName, Configuration config) {
        super(appName, config);
    }

    @Override
    public void initialize() {
        this.parserThreads = config.getInteger(TweetsLatencyConstants.Conf.PARSER_THREADS, 1);
        this.splitterThreads = config.getInteger(TweetsLatencyConstants.Conf.SPLITTER_THREADS, 1);
        this.reducerThreads = config.getInteger(TweetsLatencyConstants.Conf.REDUCER_THREADS, 1);
    }

    @Override
    public StreamExecutionEnvironment buildApplication() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setLatencyTrackingInterval(10L);

        // Spout
        DataStream<String> spout = createSource("tweetslatency");

        // Parser
        DataStream<TweetsLatencyEvent> parser = spout
            .flatMap(new TweetsLatencyParser(this.config, "tweetslatency"))
            .setParallelism(this.parserThreads);

        // Build map of event ID to events
        Map<Integer, TweetsLatencyEvent> eventLookupMap = new HashMap<>();
        DataStream<TweetsLatencyEvent> eventsWithMapping = parser.map(new MapFunction<TweetsLatencyEvent, TweetsLatencyEvent>() {
            @Override
            public TweetsLatencyEvent map(TweetsLatencyEvent event) throws Exception {
                eventLookupMap.put(event.getId(), event);
                return event;
            }
        });

        // (1) Read the TweetsLatencyEvent stream and generate metadata.
        DataStream<TweetsLatencyMetadata> generated = eventsWithMapping.flatMap(new MetadataGenerator(this.config)).setParallelism(this.splitterThreads);

        // (2) Process each metadata with a simulated delay.
        DataStream<TweetsLatencyMetadata> collected = generated.map(new MetadataCollector(this.config));

        // (3) Wrap each metadata into an event (so that we can aggregate).
        DataStream<TweetsLatencyEvent> eventsWrapped = collected.map(new MetadataWrapper(eventLookupMap));

        // (4) Key by event id and reduce (aggregating metadata into one event).
        DataStream<TweetsLatencyEvent> reduced = eventsWrapped.keyBy(event -> event.getId()).reduce(new MetadataReducer(this.config)).setParallelism(this.reducerThreads);

        // (5) Convert the aggregated event to String.
        DataStream<String> formatted = reduced.map(event -> {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.writeValueAsString(event.toString());
        });

        // Sink
        createSinkTL(formatted);

        return env;
    }

    @Override
    public String getConfigPrefix() {
        return TweetsLatencyConstants.PREFIX;
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }   
}

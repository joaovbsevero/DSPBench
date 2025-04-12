package flink.parsers;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import flink.application.tweetslatency.TweetsLatencyEvent;
import flink.util.Configurations;
import flink.util.Metrics;

public class TweetsLatencyParser extends RichFlatMapFunction<String, TweetsLatencyEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(TweetsLatencyParser.class);

    Configuration config;
    String sourceName;

    Metrics metrics = new Metrics();

    public TweetsLatencyParser(Configuration config, String sourceName){
        metrics.initialize(config, this.getClass().getSimpleName()+"-"+sourceName);
        this.config = config;
        this.sourceName = sourceName;
    }

    @Override
    public void flatMap(String value, Collector<TweetsLatencyEvent>> out) throws Exception {
        metrics.initialize(config, this.getClass().getSimpleName()+"-"+sourceName);

        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.receiveThroughput();
        }

        String[] record = value.split("\t");
        if (record.length != 4 && record.length != 6) {
            out.collect(null);
            return;
        }

        String authorName = record[0];
        String authorEmail = record[1];
        String content = record[2];
        long createdAtTimestamp = Long.parseLong(record[3]);

        TweetsLatencyEvent event;
        if (record.length == 6) {
            int duration = Integer.parseInt(record[4]);
            int chunks = Integer.parseInt(record[5]);

            event = new TweetsLatencyEvent(authorName, authorEmail, content, createdAtTimestamp, duration, chunks);
        } else {
            event = new TweetsLatencyEvent(authorName, authorEmail, content, createdAtTimestamp);
        }

        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.emittedThroughput();
        }

        out.collect(event);
    }

    // close method
    @Override
    public void close() throws Exception {
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.SaveMetrics();
        }
    }
    
}

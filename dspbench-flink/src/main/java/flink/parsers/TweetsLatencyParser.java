package flink.parsers;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import flink.application.tweetslatency.TweetsLatencyEvent;
import flink.util.Configurations;
import flink.util.Metrics;

import java.util.List;
import java.util.Arrays;
import java.util.stream.Collectors;

public class TweetsLatencyParser implements FlatMapFunction<String, TweetsLatencyEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(TweetsLatencyParser.class);

    Configuration config;
    String sourceName;

    Metrics metrics = new Metrics();

    public TweetsLatencyParser(Configuration config, String sourceName){
        metrics.initialize(config, this.getClass().getSimpleName()+"-"+sourceName);
        this.config = config;
        this.sourceName = sourceName;
    }

    public void flatMap(String value, Collector<TweetsLatencyEvent> out) throws Exception {
        metrics.initialize(config, this.getClass().getSimpleName()+"-"+sourceName);

        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.receiveThroughput();
        }

        String[] record = value.split(",");
        if (record.length != 3 && record.length != 4) {
            out.collect(null);
            return;
        }

        int id = Integer.parseInt(record[0]);
        String authorName = record[1];
        String content = record[2];

        TweetsLatencyEvent event;
        if (record.length == 4) {
            List<Integer> durations = Arrays.stream(record[3].split(" "))
                                                        .map(Integer::parseInt)
                                                        .collect(Collectors.toList());

            event = new TweetsLatencyEvent(id, authorName, content, durations);
        } else {
            event = new TweetsLatencyEvent(id, authorName, content);
        }

        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.emittedThroughput();
        }

        out.collect(event);
    }       
}

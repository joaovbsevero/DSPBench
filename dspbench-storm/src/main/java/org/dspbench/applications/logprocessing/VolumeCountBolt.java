package org.dspbench.applications.logprocessing;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.collections.buffer.CircularFifoBuffer;
import org.apache.commons.lang3.mutable.MutableLong;
import org.dspbench.applications.wordcount.WordCountConstants;
import org.dspbench.bolt.AbstractBolt;
import org.dspbench.util.config.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.dspbench.applications.logprocessing.LogProcessingConstants.Conf;
import org.dspbench.applications.logprocessing.LogProcessingConstants.Field;

/**
 * This bolt will count number of log events per minute
 */
public class VolumeCountBolt extends AbstractBolt {
    private CircularFifoBuffer buffer;
    private Map<Long, MutableLong> counts;

    @Override
    public void initialize() {
        int windowSize = config.getInt(Conf.VOLUME_COUNTER_WINDOW, 60);
        
        buffer = new CircularFifoBuffer(windowSize);
        counts = new HashMap<>(windowSize);
    }

    @Override
    public void execute(Tuple input) {
        if (!config.getBoolean(Configuration.METRICS_ONLY_SINK, false)) {
            recemitThroughput();
        }
        long minute = input.getLongByField(Field.TIMESTAMP_MINUTES);
        
        MutableLong count = counts.get(minute);
        
        if (count == null) {
            if (buffer.isFull()) {
                long oldMinute = (Long) buffer.remove();
                counts.remove(oldMinute);
            }
            
            count = new MutableLong(1);
            counts.put(minute, count);
            buffer.add(minute);
        } else {
            count.increment();
        }
        
        collector.emit(input, new Values(minute, count.longValue()));
        collector.ack(input);

    }

    @Override
    public void cleanup() {
        if (!config.getBoolean(Configuration.METRICS_ONLY_SINK, false)) {
            SaveMetrics();
        }
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.TIMESTAMP_MINUTES, Field.COUNT);
    }
}

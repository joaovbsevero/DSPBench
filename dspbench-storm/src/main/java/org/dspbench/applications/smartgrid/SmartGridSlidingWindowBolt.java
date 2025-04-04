package org.dspbench.applications.smartgrid;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import java.util.List;
import static org.dspbench.applications.smartgrid.SmartGridConstants.*;

import org.dspbench.bolt.AbstractBolt;
import org.dspbench.util.config.Configuration;
import org.dspbench.applications.smartgrid.window.SlidingWindowCallback;
import org.dspbench.applications.smartgrid.window.SlidingWindowEntry;
import org.dspbench.applications.smartgrid.SmartGridConstants.Field;
import org.dspbench.applications.smartgrid.SmartGridConstants.Measurement;
import org.dspbench.applications.smartgrid.SmartGridConstants.SlidingWindowAction;
import org.dspbench.applications.smartgrid.window.SlidingWindow;

/**
 * Author: Thilina
 * Date: 11/22/14
 */
public class SmartGridSlidingWindowBolt extends AbstractBolt {
    private SlidingWindow window;

    @Override
    public void initialize() {
        window = new SlidingWindow(1 * 60 * 60);
    }

    @Override
    public void cleanup() {
        if (!config.getBoolean(Configuration.METRICS_ONLY_SINK, false)) {
            SaveMetrics();
        }
    }

    @Override
    public void execute(Tuple tuple) {
        if (!config.getBoolean(Configuration.METRICS_ONLY_SINK, false)) {
            receiveThroughput();
        }
        int type = tuple.getIntegerByField(Field.PROPERTY);
        
        // we are interested only in load
        if (type == Measurement.WORK) {
            return;
        }
        
        SlidingWindowEntryImpl windowEntry = new SlidingWindowEntryImpl(
                tuple.getLongByField(Field.TIMESTAMP), tuple.getDoubleByField(Field.VALUE),
                tuple.getStringByField(Field.HOUSE_ID), tuple.getStringByField(Field.HOUSEHOLD_ID),
                tuple.getStringByField(Field.PLUG_ID));
        
        window.add(windowEntry, new SlidingWindowCallback() {
            @Override
            public void remove(List<SlidingWindowEntry> entries) {
                for (SlidingWindowEntry e : entries) {
                    SlidingWindowEntryImpl entry = (SlidingWindowEntryImpl) e;
                    if (!config.getBoolean(Configuration.METRICS_ONLY_SINK, false)) {
                        emittedThroughput();
                    }
                    collector.emit(new Values(entry.ts, entry.houseId, entry.houseHoldId,
                            entry.plugId, entry.value, SlidingWindowAction.REMOVE));
                }
            }
        });
        if (!config.getBoolean(Configuration.METRICS_ONLY_SINK, false)) {
            emittedThroughput();
        }
        collector.emit(new Values(windowEntry.ts, windowEntry.houseId, windowEntry.houseHoldId,
                windowEntry.plugId, windowEntry.value, SlidingWindowAction.ADD));
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.TIMESTAMP, Field.HOUSE_ID, Field.HOUSEHOLD_ID,
                Field.PLUG_ID, Field.VALUE, Field.SLIDING_WINDOW_ACTION);
    }
    
    private class SlidingWindowEntryImpl implements SlidingWindowEntry {
        private String houseId;
        private String houseHoldId;
        private String plugId;
        private long ts;
        private double value;

        private SlidingWindowEntryImpl(long ts, double value, String houseId,
                String houseHoldId, String plugId) {
            this.ts = ts;
            this.value = value;
            this.houseId = houseId;
            this.houseHoldId = houseHoldId;
            this.plugId = plugId;
        }

        @Override
        public long getTime() {
            return ts;
        }
    }

}
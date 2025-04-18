package org.dspbench.core.hook;

import com.codahale.metrics.Counter;
import org.dspbench.core.Tuple;
import org.dspbench.core.Values;

/**
 *
 * @author mayconbordin
 */
public class TupleCounterHook extends Hook {
    private Counter inputCount;
    private Counter outputCount;

    public TupleCounterHook(Counter inputCount, Counter outputCount) {
        this.inputCount = inputCount;
        this.outputCount = outputCount;
    }

    @Override
    public void afterTuple(Tuple tuple) {
        inputCount.inc();
    }

    @Override
    public void onEmit(Values values) {
        outputCount.inc();
    }
    
}

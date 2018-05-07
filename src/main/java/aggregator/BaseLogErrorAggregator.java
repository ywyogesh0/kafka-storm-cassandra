package aggregator;

import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

public class BaseLogErrorAggregator extends BaseAggregator<BaseLogErrorAggregator.State> {

    @Override
    public State init(Object o, TridentCollector tridentCollector) {
        return new BaseLogErrorAggregator.State();
    }

    @Override
    public void aggregate(State state, TridentTuple tridentTuple, TridentCollector tridentCollector) {
        if (tridentTuple.getString(0).equals("ERROR"))
            ++state.count;
    }

    @Override
    public void complete(State state, TridentCollector tridentCollector) {
        tridentCollector.emit(new Values(new Object[]{Long.valueOf(state.count)}));
    }

    static class State {
        State() {
        }

        long count = 0;
    }
}

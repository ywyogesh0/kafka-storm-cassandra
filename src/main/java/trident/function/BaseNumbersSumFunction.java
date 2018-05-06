package trident.function;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

public class BaseNumbersSumFunction extends BaseFunction {

    @Override
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
        tridentCollector.emit(new Values(tridentTuple.getInteger(0) + tridentTuple.getInteger(1)));
    }
}

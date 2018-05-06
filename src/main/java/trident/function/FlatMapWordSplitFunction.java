package trident.function;

import org.apache.storm.trident.operation.FlatMapFunction;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;

public class FlatMapWordSplitFunction implements FlatMapFunction {

    @Override
    public Iterable<Values> execute(TridentTuple tridentTuple) {
        List<Values> values = new ArrayList<>();

        for (String word : tridentTuple.getString(0).split(" ")) {
            values.add(new Values(word));
        }

        return values;
    }
}
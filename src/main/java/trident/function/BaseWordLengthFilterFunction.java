package trident.function;

import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.tuple.TridentTuple;

public class BaseWordLengthFilterFunction extends BaseFilter {

    @Override
    public boolean isKeep(TridentTuple tridentTuple) {
        return tridentTuple.getString(0).length() > 0;
    }
}

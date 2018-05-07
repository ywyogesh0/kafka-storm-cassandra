package twitter;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;
import twitter4j.HashtagEntity;
import twitter4j.Status;

public class BaseTwitterHashTagFunction extends BaseFunction {

    @Override
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
        Status status = (Status) tridentTuple.get(0);

        for (HashtagEntity hashTag : status.getHashtagEntities()) {
            tridentCollector.emit(new Values(hashTag.getText()));
        }
    }
}

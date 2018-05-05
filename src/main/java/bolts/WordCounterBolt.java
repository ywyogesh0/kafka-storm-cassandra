package bolts;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public class WordCounterBolt extends BaseRichBolt {

    private Map<String, Integer> map;
    private OutputCollector outputCollector;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.map = new HashMap<>();
        this.outputCollector = outputCollector;
    }

    public void execute(Tuple tuple) {
        String word = tuple.getStringByField("word");

        if (map.containsKey(word))
            map.put(word, map.get(word) + 1);
        else
            map.put(word, 1);

        outputCollector.emit(tuple, new Values(word, map.get(word)));
        outputCollector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word", "count"));
    }
}

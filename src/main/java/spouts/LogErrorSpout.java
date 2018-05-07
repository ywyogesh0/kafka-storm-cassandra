package spouts;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

public class LogErrorSpout extends BaseRichSpout {

    private Random random;
    private SpoutOutputCollector spoutOutputCollector;

    private static final int MAX_ERROR_NUMBER = 80;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
        this.random = new Random();
    }

    @Override
    public void nextTuple() {
        if (random.nextInt(100) <= MAX_ERROR_NUMBER)
            spoutOutputCollector.emit(new Values("SUCCESS"));
        else
            spoutOutputCollector.emit(new Values("ERROR"));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("log"));
    }
}

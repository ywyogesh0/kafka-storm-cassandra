package twitter;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class TwitterStreamingTopology {

    public static void main(String[] args) {

        Config config = new Config();
        config.setDebug(true);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("twitter-streaming-spout", new TwitterStreamingSpout());
        builder.setBolt("twitter-streaming-bolt", new TwitterStreamingBolt(), 2)
                .shuffleGrouping("twitter-streaming-spout");

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("twitter-streaming", config, builder.createTopology());
    }
}

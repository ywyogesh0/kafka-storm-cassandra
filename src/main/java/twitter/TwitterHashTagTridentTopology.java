package twitter;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.Debug;
import org.apache.storm.trident.operation.builtin.FirstN;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.tuple.Fields;

public class TwitterHashTagTridentTopology {

    public static void main(String[] args) throws InterruptedException {

        TridentTopology topology = new TridentTopology();

        topology.newStream("twitter-spout", new TwitterStreamingSpout())
                .each(new Fields("tweet"), new BaseTwitterHashTagFunction(), new Fields("hashTag"))
                .groupBy(new Fields("hashTag"))
                .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
                .newValuesStream()
                .applyAssembly(new FirstN(10, "count"))
                .each(new Fields("hashTag", "count"), new Debug());

        Config config = new Config();
        config.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("twitter-trident-hashtag-extractor", config, topology.build());

        Thread.sleep(30000);
        cluster.shutdown();
    }
}

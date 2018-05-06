package trident.topology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import trident.function.BaseWordLengthFilterFunction;
import trident.function.BaseWordLengthFunction;
import trident.function.FlatMapWordSplitFunction;
import trident.function.MapWordLowercaseFunction;

public class ComplexTridentStateTopology {

    public static void main(String[] args) throws InterruptedException {
        LocalDRPC drpc = new LocalDRPC();

        FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3,
                new Values("This is a very"),
                new Values("very short short Book"),
                new Values("cooL short Book"));

        spout.setCycle(true);

        TridentTopology topology = new TridentTopology();

        TridentState state = topology.newStream("fixed-batch-spout", spout)
                .map(new MapWordLowercaseFunction())
                .flatMap(new FlatMapWordSplitFunction())
                .filter(new BaseWordLengthFilterFunction())
                .groupBy(new Fields("sentence"))
                .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"));

        Config config = new Config();
        config.setDebug(true);

        topology.newDRPCStream("getCount", drpc)
                .stateQuery(state, new Fields("args"), new MapGet(), new Fields("count"));

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("complex-trident-state", config, topology.build());

        Thread.sleep(20000);

        for (String word : new String[]{"very", "short", "book"}) {
            System.out.println("Result for " + word + " = " + drpc.execute("getCount", word));
        }

        cluster.shutdown();
        drpc.shutdown();
    }

}

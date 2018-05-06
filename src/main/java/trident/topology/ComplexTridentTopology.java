package trident.topology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.tuple.Fields;
import trident.function.BaseWordLengthFilterFunction;
import trident.function.BaseWordLengthFunction;
import trident.function.FlatMapWordSplitFunction;
import trident.function.MapWordLowercaseFunction;

public class ComplexTridentTopology {

    public static void main(String[] args) throws InterruptedException {
        LocalDRPC drpc = new LocalDRPC();

        TridentTopology topology = new TridentTopology();
        topology.newDRPCStream("transformation", drpc)
                .map(new MapWordLowercaseFunction())
                .flatMap(new FlatMapWordSplitFunction())
                .filter(new BaseWordLengthFilterFunction())
                .groupBy(new Fields("args"))
                .aggregate(new Count(), new Fields("count"))
                .each(new Fields("args"), new BaseWordLengthFunction(), new Fields("word", "length"));

        Config config = new Config();
        config.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("complex-transformations-trident", config, topology.build());

        for (String word : new String[]{"This is", "A very VerY short short short Book", "cooL"}) {
            System.out.println("Result for " + word + " = " + drpc.execute("transformation", word));
        }

        Thread.sleep(10000);

        cluster.shutdown();
        drpc.shutdown();
    }

}

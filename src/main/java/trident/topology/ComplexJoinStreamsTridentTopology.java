package trident.topology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.tuple.Fields;
import spouts.NumberPairSpout;
import trident.function.*;

public class ComplexJoinStreamsTridentTopology {

    public static void main(String[] args) throws InterruptedException {

        TridentTopology topology = new TridentTopology();

        Stream source = topology.newStream("number-pair", new NumberPairSpout());

        Stream sum = source
                .each(new Fields("odd", "even"), new BaseNumbersSumFunction(), new Fields("sum"));
        Stream product = source
                .each(new Fields("odd", "even"), new BaseNumbersProductFunction(), new Fields("product"));

        // 2-tuple output for each pair-tuple merging
        topology.merge(sum, product)
                .peek(tridentTuple -> System.out.println("Merge Odd-Even Streams = " + tridentTuple));

        // 1-tuple output for each pair-tuple join
        topology.join(sum, new Fields("odd", "even"), product, new Fields("odd", "even"),
                new Fields("odd", "even", "sum", "product"))
                .peek(tridentTuple -> System.out.println("Join Odd-Even Streams = " + tridentTuple));

        Config config = new Config();
        config.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("join-streams-trident", config, topology.build());

        Thread.sleep(20000);
        cluster.shutdown();
    }

}

package trident;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.tuple.Fields;

public class BasicWordAppendTopology {

    public static void main(String[] args) throws InterruptedException {
        LocalDRPC drpc = new LocalDRPC();

        TridentTopology topology = new TridentTopology();
        topology.newDRPCStream("wordAppend", drpc)
                .each(new Fields("args"), new WordAppendFunction(), new Fields("processed_word"));

        Config config = new Config();
        config.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("basic-word-append-trident", config, topology.build());

        for (String word : new String[]{"yogesh wow !!!", "nanu awesome !!!", "cool"}) {
            System.out.println("Result for " + word + " = " + drpc.execute("wordAppend", word));
        }

        Thread.sleep(5000);
        cluster.shutdown();
    }

}

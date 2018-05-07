package drpc;

import bolts.Add10Bolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.drpc.LinearDRPCTopologyBuilder;

public class LocalDRPCTopology {

    public static void main(String[] args) throws InterruptedException {

        LocalDRPC drpc = new LocalDRPC();

        LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("sum");
        builder.addBolt(new Add10Bolt(), 3);

        Config config = new Config();
        config.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("local-drpc", config, builder.createLocalTopology(drpc));

        for (Integer num : new Integer[]{12, 22, 21}) {
            System.out.println("Result for " + num + " = " + drpc.execute("sum", String.valueOf(num)));
        }

        Thread.sleep(5000);

        cluster.shutdown();
        drpc.shutdown();
    }
}

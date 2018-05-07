package drpc;

import bolts.Add10Bolt;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.drpc.LinearDRPCTopologyBuilder;

import java.util.ArrayList;
import java.util.List;

public class RemoteDRPCTopology {

    public static void main(String[] args) throws Exception {

        LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("remote-drpc");
        builder.addBolt(new Add10Bolt(), 3);

        Config config = new Config();
        config.setDebug(true);

        List<String> drpcServers = new ArrayList<>();
        drpcServers.add("127.0.0.1");

        config.put(Config.DRPC_SERVERS, drpcServers);
        config.put(Config.DRPC_PORT, "3227");

        StormSubmitter.submitTopology("remote-drpc", config, builder.createRemoteTopology());
    }
}

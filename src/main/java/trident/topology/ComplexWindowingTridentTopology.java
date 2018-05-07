package trident.topology;

import aggregator.BaseLogErrorAggregator;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.Debug;
import org.apache.storm.trident.windowing.InMemoryWindowsStoreFactory;
import org.apache.storm.trident.windowing.WindowsStoreFactory;
import org.apache.storm.trident.windowing.config.*;
import org.apache.storm.tuple.Fields;
import spouts.LogErrorSpout;
import trident.function.BaseWordLengthFilterFunction;
import trident.function.BaseWordLengthFunction;
import trident.function.FlatMapWordSplitFunction;
import trident.function.MapWordLowercaseFunction;

import java.util.concurrent.TimeUnit;

public class ComplexWindowingTridentTopology {

    public static void main(String[] args) throws InterruptedException {

        /*WindowConfig windowConfig1 = SlidingCountWindow.of(100, 10);
        WindowConfig windowConfig2 = TumblingCountWindow.of(100);

        WindowConfig windowConfig3 = SlidingDurationWindow
                .of(new BaseWindowedBolt.Duration(1000, TimeUnit.MILLISECONDS),
                        new BaseWindowedBolt.Duration(100, TimeUnit.MILLISECONDS));*/

        WindowConfig windowConfig4 = TumblingDurationWindow
                .of(new BaseWindowedBolt.Duration(1000, TimeUnit.MILLISECONDS));

        WindowsStoreFactory windowsStoreFactory = new InMemoryWindowsStoreFactory();

        TridentTopology topology = new TridentTopology();

        topology.newStream("log-error-spout", new LogErrorSpout())
                .window(windowConfig4, windowsStoreFactory, new Fields("log"),
                        new BaseLogErrorAggregator(), new Fields("count"))
                .each(new Fields("count"), new Debug());

        Config config = new Config();
        config.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("windowing-trident", config, topology.build());

        Thread.sleep(20000);

        cluster.shutdown();
    }

}

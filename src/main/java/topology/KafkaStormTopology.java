package topology;

import bolts.CassandraCounterBolt;
import bolts.WordCounterBolt;
import bolts.WordSplitterBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class KafkaStormTopology {

    private final static String CASSANDRA_KEYSPACE_KEY = "CASSANDRA_KEYSPACE_KEY";
    private final static String CASSANDRA_TABLE_KEY = "CASSANDRA_TABLE_KEY";

    private final static String CASSANDRA_KEYSPACE_VALUE = "counter";
    private final static String CASSANDRA_TABLE_VALUE = "word_counter";

    public static void main(String[] args) {

        String zookeeperHost = "localhost:2181";
        String topic = args[1];
        String zkRoot = args[2];
        String spoutId = args[3];

        BrokerHosts hosts = new ZkHosts(zookeeperHost);

        SpoutConfig spoutConfig = new SpoutConfig(hosts, topic, zkRoot, spoutId);
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConfig.startOffsetTime = -1;

        Config config = new Config();
        config.put(CASSANDRA_KEYSPACE_KEY,CASSANDRA_KEYSPACE_VALUE);
        config.put(CASSANDRA_TABLE_KEY, CASSANDRA_TABLE_VALUE);
        config.setDebug(true);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-storm", new KafkaSpout(spoutConfig));
        builder.setBolt("word-splitter", new WordSplitterBolt(), 2)
                .shuffleGrouping("kafka-storm");
        builder.setBolt("word-counter", new WordCounterBolt(), 2)
                .fieldsGrouping("word-splitter", new Fields("word"));
        builder.setBolt("cassandra-word-counter", new CassandraCounterBolt(), 2)
                .shuffleGrouping("word-counter");

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(args[0], config, builder.createTopology());
    }

}

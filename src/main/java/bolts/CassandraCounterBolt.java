package bolts;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public class CassandraCounterBolt extends BaseRichBolt {

    private Cluster cluster;
    private Session session;

    private String keyspaceName;
    private String tableName;

    private OutputCollector outputCollector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;

        System.out.println("Connecting Cassandra Cluster on 127.0.0.1...");
        cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        session = cluster.connect();
        System.out.println("Cassandra Cluster Connected Successfully...");

        keyspaceName = (String) map.get("CASSANDRA_KEYSPACE_KEY");
        tableName = (String) map.get("CASSANDRA_TABLE_KEY");
    }

    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getStringByField("word");
        Integer count = tuple.getIntegerByField("count");

        // Replacing ' -> '' ( Db Requirement )
        word = word.replaceAll("'", "''");

        String cqlStatement = "UPDATE " + keyspaceName + "." + tableName +
                " SET count=" + count + " WHERE word='" + word + "'";

        System.out.println("Executing CQL Statement .....");
        System.out.println(cqlStatement);

        session.execute(cqlStatement);
        outputCollector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}

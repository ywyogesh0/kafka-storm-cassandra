package twitter;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterStreamingSpout extends BaseRichSpout {

    private SpoutOutputCollector spoutOutputCollector;
    private TwitterStream twitterStream;
    private LinkedBlockingQueue<Status> linkedBlockingQueue;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;

        ConfigurationBuilder builder = new ConfigurationBuilder();
        builder.setDebugEnabled(true)
                .setOAuthConsumerKey("Kg1NZijUKorY7cpTfjHZd6uUG")
                .setOAuthConsumerSecret("W6fSjxlXXV02NNUfgHz5ZByRuA729Y6nF7a5b9Mb5gl5WaJEJ0")
                .setOAuthAccessToken("906410929272655873-GqS4k2kD18oJqZZZafTtjCq79R3HxRA")
                .setOAuthAccessTokenSecret("geQ2U80n0iBVc2w8e20V60UrdQwclrNaUDHD4ZAdWb5dU");

        this.twitterStream = new TwitterStreamFactory(builder.build()).getInstance();
        linkedBlockingQueue = new LinkedBlockingQueue<>();

        final StatusListener statusListener = new StatusListener() {
            @Override
            public void onStatus(Status status) {
                linkedBlockingQueue.offer(status);
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {

            }

            @Override
            public void onTrackLimitationNotice(int i) {

            }

            @Override
            public void onScrubGeo(long l, long l1) {

            }

            @Override
            public void onStallWarning(StallWarning stallWarning) {

            }

            @Override
            public void onException(Exception e) {

            }
        };

        final FilterQuery query = new FilterQuery();
        query.track(new String[]{"Avengers"});

        twitterStream.addListener(statusListener);
        twitterStream.filter(query);
    }

    @Override
    public void nextTuple() {
        Status status = linkedBlockingQueue.poll();
        if (status == null)
            Utils.sleep(5000);
        else
            spoutOutputCollector.emit(new Values(status));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("tweet"));
    }
}

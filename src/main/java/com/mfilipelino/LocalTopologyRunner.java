package com.mfilipelino;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

class CommitFeedListener extends BaseRichSpout {

    private SpoutOutputCollector outputCollector;
    private List<String> commits;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("commit"));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.outputCollector = collector;
        commits = FakeData.changeLog();
    }

    @Override
    public void nextTuple() {
        for (String commit : commits) {
            outputCollector.emit(new Values(commit));
        }
    }
}

class EmailExtractor extends BaseBasicBolt {

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String commit = input.getStringByField("commit");
        String[] parts = commit.split(" ");
        collector.emit(new Values(parts[1]));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("email"));
    }
}


class EmailCounter extends BaseRichBolt {

    private Map<String, Integer> counts;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        initMap();
    }

    private void initMap(){
        this.counts = new HashMap < String, Integer > ();
    }

    @Override
    public void execute(Tuple input) {
        String email = input.getStringByField("email");
        counts.put(email, countFor(email) + 1);
        printCounts();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // nada
    }

    private Integer countFor(String email){
        Integer count = counts.get(email);
        return count == null ? 0 : count;
    }

    private void printCounts(){
        for (String email: counts.keySet()){
            System.out.println(String.format("%s has count of %s", email, counts.get(email)));
        }
    }
}

public class LocalTopologyRunner{

    private static final int TEN_MINUTES = 600000;

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("commit-feed-listener", new CommitFeedListener());
        builder.setBolt("email-extractor", new EmailExtractor())
                .shuffleGrouping("commit-feed-listener");

        builder.setBolt("email-counter", new EmailCounter())
                .fieldsGrouping("email-extractor", new Fields("email"));

        Config config = new Config();
        config.setDebug(true);

        StormTopology topology = builder.createTopology();

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(
                "github-commit-topology",
                config,
                topology
        );

        Utils.sleep(TEN_MINUTES);
        cluster.killTopology("github-commit-topology");
        cluster.shutdown();
    }

}
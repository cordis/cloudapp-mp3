
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * This topology reads a file and counts the words in that file
 */
public class TopWordFinderTopologyPartB {

    public static void main(String[] args) throws Exception {
        Config config = new Config();
        config.setDebug(true);
        config.setMaxTaskParallelism(3);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new FileReaderSpout(args[0]), 1);
        builder.setBolt("split", new SplitSentenceBolt()).shuffleGrouping("spout");
        builder.setBolt("count", new WordCountBolt()).fieldsGrouping("split", new Fields("word"));

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("word-count", config, builder.createTopology());

        //wait for 2 minutes and then kill the job
        Thread.sleep(2 * 60 * 1000);

        cluster.shutdown();
    }
}

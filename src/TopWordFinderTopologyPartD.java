
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
 * This topology reads a file and counts the words in that file, then finds the top N words.
 */
public class TopWordFinderTopologyPartD {

    private static final int N = 10;

    public static void main(String[] args) throws Exception {
        Config config = new Config();
        config.setDebug(true);
        config.setMaxTaskParallelism(3);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new FileReaderSpout(args[0]), 1);
        builder.setBolt("split", new SplitSentenceBolt()).shuffleGrouping("spout");
        builder.setBolt("normalize", new NormalizerBolt()).shuffleGrouping("count");
        builder.setBolt("count", new WordCountBolt()).fieldsGrouping("split", new Fields("word"));
        builder.setBolt("top-n", new TopNFinderBolt(10)).noneGrouping("normalize");

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("word-count", config, builder.createTopology());

        //wait for 2 minutes and then kill the job
        Thread.sleep(2 * 60 * 1000);

        cluster.shutdown();
    }
}


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
 * This topology reads a file, splits the senteces into words, normalizes the words such that all words are
 * lower case and common words are removed, and then count the number of words.
 */
public class TopWordFinderTopologyPartC {

    public static void main(String[] args) throws Exception {
        Config config = new Config();
        config.setDebug(true);
        config.setMaxTaskParallelism(3);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new FileReaderSpout(args[0])).setNumTasks(1);
        builder.setBolt("split", new SplitSentenceBolt()).shuffleGrouping("spout");
        builder.setBolt("normalize", new NormalizerBolt()).shuffleGrouping("split");
        builder.setBolt("count", new WordCountBolt()).fieldsGrouping("normalize", new Fields("word"));

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("word-count", config, builder.createTopology());

        //wait for 2 minutes then kill the job
        Thread.sleep(2 * 60 * 1000);

        cluster.shutdown();
    }
}

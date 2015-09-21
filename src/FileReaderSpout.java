
import java.io.*;
import java.util.Map;
import java.util.Scanner;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class FileReaderSpout implements IRichSpout {
    private final String filepath;
    private SpoutOutputCollector collector;
    private TopologyContext context;
    private Scanner fileScanner;

    public FileReaderSpout(String filepath) {
        this.filepath = filepath;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.context = context;
        this.collector = collector;
        this.fileScanner = this.makeFileScanner(this.filepath);
    }

    private Scanner makeFileScanner(String filepath) {
        try {
            return new Scanner(new File(filepath));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void nextTuple() {
        if (this.fileScanner.hasNext()) {
            this.collector.emit(new Values(this.fileScanner.nextLine()));
        }
        else {
            try {
                Thread.sleep(2 * 60 *1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

    @Override
    public void close() {
        this.fileScanner.close();
    }


    @Override
    public void activate() {
    }

    @Override
    public void deactivate() {
    }

    @Override
    public void ack(Object msgId) {
    }

    @Override
    public void fail(Object msgId) {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}

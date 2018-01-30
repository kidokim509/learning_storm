package olaf.kido.wordcount;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import olaf.kido.Utils;

import java.util.Map;

public class SentenceSpout extends BaseRichSpout{
    private SpoutOutputCollector collector;
    private String[] sentences = {
      "my dog has fleas",
      "i like cold beverages",
      "the dog ate my homework",
      "don't have a cow man",
      "i don't think i like fleas"
    };
    private int index = 0;

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    public void nextTuple() {
        this.collector.emit(new Values(sentences[index % sentences.length]), index);
        index++;
        /*if (index >= sentences.length) {
            index = 0;
        }*/
        Utils.waitForMillis(1);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("sentence"));
    }

    @Override
    public void ack(Object msgId) {
        System.out.println("============================== acked: " + msgId + "(index: " + index + ")");
    }

    @Override
    public void fail(Object msgId) {
        System.out.println("============================== failed: " + msgId + "(index: " + index + ")");
    }
}

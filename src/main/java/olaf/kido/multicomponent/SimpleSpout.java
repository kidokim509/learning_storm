package olaf.kido.multicomponent;

import olaf.kido.Utils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class SimpleSpout extends BaseRichSpout {
    SpoutOutputCollector collector;
    TopologyContext context;
    Integer[] values = {1,2,3,4,5};
    int idx = 0;

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.context = topologyContext;
        this.collector = spoutOutputCollector;
    }

    public void nextTuple() {
        if (idx < values.length) {
            collector.emit(new Values(idx, context.getThisComponentId() + "(" + values[idx] + ")"));
        }
        Utils.waitForMillis(1);
        idx++;
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("key", "value"));
    }
}

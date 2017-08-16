package olaf.kido.multicomponent;

import olaf.kido.Utils;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class MultiSpoutAndBolt {
    private static final String SPOUT1_ID = "spout1";
    private static final String SPOUT2_ID = "spout2";
    private static final String BOLT1_ID = "bolt1";
    private static final String BOLT2_ID = "bolt2";
    private static final String TOPOLOGY_NAME = "multispoutandbolt-topology";

    public static void main(String[] args) {
        SimpleSpout simpleSpout1 = new SimpleSpout();
        SimpleSpout simpleSpout2 = new SimpleSpout();
        PrintBolt bolt1 = new PrintBolt();
        PrintBolt bolt2 = new PrintBolt();

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(SPOUT1_ID, simpleSpout1);
        builder.setSpout(SPOUT2_ID, simpleSpout2);
        builder.setBolt(BOLT1_ID, bolt1)
                .shuffleGrouping(SPOUT1_ID);
        builder.setBolt(BOLT2_ID, bolt2)
                .shuffleGrouping(SPOUT2_ID);

        Config config = new Config();

        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
        Utils.waitForSeconds(10);
        cluster.killTopology(TOPOLOGY_NAME);
        cluster.shutdown();
    }
}

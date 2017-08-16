package olaf.kido.multicomponent;

import olaf.kido.Utils;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class MultiBoltTopology {
    private static final String SPOUT_ID = "spout";
    private static final String BOLT1_ID = "bolt1";
    private static final String BOLT2_ID = "bolt2";
    private static final String TOPOLOGY_NAME = "multibolt-topology";

    public static void main(String[] args) {
        SimpleSpout simpleSpout = new SimpleSpout();
        PrintBolt bolt1 = new PrintBolt();
        PrintBolt bolt2 = new PrintBolt();

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(SPOUT_ID, simpleSpout);
        builder.setBolt(BOLT1_ID, bolt1)
                .shuffleGrouping(SPOUT_ID);
        builder.setBolt(BOLT2_ID, bolt2)
                .shuffleGrouping(SPOUT_ID);

        Config config = new Config();

        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
        Utils.waitForSeconds(10);
        cluster.killTopology(TOPOLOGY_NAME);
        cluster.shutdown();
    }
}

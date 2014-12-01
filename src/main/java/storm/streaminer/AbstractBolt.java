package storm.streaminer;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import java.util.Map;

/**
 *
 * @author mayconbordin
 */
public abstract class AbstractBolt extends BaseRichBolt {
    private Fields outputFields;
    private OutputCollector collector;
            
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(outputFields);
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public void setOutputFields(Fields outputFields) {
        this.outputFields = outputFields;
    }

    @Override
    public void execute(Tuple input) {
        execute(input, collector);
        collector.ack(input);
    }

    public abstract void execute(Tuple input, OutputCollector collector);
}

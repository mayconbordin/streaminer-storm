package storm.streaminer;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.streaminer.stream.sampler.ISampleList;
import storm.streaminer.util.TupleHelpers;

/**
 * For each tuple received adds it to the sampler and at periodic intervals (ticks)
 * emits all tuples included in the sample.
 * 
 * @author mayconbordin
 */
public class PeriodicSampler extends AbstractBolt {
    private final ISampleList<Tuple> sampler;
    private final int emitFrequencyInSeconds;
    
    /**
     * @param sampler An instance of a sampler to be used
     * @param emitFrequencyInSeconds The frequency of emits in seconds
     */
    public PeriodicSampler(ISampleList<Tuple> sampler, int emitFrequencyInSeconds) {
        this.sampler = sampler;
        this.emitFrequencyInSeconds = emitFrequencyInSeconds;
    }

    @Override
    public void execute(Tuple input, OutputCollector collector) {
        if (TupleHelpers.isTickTuple(input)) {
            Collection<Tuple> tuples = sampler.getSamples();
            
            for (Tuple tuple : tuples) {
                collector.emit(tuples, tuple.getValues());
            }
        } else {
            sampler.sample(input);
        }
    }
    
    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencyInSeconds);
        return conf;
    }
}

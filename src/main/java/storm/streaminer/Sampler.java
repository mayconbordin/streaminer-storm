package storm.streaminer;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import org.streaminer.stream.sampler.ISampler;

/**
 * For each tuple received decides if the tuple should continue (be re-emitted)
 * or not based on the given sampler.
 * 
 * @author mayconbordin
 */
public class Sampler extends AbstractBolt {
    private final ISampler sampler;
    
    /**
     * @param sampler An instance of a sampler to be used
     */
    public Sampler(ISampler sampler) {
        this.sampler = sampler;
    }

    @Override
    public void execute(Tuple input, OutputCollector collector) {
        if (sampler.next()) {
            collector.emit(input, input.getValues());
        }
    }
}

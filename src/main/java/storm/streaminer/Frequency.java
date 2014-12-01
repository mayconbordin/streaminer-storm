package storm.streaminer;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streaminer.stream.frequency.FrequencyException;
import org.streaminer.stream.frequency.ISimpleFrequency;

/**
 * For each tuple received includes it in the frequency data structure and re-emits
 * the tuple appending at the end the value of the frequency of the tuple.
 * 
 * @author mayconbordin
 * @param <T> The type of the key to be stored
 */
public class Frequency<T> extends AbstractBolt {
    private static final Logger LOG = LoggerFactory.getLogger(Frequency.class);
    private ISimpleFrequency<T> frequency;
    private int keyIndex;

    /**
     * Create a new frequency bolt using as keyIndex the first field (0).
     * @param frequency The algorithm to be used to calculate the frequency of items
     */
    public Frequency(ISimpleFrequency<T> frequency) {
        this(frequency, 0);
    }

    /**
     * @param frequency The algorithm to be used to calculate the frequency of items
     * @param keyIndex The key of the field to be used as key
     */
    public Frequency(ISimpleFrequency<T> frequency, int keyIndex) {
        this.frequency = frequency;
        this.keyIndex = keyIndex;
    }

    @Override
    public void execute(Tuple input, OutputCollector collector) {
        T key = (T) input.getValue(keyIndex);
        
        try {
            frequency.add(key);
        } catch (FrequencyException ex) {
            LOG.error(ex.getMessage(), ex);
        }
        
        List<Object> values = input.getValues();
        values.add(frequency.estimateCount(key));
        collector.emit(input, values);
    }
    
}

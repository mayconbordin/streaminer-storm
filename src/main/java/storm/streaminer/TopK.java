package storm.streaminer;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streaminer.stream.frequency.FrequencyException;
import org.streaminer.stream.frequency.topk.ITopK;
import org.streaminer.stream.frequency.util.CountEntry;
import storm.streaminer.util.TupleHelpers;

/**
 * For each tuple received adds it to the TopK data structure and at periodic intervals (ticks)
 * emits the top K items.
 * 
 * @author mayconbordin
 * @param <T> The type of the key to be stored
 */
public class TopK<T> extends AbstractBolt {
    private static final Logger LOG = LoggerFactory.getLogger(Frequency.class);
    
    private final ITopK<T> topK;
    private final int emitFrequencyInSeconds;
    private int keyIndex;
    private int keyValue;
    
    /**
     * @param topK An instance of a TopK to be used
     * @param emitFrequencyInSeconds The frequency of emits in seconds
     */
    public TopK(ITopK<T> topK, int emitFrequencyInSeconds) {
        this(topK, emitFrequencyInSeconds, 0, 10);
    }
    
    /**
     * @param topK An instance of a TopK to be used
     * @param emitFrequencyInSeconds The frequency of emits in seconds
     * @param keyIndex The key of the field to be used as key
     * @param keyValue The value of K, i.e. the number of items to emit
     */
    public TopK(ITopK<T> topK, int emitFrequencyInSeconds, int keyIndex, int keyValue) {
        this.topK = topK;
        this.emitFrequencyInSeconds = emitFrequencyInSeconds;
        this.keyIndex = keyIndex;
        this.keyValue = keyValue;
    }

    @Override
    public void execute(Tuple input, OutputCollector collector) {
        if (TupleHelpers.isTickTuple(input)) {
            List<CountEntry<T>> items = topK.peek(keyValue);

            for (CountEntry<T> item : items) {
                collector.emit(new Values(item.getItem(), item.getFrequency()));
            }
        } else {
            T key = (T) input.getValue(keyIndex);
            
            try {
                topK.add(key);
            } catch (FrequencyException ex) {
                LOG.error(ex.getMessage(), ex);
            }
        }
    }
    
    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencyInSeconds);
        return conf;
    }
}

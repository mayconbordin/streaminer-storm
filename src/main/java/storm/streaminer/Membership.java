package storm.streaminer;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import java.util.List;
import org.streaminer.stream.membership.IFilter;

/**
 * For each tuple received checks if it has been seen already and appends the
 * boolean result at the end of the tuple and re-sends it.
 * 
 * @author mayconbordin
 * @param <T> The type of the key to be used to check the membership
 */
public class Membership<T> extends AbstractBolt {
    private final IFilter<T> filter;
    private int keyIndex;

    /**
     * Create a membership bolt with keyIndex set to 0.
     * @param filter The filter to be used to check membership
     */
    public Membership(IFilter<T> filter) {
        this(filter, 0);
    }

    /**
     * Create a membership bolt.
     * @param filter The filter to be used to check membership
     * @param keyIndex The index of the key of the tuple
     */
    public Membership(IFilter<T> filter, int keyIndex) {
        this.filter = filter;
        this.keyIndex = keyIndex;
    }

    @Override
    public void execute(Tuple input, OutputCollector collector) {
        T key = (T) input.getValue(keyIndex);
        filter.add(key);
        
        List<Object> values = input.getValues();
        values.add(filter.membershipTest(key));
        collector.emit(input, values);
    }
}

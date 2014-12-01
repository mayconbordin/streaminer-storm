package storm.streaminer;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.streaminer.stream.cardinality.IBaseCardinality;

/**
 * Calculates the cardinality of one or more streams based on the key of the tuple,
 * indicated by the keyIndex value.
 * 
 * @author mayconbordin
 */
public class Cardinality extends AbstractBolt {
    private IBaseCardinality cardinality;
    private int keyIndex;

    /**
     * Create a new cardinality bolt using 0 as the keyIndex
     * @param cardinality The algorithm to be used to calculate the cardinality
     */
    public Cardinality(IBaseCardinality cardinality) {
        this(cardinality, 0);
    }

    /**
     * @param cardinality The algorithm to be used to calculate the cardinality
     * @param keyIndex The field to be used as key
     */
    public Cardinality(IBaseCardinality cardinality, int keyIndex) {
        this.cardinality = cardinality;
        this.keyIndex = keyIndex;
    }
    
    @Override
    public void execute(Tuple input, OutputCollector collector) {
        cardinality.offer(input.getValue(keyIndex));
        collector.emit(input, new Values(cardinality.cardinality()));
    }
    
}

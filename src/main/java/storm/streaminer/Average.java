package storm.streaminer;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.streaminer.stream.avg.IAverage;

/**
 * For each tuple received calculates the average of the given field and emits
 * the current average value.
 * 
 * @author mayconbordin
 */
public class Average extends AbstractBolt {
    private final IAverage average;
    private final int fieldIndex;

    /**
     * @param average The average algorithm to be used
     * @param fieldIndex The numeric field from the tuple used to calculate the average
     */
    public Average(IAverage average, int fieldIndex) {
        this.average = average;
        this.fieldIndex = fieldIndex;
    }

    @Override
    public void execute(Tuple input, OutputCollector collector) {
        Number n = (Number) input.getValue(fieldIndex);
        average.add(n.doubleValue());
        collector.emit(input, new Values(average.getAverage()));
    }
    
}

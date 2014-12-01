package storm.streaminer;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streaminer.stream.quantile.IQuantiles;
import org.streaminer.stream.quantile.QuantilesException;

/**
 * For each tuple received adds it to the quantiles data structure and emits
 * the selected quantiles.
 * 
 * @author mayconbordin
 * @param <T> The type of the key of the tuple
 */
public class Quantile<T> extends AbstractBolt {
    private static final Logger LOG = LoggerFactory.getLogger(Quantile.class);
    
    private final IQuantiles<T> quantile;
    private final int keyIndex;
    private final double[] quantiles;

    /**
     * @param quantile The algorithm to be used to calculate the quantiles
     * @param keyIndex The key field to identify the tuple
     * @param quantiles The quantiles to be emitted, values between 0 and 1
     */
    public Quantile(IQuantiles<T> quantile, int keyIndex, double[] quantiles) {
        this.quantile = quantile;
        this.keyIndex = keyIndex;
        this.quantiles = quantiles;
        
        setDefaultOutputFields();
    }
    
    @Override
    public void execute(Tuple input, OutputCollector collector) {
        T key = (T) input.getValue(keyIndex);
        quantile.offer(key);
        
        try {
            Values values = new Values();
            for (double q : quantiles) {
                values.add(quantile.getQuantile(q));
            }
            collector.emit(input, values);
        } catch (QuantilesException ex) {
            LOG.error(ex.getMessage(), ex);
        }
    }
    
    /**
     * Set the names of the output fields based on their values.
     * Ex.: quantiles {0.4, 0.6, 0.8} generate field names {"q0_4", "q0_6", "q0_8"}
     */
    public final void setDefaultOutputFields() {
        String[] fields = new String[quantiles.length];
        for (int i=0; i<quantiles.length; i++) {
            fields[i] = ("q" + quantiles[i]).replace(".", "_");
        }
        
        setOutputFields(new Fields(fields));
    }
}

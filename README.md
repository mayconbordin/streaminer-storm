# streaminer-storm


Adapter to use streaminer algorithms as Storm bolts

## Bolts

 - Average
 - Cardinality
 - Frequency
 - Membership
 - PeriodicSampler
 - Quantile
 - Sampler

## Example: Word Count

```java
TopologyBuilder builder = new TopologyBuilder();

builder.setSpout("spout", new RandomSentenceSpout(), 5);

builder.setBolt("split", new SplitSentence(), 8)
       .shuffleGrouping("spout");

LossyCounting<String> counter = new LossyCounting<String>(0.1);
builder.setBolt("count", new Frequency<String>(counter).setOutputFields("word", "count"), 12)
       .fieldsGrouping("split", new Fields("word"));
```

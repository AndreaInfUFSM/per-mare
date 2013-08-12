/* *************************************************************** *
 * PER-MARE Project (project number 13STIC07)
 * http://cosy.univ-reims.fr/~lsteffenel/per-mare
 * A CAPES/MAEE/ANII STIC-AmSud collaboration program.
 * All rigths reserved to project partners:
 *  - Universite de Reims Champagne-Ardenne, Reims, France 
 *  - Universite Paris 1 Pantheon Sorbonne, Paris, France
 *  - Universidade Federal de Santa Maria, Santa Maria, Brazil
 *  - Universidad de la Republica, Montevideo, Uruguay
 * 
 * *************************************************************** *
 */
package org.permare.hadoop.wordcounter;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.permare.util.MultiMap;
import org.permare.wordcounter.CounterExample;

/**
 * Word counter example using Hadoop.
 *
 * @author kirsch
 */
public class CounterExampleHadoop extends MapReduceBase
        implements Mapper<Object, Text, Text, LongWritable>,
        Reducer<Text, LongWritable, Text, LongWritable> {

    private CounterExample counter = new CounterExample();

    public CounterExample getCounter() {
        return counter;
    }

    public void setCounter(CounterExample counter) {
        this.counter = counter;
    }

    @Override
    public void map(Object k1, Text v1, OutputCollector<Text, LongWritable> oc, Reporter rprtr) throws IOException {
        //System.out.println("mapping " + v1.toString());
        MultiMap<String, Integer> mapping = this.counter.map(k1.toString(), v1.toString());
        this.multiMapToOutputCollector(mapping, oc);
    }

    @Override
    public void reduce(Text k2, Iterator<LongWritable> input, OutputCollector<Text, LongWritable> oc, Reporter rprtr) throws IOException {
        BridgeIterator iterator = new BridgeIterator(input);
        MultiMap<String, Integer> multimap = this.counter.reduce(k2.toString(), iterator);
        this.multiMapToOutputCollector(multimap, oc);
    }

    private void multiMapToOutputCollector(MultiMap<String, Integer> multimap, OutputCollector<Text, LongWritable> oc) throws IOException {
        //System.out.println("copying " + multimap.size() + " keys");
        for (String key : multimap.getKeys()) {
            Iterator<Integer> values = multimap.keyIterator(key);
            while (values.hasNext()) {
                oc.collect(new Text(key), new LongWritable(values.next()));
            }
        }
    }

    
    /**
     * bridge class that gives access to a Iterator&lt;LongWritable&gt; as a
     * Iterator&lt;Integer&gt;.
     */
    private class BridgeIterator implements Iterator<Integer> {

        Iterator<LongWritable> main;

        BridgeIterator(Iterator<LongWritable> mainIterator) {
            main = mainIterator;
        }

        @Override
        public boolean hasNext() {
            return main.hasNext();
        }

        @Override
        public Integer next() {
            return new Integer((int) main.next().get());
        }

        @Override
        public void remove() {
            main.remove();
        }
    }
}

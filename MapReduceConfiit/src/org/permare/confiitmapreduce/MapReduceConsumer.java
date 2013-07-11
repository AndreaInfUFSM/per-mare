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
package org.permare.confiitmapreduce;

import cloudfit.core.Distributed;
import java.io.Serializable;
import org.permare.util.MultiMap;

/**
 * Base class for Mapper and Reducer implementations over Confiit.
 *
 * @param K key type used by the accumulator
 * @param V value type used by the accumulator
 * 
 * @author kirsch
 */
public abstract class MapReduceConsumer<K, V> extends Distributed {

    private final boolean debug = true;
    
    
    
    

    /* consumes a block of data, adding it to the accumulator.
     * This method can be compared to a <i>combiner</i> into Hadoop. It groups
     * results from a block, putting them into a shared variable, the accumulator. 
     * 
     * @todo Explain somewhere what is a segment and a block
     * 
     * @param number block numeber in a segment (0 a <i>n</i> - 1).
     * @param value block content
     * @return MultiMap<K, V> containing the modified segment (the accumulator once updated)
     */
    public Serializable consumeBlock(int number, Serializable value) {
        if (debug) {
            System.out.println("## consumeBlock " + number + ", " + value);
        }

        if (number >= getNumberOfBlocks()) {
            return getAccumulator();
        }


        MultiMap<K, V> accumulator;
        try {
            accumulator = (MultiMap<K, V>) getAccumulator();
        } catch (ClassCastException ex) {
            accumulator = new MultiMap<K, V>();
        }

        accumulator.putAll ((MultiMap<K, V>) value);
        
        /*
        Set<K> keys = dataset.getKeys();

        // @TODO : optimise ! 
        Iterator<K> ikeys = keys.iterator();
        while (ikeys.hasNext()) {
            K key = ikeys.next();
            // anexar metodo a MultiMap para add multiplo
            Iterator<V> ivals = dataset.keyIterator(key);
            while (ivals.hasNext()) {
                accumulator.add(key, (V) ivals.next());
            }
        }
        */
        
        return accumulator;
    }

    /**
     * Consumer starter : initialises the task consumer component
     *
     * @return MultiMap accumulator for the tasks
     *
     * @todo Better (and understandable) javadoc
     */
    public Serializable initializeConsumer() {
        if (debug) {
            System.out.println("## initializeConsumer");
        }

        return new MultiMap<K, V>();
    }

    /**
     * Finalizes the consumer, returing its accumulator.
     *
     * @return MultiMap<K, V> accumulator with calculated data
     */
    public Serializable finalizeConsumer() {
        if (debug) {

            System.out.println("## finalizeConsumer ");

        }
        return getAccumulator();
    }

    public abstract int numberOfBlocks();

    public abstract Serializable produceBlock(int number, Serializable[] required);
}
/* *************************************************************** *
 * PER-MARE Project (project number 13STIC07)
 * http://cosy.univ-reims.fr/~lsteffenel/per-mare
 * A CAPES/MAEE/ANII STIC-AmSud collaboration program.
 * All rights reserved to project partners:
 *  - Universite de Reims Champagne-Ardenne, Reims, France 
 *  - Universite Paris 1 Pantheon Sorbonne, Paris, France
 *  - Universidade Federal de Santa Maria, Santa Maria, Brazil
 *  - Universidad de la Republica, Montevideo, Uruguay
 * 
 * *************************************************************** *
 */
package org.permare.cloudfitmapreduce;

import cloudfit.application.Distributed;
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

    private final boolean debug = false;
    
    
    
    

    /** consumes a block of data, adding it to the result_accumulator.
     * This method can be compared to a <i>combiner</i> into Hadoop. It groups
     * results from a block, putting them into a shared variable, the result_accumulator. 
     * 
     * @todo Explain somewhere what is a segment and a block
     * 
     * @param number block numeber in a segment (0 a <i>n</i> - 1).
     * @param value block content
     * @return MultiMap<K, V> containing the modified segment (the result_accumulator once updated)
     */
    public void consumeBlock(Serializable result_accumulator, int number, Serializable value) {
        if (debug) {
            System.out.println("## consumeBlock " + number + ", " + value);
        }

        
                ((MultiMap<K,V>)result_accumulator).putAll ((MultiMap<K, V>) value);

                
        // Removed as we changed the way accumulator works. 
        // now, instead of returning a variable, we directly modify the accumulator
        // Furthermore, as each task does this, we don't need to check if this is a valid task number
        
//        if (number >= getNumberOfBlocks()) {
//            return getAccumulator();
//        }


//        MultiMap<K, V> result_accumulator;
//        try {
//            result_accumulator = (MultiMap<K, V>) getAccumulator();
//        } catch (ClassCastException ex) {
//            result_accumulator = new MultiMap<K, V>();
//        }

        
        /*
        Set<K> keys = dataset.getKeys();

        // @TODO : optimise ! 
        Iterator<K> ikeys = keys.iterator();
        while (ikeys.hasNext()) {
            K key = ikeys.next();
            // anexar metodo a MultiMap para add multiplo
            Iterator<V> ivals = dataset.keyIterator(key);
            while (ivals.hasNext()) {
                result_accumulator.add(key, (V) ivals.next());
            }
        }
        */
        
        //return result_accumulator;

    
    
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

    //this method is now defined on the superclass
    //public abstract void setNumberOfBlocks(int nbBlocks);

    @Override
    public abstract void initNumberOfBlocks();

    @Override
    public abstract Serializable produceBlock(int number, Serializable[] required);
}
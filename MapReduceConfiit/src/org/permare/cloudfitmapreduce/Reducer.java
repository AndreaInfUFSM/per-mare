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
package org.permare.cloudfitmapreduce;

//import confiit.util.Context;
import cloudfit.util.MultiMap;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.permare.wordcounter.CounterExample;

/**
 * reducer class using Confiit
 *
 * @author kirsch
 */
public class Reducer extends MapReduceConsumer {

    private final boolean debug = false;
    MultiMap<String, Integer> accumulator = null;

    /**
     * Evaluates the number of blocks in the resource segment. This number must
     * be greater than 0.
     *
     * @return number of blocks in the segment
     */
    @Override
    public void initNumberOfBlocks() {
        int nb = 1;
        try {
            nb = Integer.parseInt(getArgs()[1]);  // Nombre de taches a renvoyer
            //nb = 2;
        } catch (Exception ex) {
            nb = 1;
        }
        this.setNumberOfBlocks(nb);
        //nbBlocks = nb;
        //return nb;
    }

    /**
     * Evaluates task <i>number</i> and returns task results. It performs the
     * reduce phase, grouping results are performed by consumeBlocks method
     *
     * @param number task number in the job (from 0 up to <i>nbTask</i> - 1).
     * @param required required blocks (results from prerequired tasks)
     * @return MultiMap<String, Integer> results
     */
    @Override
    public Serializable produceBlock(int number, Serializable[] required) {
        CounterExample counter = new CounterExample();
        MultiMap<String, Integer> partial = new MultiMap<String, Integer>();

        try {
            if (accumulator == null) {
                accumulator = (MultiMap<String, Integer>) getResults(getArgs()[0]);
            }
            //                long init = System.currentTimeMillis();
                            
            //System.out.println(accumulator.getKeys().size());
            int step = (int) Math.ceil(accumulator.getKeys().size() / getNumberOfBlocks());
            for (int i = number * step; i < Math.min((number + 1) * step, accumulator.getKeys().size()); ++i) {

                String key = accumulator.getKey(i);
                Collection<Integer> values = accumulator.getValues(key);

                MultiMap<String, Integer> stepmap = counter.reduce(key, values.iterator());
                //System.out.println("top");
                partial.putAll(stepmap);
                
            }
            //long end = System.currentTimeMillis();
            //System.out.println("reduce for task " + number + " = " + (end - init));

        } catch (Exception ex) {
            if (debug) {
                System.out.println("ERROR : impossible to perform reduce");
                ex.printStackTrace(System.out);
            }
        }
        return partial;

    }

    public Serializable getResults(String key) {
//        File file = Context.getResultFile(iid);
//        Serializable result = null;
//
//        try {
//            InputStream input = new FileInputStream(file);
//            ResultParsing parse = new ResultParsing(input);
//
//            result = parse.getResults();
//
//        } catch (FileNotFoundException fnfex) {
//            if (debug) {
//                System.out.println("ERROR : impossible to open file");
//                fnfex.printStackTrace(System.out);
//            }
//            result = null;
//
//        } catch (TasksCommunicationException tcex) {
//            if (debug) {
//                System.out.println("ERROR : impossible to parse file");
//                tcex.printStackTrace(System.out);
//            }
//            result = null;
//        }
//
//        return result;


        Serializable element = null;
        try {
            //use buffering
            InputStream file = new FileInputStream(key);
            InputStream buffer = new BufferedInputStream(file);
            ObjectInput input = new ObjectInputStream(buffer);
            try {
                //deserialize the List
                element = (Serializable) input.readObject();

            } finally {
                input.close();
            }
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(Reducer.class.getName()).log(Level.SEVERE, "Cannot perform deserializing", ex);
        } catch (IOException ex) {
            Logger.getLogger(Reducer.class.getName()).log(Level.SEVERE, "Cannot perform input", ex);
        }
        return element;
    }

// this code was mouved to the super class.     
//    @Override
//    public int getNumberOfBlocks() {
//        return nbBlocks;
//    }
//
//    @Override
//    public void setNumberOfBlocks(int nbBlocks) {
//        this.nbBlocks = nbBlocks;
//    }
}
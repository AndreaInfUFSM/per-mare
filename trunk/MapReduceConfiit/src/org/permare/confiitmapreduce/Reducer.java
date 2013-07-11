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

import confiit.util.Context;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Collection;
import org.permare.util.MultiMap;
import org.permare.wordcounter.CounterExample;

/**
 * reducer class using Confiit
 *
 * @author kirsch
 */
public class Reducer extends MapReduceConsumer {

    private final boolean debug = false;

    /**
     * Evaluates the number of blocks in the resource segment. This number must
     * be greater than 0.
     *
     * @return number of blocks in the segment
     */
    @Override
    public int numberOfBlocks() {
        int nb = 1;
        try {
            nb = Integer.parseInt(getArgs()[1]);  // Nombre de taches a renvoyer
            //nb = 2;
        } catch (Exception ex) {
            nb = 1;
        }

        return nb;
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
        MultiMap<String, Integer> map = new MultiMap<String, Integer>();

        try {
            MultiMap<String, Integer> accumulator = (MultiMap<String, Integer>) getResults(getArgs()[0]);

            int step = (int) Math.ceil(accumulator.getKeys().size() / numberOfBlocks());
            for (int i = number * step; i < Math.min((number + 1) * step, accumulator.getKeys().size()); ++i) {

                String key = accumulator.getKey(i);
                Collection<Integer> values = accumulator.getValues(key);

                MultiMap<String, Integer> stepmap = counter.reduce(key, values.iterator());

                map.putAll(stepmap);
            }
        } catch (Exception ex) {
            if (debug) {
                System.out.println("ERROR : impossible to perform reduce");
                ex.printStackTrace(System.out);
            }
        }
        return map;

    }


    public Serializable getResults(String iid) {
        File file = Context.getResultFile(iid);
        Serializable result = null;

        try {
            InputStream input = new FileInputStream(file);
            ResultParsing parse = new ResultParsing(input);

            result = parse.getResults();

        } catch (FileNotFoundException fnfex) {
            if (debug) {
                System.out.println("ERROR : impossible to open file");
                fnfex.printStackTrace(System.out);
            }
            result = null;

        } catch (TasksCommunicationException tcex) {
            if (debug) {
                System.out.println("ERROR : impossible to parse file");
                tcex.printStackTrace(System.out);
            }
            result = null;
        }

        return result;
    }
}
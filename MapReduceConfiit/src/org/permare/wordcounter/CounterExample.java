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
package org.permare.wordcounter;

import cloudfit.util.MultiMap;
import java.util.Iterator;

/**
 * CounterExample is a test class allowing counting words in a text. It can be
 * used for building both Hadoop applications and Confiit applications.
 *
 * @author kirsch
 */
public class CounterExample {

    private final static Integer ONE = new Integer(1);

    /**
     * Mapper function that searches for a word occurences in a document. Its
     * output is a set of <word, 1> pairs.
     *
     * @param key map entry key
     * @param value text that will be analysed by mapper
     * @return MultiMap containing &lt; word, &lt; 1,1,1...&gt; &gt; (with as 1
     * as occurrences of word)
     */
    public MultiMap<String, Integer> map(String key, String value) {
        MultiMap<String, Integer> map = new MultiMap<String, Integer>();

        // StringTokenizer is depreciated (and it isn't so performant neither)
        //replacing it by the appropriate String.split regular expression

        String []tokens = value.toString().split("\\W");

        //System.out.println("Spliting " + value + " into " + tokens.length);
        
        for (String wd : tokens) {    
            map.add(wd, ONE);
        }

        return map;
    }

    /**
     * Reduces a set of <word,1> pairs to a set <word, count> set of pairs.
     *
     * @param key word to be counted (reduced)
     * @param values values observed during Map phase
     * @return MultiMap set of <word, N> pairs in which N is the word counting
     */
    public MultiMap<String, Integer> reduce(String key, Iterator<Integer> values) {
        MultiMap<String, Integer> map = new MultiMap<String, Integer>();
        int sum = 0;
        
        //System.out.println("Reducing " + key );

        while (values.hasNext()) {
            sum += values.next().intValue();
        }

        map.add(key, new Integer(sum));

        return map;
    }
}
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
package org.permare.confiitmapreduce;

import confiit.Community;
import java.io.File;
import java.util.Iterator;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.permare.util.FileHandler;
//fixed import
//import org.permare.util.MultiMap;
import cloudfit.util.MultiMap;

public class CounterJob<K, V> {

    private String mapperClass;
    private String reducerClass;
    private String[] mapargs;
    private String outputDirectory;

    public CounterJob() {
        this.mapperClass = "";
        this.reducerClass = "";
    }

    public void setReducer(String classname) {
        this.reducerClass = classname;
    }

    public void setMapper(String classname) {
        this.mapperClass = classname;
    }

    public String getMapper() {
        return this.mapperClass;
    }

    public String getReducer() {
        return this.reducerClass;
    }

    public String[] getMapArguments() {
        return mapargs;
    }

    public void setMapArguments(String[] args) {
        this.mapargs = args;
    }

    public String getOutputDirectory() {
        return outputDirectory;
    }

    public void setOutputDirectory(String output) {
        this.outputDirectory = output;
    }

    public MultiMap<K, V> runJob() {
        Community community;            // Communaute Confiit
        String mapper;                  // Identifiant de l'instance Mapper
        MultiMap<K, V> intRes = null;

        try {
            community = new Community();

            mapper = this.runMapper(community);

            String[] reduceargs = new String[2];
            reduceargs[0] = mapper;
            // reduceargs[1] indique combien de tasks REDUCE seront créées
            reduceargs[1] = Integer.toString((community.getNodes()).length);
            Thread.sleep(1000);
            intRes = this.runReducer(community, reduceargs);

            this.saveOutput(intRes);

        } catch (Exception ex) {
            ex.printStackTrace(System.out);
        }

        return intRes;
    }

    private String runMapper(Community community) {
        String mapper = null;// Identifiant de l'instance Mapper
        try {
            // ici on indique la classe qui fera le MAP
            mapper = community.plug(this.getMapper(), this.getMapArguments());
            System.out.println(mapper);
            community.wait(mapper);
        } catch (Exception ex) {
            Logger.getLogger(CounterJob.class.getName()).log(Level.SEVERE, null, ex);
        }
        return mapper;

    }

    private MultiMap<K, V> runReducer(Community community, String[] reduceargs) {
        MultiMap<K, V> res = null;
        // ici on indique la classe qui fera le REDUCE
        try {
            String reducer = community.plug(this.getReducer(), reduceargs);
            community.wait(reducer);

            res = (MultiMap<K, V>) community.getResult(reducer, true);
        } catch (Exception ex) {
            Logger.getLogger(CounterJob.class.getName()).log(Level.SEVERE, null, ex);
        }
        return res;
    }

    private void saveOutput(MultiMap<K, V> intRes) {
        Set<K> keys = intRes.getKeys();
        FileHandler fhandler;
        File outfile, outdir;

        outdir = new File(this.getOutputDirectory());
        if (!outdir.exists()) {
            outdir.mkdir();
        }
        outfile = new File(this.getOutputDirectory().concat("/part-00000"));


        fhandler = new FileHandler(outfile);
        if (fhandler.open(FileHandler.WRITE)) {

            Iterator<K> ikeys = keys.iterator();
            while (ikeys.hasNext()) {
                K key = ikeys.next();
                Iterator<V> it = intRes.keyIterator(key);

                while (it.hasNext()) {
                    V group = it.next();
                    String line = String.format("%s = %s\n", key.toString(), group.toString());
                    fhandler.writeLine(line);
                }
            }

            fhandler.flushing();
            fhandler.close();
        }
    }

    public static void main(String[] args) {
        long start;
        long end;
        start = System.currentTimeMillis();

        CounterJob<String, Integer> job = new CounterJob<String, Integer>();
        try {
            job.setOutputDirectory(args[1]);
            job.setMapper("org.permare.confiitmapreduce.Mapper");
            //job.setMapper("Mapper");
            job.setMapArguments(args);

            job.setReducer("org.permare.confiitmapreduce.Reducer");
            //job.setReducer("Reducer");

            MultiMap<String, Integer> res = job.runJob();

            countTotal(res);

        } catch (Exception ex) {
            System.err.println(ex.getMessage());
        }
        end = System.currentTimeMillis();

        System.out.println("Total time = " + (end - start));

    }

    private static void countTotal(MultiMap<String, Integer> res) {
        Iterator<String> ikeys = res.getKeys().iterator();
        int count = 0;
        while (ikeys.hasNext()) {
            String key = (String) ikeys.next();
            Iterator<Integer> iValues = res.keyIterator(key);
            while (iValues.hasNext()) {
                Integer v = iValues.next();
                count += v.intValue();
            }
        }
        System.out.println("Count = " + count);
    }
}
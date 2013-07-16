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

import cloudfit.application.ApplicationInterface;
import cloudfit.core.Community;
import cloudfit.core.CoreORB;
import cloudfit.core.CoreQueue;
import cloudfit.network.EasyPastryAdapter;
import cloudfit.network.NetworkAdapterInterface;
import cloudfit.storage.SerializedDiskStorage;
import cloudfit.util.MultiMap;
import java.io.File;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.permare.util.FileHandler;

public class MRLauncher<K, V> {

    private ApplicationInterface mapperClass;
    private ApplicationInterface reducerClass;
    private String[] mapargs;
    private String outputDirectory;
    private Community community;

    public MRLauncher() {
        this.mapperClass = null;
        this.reducerClass = null;
    }

    public void setReducer(ApplicationInterface classname) {
        this.reducerClass = classname;
    }

    public void setMapper(ApplicationInterface classname) {
        this.mapperClass = classname;
    }

    public ApplicationInterface getMapper() {
        return this.mapperClass;
    }

    public ApplicationInterface getReducer() {
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
        Serializable mapper;                  // Result de l'instance Mapper
        MultiMap<K, V> intRes = null;

        ///////////////////// Pastry

        /* Declaration of the main class
         * all the internal initialization is made on the constructor
         */
        CoreORB TDTR = new CoreORB();

        /* Define if connecting to a peer or network discovery
         * 
         */
        CoreQueue queue = new CoreQueue();
        queue.start();
        TDTR.setQueue(queue);

        NetworkAdapterInterface P2P = null;
        if (mapargs.length > 2) {
            InetSocketAddress peer = new InetSocketAddress(mapargs[2], Integer.parseInt(mapargs[3]));
            P2P = new EasyPastryAdapter(queue, peer);

        } else {
            // if peer == null then launch discovery
            P2P = new EasyPastryAdapter(queue);
        }
        TDTR.setNetworkAdapter(P2P);
        
        TDTR.setStorage(new SerializedDiskStorage());



        /* creates a module to plug on the main class
         * and subscribe it to the messaging system
         */
        community = new Community(1, TDTR);

        TDTR.subscribe(community);
        
            ///////////////////////////////////////

            //try {
        
        try {
            System.out.println("a little sleep to ensure all nodes are connected");
            Thread.sleep(5000);
            System.out.println("starting");
        } catch (InterruptedException ex) {
            Logger.getLogger(MRLauncher.class.getName()).log(Level.SEVERE, null, ex);
        }
        intRes = (MultiMap<K, V>) this.runMapper(community);


            String[] reduceargs = new String[2];
            System.out.println("Fini !");
           //this.saveMapOutput(intRes);
            TDTR.save("map", intRes);

////            
            reduceargs[0] = "map";
            //reduceargs[1] indique combien de tasks REDUCE seront créées
            //reduceargs[1] = Integer.toString(community.getNodes());
            reduceargs[1] = Integer.toString(10);
//            Thread.sleep(1000);
            intRes = (MultiMap<K, V>) this.runReducer(community, reduceargs);
            TDTR.save("reduce", intRes);

//        } catch (Exception ex) {
//            ex.printStackTrace(System.out);
//        }

        return intRes;
    }

    private Serializable runMapper(Community community) {
        int mapperId = 0;// Identifiant de l'instance Mapper
        Serializable result = null;
        try {
            // ici on indique la classe qui fera le MAP
            mapperId = community.plug(this.getMapper(), this.getMapArguments());
            System.out.println("mapperId =" + mapperId);
            result = community.waitJob(mapperId);
        } catch (Exception ex) {
            Logger.getLogger(MRLauncher.class.getName()).log(Level.SEVERE, null, ex);
        }
        return result;

    }

    private Serializable runReducer(Community community, String[] reduceargs) {
        Serializable res = null;
        int reducerId = 0;
        // ici on indique la classe qui fera le REDUCE
        try {
            reducerId = community.plug(this.getReducer(), reduceargs);
            
            res = community.waitJob(reducerId);
        } catch (Exception ex) {
            Logger.getLogger(MRLauncher.class.getName()).log(Level.SEVERE, null, ex);
        }
        return res;
    }

    
    private void saveMapOutput(MultiMap<K, V> intRes) {
        Set<K> keys = intRes.getKeys();
        FileHandler fhandler;
        File outfile, outdir;

        outdir = new File(this.getOutputDirectory());
        if (!outdir.exists()) {
            outdir.mkdir();
        }
            outfile = new File(this.getOutputDirectory().concat("/temp-0000"));
        


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



        MRLauncher<String, Integer> job = new MRLauncher<String, Integer>();
        try {
            job.setOutputDirectory(args[1]);
            job.setMapper(new MapperArray());
            job.setMapArguments(args);

            job.setReducer(new Reducer());
            //job.setReducer("Reducer");

            MultiMap<String, Integer> res = job.runJob();
            
            job.saveOutput(res);
            
//            System.out.println("mapper2  =" + res);
//                // prints Mapper intermediate results
//
//                Set<String> keys = res.getKeys();
//                System.out.println("keys size = " + keys.size());
//                Iterator ikeys = keys.iterator();
//                while (ikeys.hasNext()) {
//                    String key = (String) ikeys.next();
//                    System.out.print(key + " - ");
//                    Iterator it = res.keyIterator(key);
//                    while (it.hasNext()) {
//                        System.out.print(it.next());
//                    }
//                    System.out.println("");
//                }

            //countTotal(res);

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
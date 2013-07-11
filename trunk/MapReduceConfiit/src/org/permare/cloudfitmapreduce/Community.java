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

import cloudfit.core.ActiveBlockingQueue;
import cloudfit.core.ORBInterface;
import cloudfit.core.ServiceInterface;
import cloudfit.test.TestSvcDataTransfer;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.permare.confiitmapreduce.MapReduceConsumer;
import org.permare.confiitmapreduce.Mapper;
import org.permare.util.MultiMap;


public class Community implements ServiceInterface {

    private long processId = 1;
    private ActiveBlockingQueue appQueue = null;
    private ORBInterface router = null;
    private LinkedList taskList = null;

    public Community(long pid, ORBInterface na) {
        this.processId = pid;
        this.appQueue = new ActiveBlockingQueue(this.processId, this);
        this.router = na;
    }

    public void put(Serializable obj) {
        try {
            this.appQueue.put(obj);
        } catch (InterruptedException ex) {
            Logger.getLogger(TestSvcDataTransfer.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public long getProcessId() {
        return processId;
    }

    @Override
    public void notify(Serializable obj) {
        //try {
        //System.out.println("App "+ getProcessId() + " reçu : "+obj.toString());
        System.out.println(System.currentTimeMillis() + " " + this.appQueue.size());
        try {
            //if (false)
            Thread.sleep(50);
        } catch (InterruptedException ex) {
            Logger.getLogger(TestSvcDataTransfer.class.getName()).log(Level.SEVERE, null, ex);
        }
        Runtime.getRuntime().gc();

    }

   

    public String plug(MapReduceConsumer obj, String[] args) {
        obj.setArgs(args);
        System.out.println(obj.numberOfBlocks());
        
        

        
        MultiMap<String,Integer> serRes = (MultiMap<String,Integer>)obj.produceBlock(1, null);
             for (int i = 0; i < serRes.getKeys().size(); ++i) {

                String key = (String) serRes.getKey(i);
                 System.out.print(key+" - ");
                Collection<Integer> values = serRes.getValues(key);
                Iterator it = values.iterator();
                while (it.hasNext()) {
                    System.out.print(it.next());
                }
                 System.out.println("");
            }
////
//
//        } catch (InstantiationException ex) {
//            Logger.getLogger(Community.class.getName()).log(Level.SEVERE, null, ex);
//        } catch (IllegalAccessException ex) {
//            Logger.getLogger(Community.class.getName()).log(Level.SEVERE, null, ex);
//        } catch (IllegalArgumentException ex) {
//            Logger.getLogger(Community.class.getName()).log(Level.SEVERE, null, ex);
////        } catch (InvocationTargetException ex) {
//            Logger.getLogger(Community.class.getName()).log(Level.SEVERE, null, ex);
//        } catch (NoSuchMethodException ex) {
//            Logger.getLogger(Community.class.getName()).log(Level.SEVERE, null, ex);
//        } catch (SecurityException ex) {
//            Logger.getLogger(Community.class.getName()).log(Level.SEVERE, null, ex);
//        } catch (ClassNotFoundException ex) {
//            Logger.getLogger(Community.class.getName()).log(Level.SEVERE, null, ex);
//        }
        return "toto";
    }
}

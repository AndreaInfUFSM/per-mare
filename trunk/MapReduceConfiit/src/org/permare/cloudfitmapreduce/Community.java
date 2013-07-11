/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
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

/**
 *
 * @author Luiz Angelo STEFFENEL <Luiz-Angelo.Steffenel@univ-reims.fr>
 */
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

    public int getNodes() {
        return 5;
    }

    public String plug(MapReduceConsumer obj, String[] args) {
        obj.setArgs(args);
        System.out.println(obj.numberOfBlocks());
        
        
/* Commenté car la reflexion ne marche pas avec les génériques */       
//        System.out.println("plugging class " + classname);
//        Class targetClass;
//        Loader cl = new Loader(classname);
//        try {
//            targetClass = cl.loadClass(classname);
////            Method[] liste = targetClass.getMethods();
////            for (int i=0;i<liste.length; i++)
////            {
////                System.out.println(liste[i].getName());
////                Class[] params = liste[i].getParameterTypes();
////                for (int j=0;j<params.length; ++j)
////                {
////                    System.out.println(" - "+params[j].getName());
////                }
////            }
//
//            // This method prepares the target class with the setArgs() method
//            Method prepare = targetClass.getMethod("setArgs", new Class[]{String[].class});
//
//            Object cibleInstance = targetClass.newInstance();
//            Class<? extends MapReduceConsumer> sub = targetClass.asSubclass(MapReduceConsumer.class);
//            MapReduceConsumer cons = (MapReduceConsumer) sub.newInstance();
//            System.out.println(cons.numberOfBlocks());
//            
////            prepare.invoke(cibleInstance, (Object) args);
////
////            // This method gets the number of tasks in this Job
////            Method nbTasks = targetClass.getMethod("numberOfBlocks", (java.lang.Class[]) null);
////
////            Object[] params = null;
////            Object nb = nbTasks.invoke(cibleInstance, params);
////
////            int number = ((Integer) nb).intValue();
////            System.out.println(number);
////
////
////            // This method gets the number of tasks in this Job
////            Method produceBlock = targetClass.getMethod("produceBlock", new Class[]{int.class, Serializable[].class});
////            Object[] produceParams = new Object[]{1, null};
////
////
////            Object res = produceBlock.invoke(cibleInstance, produceParams);
////            System.out.println("res is a "+res.getClass().getName() + 
////                    " instanceof : " + (res instanceof MultiMap));
////            
////            //Serializable ser = (Serializable)produceBlock.invoke(cibleInstance, produceParams);
////            //MultiMap<String, Integer> serRes = (MultiMap<String, Integer>)produceBlock.invoke(cibleInstance, produceParams);;
//////            try {
//////                serRes = (MultiMap<String, Integer>) res;
//////            } catch (ClassCastException ex) {
//////                ex.printStackTrace();
////////            }
        
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

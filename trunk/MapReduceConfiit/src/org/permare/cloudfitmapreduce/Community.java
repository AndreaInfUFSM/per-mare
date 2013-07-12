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
import cloudfit.core.ApplicationInterface;
import cloudfit.core.Message;
import cloudfit.core.ORBInterface;
import cloudfit.core.ServiceInterface;
import cloudfit.test.TestSvcDataTransfer;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.permare.confiitmapreduce.MapReduceConsumer;
import org.permare.util.MultiMap;

public class Community implements ServiceInterface {

    private int jobId = 0;
    private long processId = 1;
    private ActiveBlockingQueue appQueue = null;
    private ORBInterface router = null;
    private ArrayList<TaskStatus> taskList = null;
    private ExecutorService executor;

    public Community(long pid, ORBInterface na) {
        this.processId = pid;
        this.appQueue = new ActiveBlockingQueue(this.processId, this);
        this.router = na;
        // in production, replace 1 by System.getRuntime().availableProcessors() or a command-line parameter -nbthread
        executor = Executors.newFixedThreadPool(1);
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

        if (obj.getClass() == JobMessage.class) {
            if (((JobMessage) obj).getJobId() > jobId) {
                System.out.println("new job "+((JobMessage) obj).getJobId());
                jobId = ((JobMessage) obj).getJobId();
                System.out.println("a new job has arrived!");
                MapReduceConsumer jobClass = ((JobMessage) obj).getJobClass();
                //    initialize obj
                jobClass.setArgs(((JobMessage) obj).getArgs());
                //    start tasklist
                startTaskList(((JobMessage) obj).getJobId(), jobClass.numberOfBlocks());
                //    start solvers (threads?)

                for (int i = 0; i < taskList.size(); i++) {
                    Runnable worker = new WorkerThread(this, jobClass, taskList.get(i));
                    executor.execute(worker);
                }
                executor.shutdown();

                // TaskStatusMessage tm = new TaskStatusMessage(((JobMessage) obj).getJobId(), taskList.get(0).getTaskId(), taskList.get(0).getTaskResult());
                // router.sendAll(new Message(new Long(1), tm));
            }
        }
        if (obj.getClass() == TaskStatusMessage.class) {
            System.out.println("a taskResult has arrived!");
                        
            for (int i = 0; i < taskList.size(); ++i) {
                if (taskList.get(i).getTaskId() == ((TaskStatusMessage) obj).getTaskId()) {
                    if (taskList.get(i).getStatus() == TaskStatus.NEW || taskList.get(i).getStatus() == TaskStatus.STARTED) {
                        taskList.get(i).setStatus(TaskStatus.COMPLETED);
                        taskList.get(i).setTaskResult(((TaskStatusMessage) obj).getTaskValue());
                        System.out.println(taskList.get(i).getTaskId() + "[" + taskList.get(i).getStatus() + "]");
                    }
                }
                
            }
//            if (taskList.(((TaskStatusMessage)obj).getTaskId()))
        }
        // if obj = TaskStatusMessage
        //    update tasklist

        //try {
        //System.out.println("App "+ getProcessId() + " reçu : "+obj.toString());
        //System.out.println(System.currentTimeMillis() + " " + this.appQueue.size());
        try {
            //if (false)
            Thread.sleep(50);
        } catch (InterruptedException ex) {
            Logger.getLogger(TestSvcDataTransfer.class.getName()).log(Level.SEVERE, null, ex);
        }
        Runtime.getRuntime().gc();

    }

    public String plug(MapReduceConsumer obj, String[] args) {

        jobId++;
        JobMessage jm = new JobMessage(jobId, obj, args);
        router.sendAll(new Message(new Long(processId), jm));


        //    initialize obj
        obj.setArgs(args);
        //    start tasklist
        startTaskList(jobId, obj.numberOfBlocks());
        //    start solvers (threads?)

        for (int i = 0; i < taskList.size(); i++) {
            Runnable worker = new WorkerThread(this, obj, taskList.get(i));
            executor.execute(worker);
        }
        //executor.shutdown();
//        try {
//            executor.awaitTermination(50L, TimeUnit.SECONDS);
//        } catch (InterruptedException ex) {
//            Logger.getLogger(Community.class.getName()).log(Level.SEVERE, null, ex);
//        }
        return "Toto";
    }

    public Serializable waitJob() {
        boolean finished = false;

        while (finished == false) {
            finished = true;
            for (int i = 0; i < taskList.size(); i++) {
                if (taskList.get(i).getStatus() != TaskStatus.COMPLETED) {
                    finished = false;
                }
            }
        }

        executor.shutdown();
        try {
            executor.awaitTermination(50L, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            Logger.getLogger(Community.class.getName()).log(Level.SEVERE, null, ex);
        }
        System.out.println("fini !!!");
        for (int i = 0; i < taskList.size(); i++) {
            System.out.print(taskList.get(i).getTaskId() + "#" + taskList.get(i).getStatus() + "# - ");
        }

        return "Toto";
    }

    public void startTaskList(int jobId, int nbTasks) {
        taskList = new ArrayList<TaskStatus>();
        for (int i = 0; i < nbTasks; i++) {
            taskList.add(new TaskStatus(jobId, i));
        }
        Collections.shuffle(taskList);
        Collections.shuffle(taskList);
        Collections.shuffle(taskList);
        
        System.out.println("Tasklist ready " + nbTasks);
        for (int i = 0; i < nbTasks; ++i) {
            System.out.print(taskList.get(i).getTaskId() + "[" + taskList.get(i).getStatus() + "] - ");
        }
    }

    @Override
    public void send(Serializable msg) {
        router.sendNext(new Message(new Long(processId), msg));
    }

    @Override
    public void sendAll(Serializable msg) {
        router.sendAll(new Message(new Long(processId), msg));
    }
}

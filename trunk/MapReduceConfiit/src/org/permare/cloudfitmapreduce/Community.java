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
import cloudfit.core.Distributed;
import cloudfit.core.Message;
import cloudfit.core.ORBInterface;
import cloudfit.core.ServiceInterface;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Community implements ServiceInterface {

    private int jobId = 0;
    private long processId = 1;
    private ActiveBlockingQueue appQueue = null;
    private ORBInterface router = null;
    private ArrayList<ThreadSolve> Jobs = null;

    public Community(long pid, ORBInterface na) {
        this.processId = pid;
        this.appQueue = new ActiveBlockingQueue(this.processId, this);
        this.router = na;
        this.Jobs = new ArrayList<ThreadSolve>();
        // in production, replace 1 by System.getRuntime().availableProcessors() or a command-line parameter -nbthread
    }

    public void put(Serializable obj) {
        try {
            this.appQueue.put(obj);
        } catch (InterruptedException ex) {
            Logger.getLogger(Community.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public long getProcessId() {
        return processId;
    }

    @Override
    public void notify(Serializable obj) {

        if (obj.getClass() == JobMessage.class) {
            boolean existing  = false;
            for (int i = 0; i < Jobs.size(); i++) {
                if (Jobs.get(i).getJobId() == ((JobMessage) obj).getJobId()) {
                    existing = true;
                }
            }
            if (existing == false) {
                System.out.println("new job " + ((JobMessage) obj).getJobId());
                jobId = ((JobMessage) obj).getJobId();
                System.out.println("a new job has arrived!");

                Distributed jobClass = ((JobMessage) obj).getJobClass();

                ThreadSolve TS = new ThreadSolve(this, jobId, jobClass, ((JobMessage) obj).getArgs());
                Jobs.add(TS);
                TS.start();
//                //    initialize obj
//                jobClass.setArgs(((JobMessage) obj).getArgs());
//                //    start tasklist
//                startTaskList(((JobMessage) obj).getJobId(), jobClass.numberOfBlocks());
//                //    start solvers (threads?)


                //executor.shutdown();

                // TaskStatusMessage tm = new TaskStatusMessage(((JobMessage) obj).getJobId(), taskList.get(0).getTaskId(), taskList.get(0).getTaskResult());
                // router.sendAll(new Message(new Long(1), tm));
            }
        }
        if (obj.getClass() == TaskStatusMessage.class) {
            System.out.println("a taskResult has arrived!");
            for (int i = 0; i < Jobs.size(); i++) {
                if (Jobs.get(i).getJobId() == ((TaskStatusMessage) obj).getJobId()) {
                    Jobs.get(i).statusMessage(obj);
                }
            }

//            for (int i = 0; i < taskList.size(); ++i) {
//                if (taskList.get(i).getTaskId() == ((TaskStatusMessage) obj).getTaskId()) {
//                    if (taskList.get(i).getStatus() == TaskStatus.NEW || taskList.get(i).getStatus() == TaskStatus.STARTED) {
//                        taskList.get(i).setStatus(TaskStatus.COMPLETED);
//                        taskList.get(i).setTaskResult(((TaskStatusMessage) obj).getTaskValue());
//                        System.out.println("New result from others: "+ taskList.get(i).getTaskId() + "[" + taskList.get(i).getStatus() + "]");
//                    }
//                }
//                
//            }
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
            Logger.getLogger(Community.class.getName()).log(Level.SEVERE, null, ex);
        }
        Runtime.getRuntime().gc();

    }

    public int plug(Distributed obj, String[] args) {

        jobId++;
        JobMessage jm = new JobMessage(jobId, obj, args);
        router.sendAll(new Message(new Long(processId), jm));

// commented because not used anymore -> all "wait" logic lies on ThreadSolve
//        //    initialize obj
//        obj.setArgs(args);
//        //    start tasklist
//        startTaskList(jobId, obj.numberOfBlocks());
//        //    start solvers (threads?)
//
//        for (int i = 0; i < taskList.size(); i++) {
//            Runnable worker = new WorkerThread(this, obj, taskList.get(i));
//            executor.execute(worker);
//        }

        return jobId;
    }

    public Serializable waitJob(int waitingJobId) {

        for (int i = 0; i < Jobs.size(); i++) {
            if (Jobs.get(i).getJobId() == waitingJobId) {
                while (Jobs.get(i).isFinished() == false) {
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException ex) {
                        Logger.getLogger(Community.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
                return Jobs.get(i).getResult();
            }
        }
        return null;
    }

    

    @Override
        public void send(Serializable msg) {
        router.sendNext(new Message(new Long(processId), msg));
    }

    @Override
        public void sendAll(Serializable msg) {
        //System.out.println("Sending"+msg.getClass());
        router.sendAll(new Message(new Long(processId), msg));
    }
}

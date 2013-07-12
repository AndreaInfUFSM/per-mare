/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.permare.cloudfitmapreduce;

import cloudfit.core.ApplicationInterface;
import cloudfit.core.ServiceInterface;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.permare.util.MultiMap;

/**
 *
 * @author Luiz Angelo STEFFENEL <Luiz-Angelo.Steffenel@univ-reims.fr>
 */
public class ThreadSolve extends Thread {

    private int jobId;
    private boolean Finished = false;
    private Serializable Accumulator = null;

    public boolean isFinished() {
        return Finished;
    }

    public int getJobId() {
        return jobId;
    }
    private ArrayList<TaskStatus> taskList = null;
    private ExecutorService executor;
    private ApplicationInterface jobClass;
    private ServiceInterface service;

    public ThreadSolve(ServiceInterface service, int jobId, ApplicationInterface jobClass, String[] args) {
        this.service = service;
        this.jobId = jobId;
        this.executor = Executors.newFixedThreadPool(1);
        this.jobClass = jobClass;
        jobClass.setArgs(args);
        jobClass.initNumberOfBlocks();
        startTaskList(jobId, jobClass.getNumberOfBlocks());

    }

    @Override
    public void run() {
        for (int i = 0; i < taskList.size(); i++) {
            Runnable worker;
            try {
                // creates a new instance to avoid threads sharing the same object
                // in the future, replace by a clone or a sandbox execution
                ApplicationInterface jobInstance = jobClass.getClass().newInstance();
                jobInstance.setArgs(jobClass.getArgs());
                jobInstance.initNumberOfBlocks();
                
                worker = new WorkerThread(this, jobInstance, taskList.get(i));
                executor.execute(worker);
            } catch (InstantiationException ex) {
                Logger.getLogger(ThreadSolve.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IllegalAccessException ex) {
                Logger.getLogger(ThreadSolve.class.getName()).log(Level.SEVERE, null, ex);
            }    
        }
        executor.shutdown();
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ex) {
            Logger.getLogger(Community.class.getName()).log(Level.SEVERE, null, ex);
        }
        Finished = true;
        System.out.println("fini !!!");
        for (int i = 0; i < taskList.size(); i++) {
            System.out.print(taskList.get(i).getTaskId() + "#" + taskList.get(i).getStatus() + "# - ");
        }
        finalizeResult();
    }

    public void startTaskList(int jobId, int nbTasks) {
        taskList = new ArrayList<TaskStatus>();
        for (int i = 0; i < nbTasks; i++) {
            taskList.add(new TaskStatus(jobId, i));
        }
        // We do it 3 times to really "shake" the list and avoid similar list orders
        Collections.shuffle(taskList);
        Collections.shuffle(taskList);
        Collections.shuffle(taskList);

        System.out.println("Tasklist ready " + nbTasks);
        for (int i = 0; i < nbTasks; ++i) {
            System.out.print(taskList.get(i).getTaskId() + "[" + taskList.get(i).getStatus() + "] - ");
        }
    }

    public synchronized void sendAll(Serializable msg) {
        service.sendAll(msg);
    }

    void statusMessage(Serializable obj) {

        for (int i = 0; i < taskList.size(); ++i) {
            if (taskList.get(i).getTaskId() == ((TaskStatusMessage) obj).getTaskId()) {
                if (taskList.get(i).getStatus() == TaskStatus.NEW || taskList.get(i).getStatus() == TaskStatus.STARTED) {
                    taskList.get(i).setStatus(TaskStatus.COMPLETED);
                    taskList.get(i).setTaskResult(((TaskStatusMessage) obj).getTaskValue());
                    System.out.println("New result from others: " + taskList.get(i).getTaskId() + "[" + taskList.get(i).getStatus() + "]");
                }
            }

        }
    }

    Serializable getResult() {
        return Accumulator;
    }

    private synchronized void finalizeResult() {
        if (Accumulator == null)
            Accumulator = new MultiMap<String,Integer>();
        for (int i = 0; i < taskList.size(); ++i) {
            jobClass.consumeBlock(Accumulator,taskList.get(i).getTaskId(), taskList.get(i).getTaskResult());
        }
    }
}

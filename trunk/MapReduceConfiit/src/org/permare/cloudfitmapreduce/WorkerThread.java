/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.permare.cloudfitmapreduce;

import cloudfit.core.ApplicationInterface;
import cloudfit.core.ServiceInterface;
import java.io.Serializable;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Luiz Angelo STEFFENEL <Luiz-Angelo.Steffenel@univ-reims.fr>
 */
public class WorkerThread implements Runnable {

    private ApplicationInterface jobClass;
    private ServiceInterface app;
    private TaskStatus taskId;
    
    public WorkerThread (ServiceInterface app, ApplicationInterface obj, TaskStatus ts) {
        this.app = app;
        this.jobClass = obj;
        this.taskId = ts;    
    }
    
    
    @Override
    public void run() {
        if (taskId.getStatus()!=TaskStatus.COMPLETED)
        {
            taskId.setStatus(TaskStatus.STARTED);
            solve();
            try {
                Thread.sleep(3);
            } catch (InterruptedException ex) {
                Logger.getLogger(WorkerThread.class.getName()).log(Level.SEVERE, null, ex);
            }
            taskId.setStatus(TaskStatus.COMPLETED);
            TaskStatusMessage tm = new TaskStatusMessage(taskId.getJobId(), taskId.getTaskId(), taskId.getTaskResult());
            app.sendAll(tm);
        }
    }
    
    public void solve() {

        Serializable serRes = jobClass.produceBlock(taskId.getTaskId(), null);
        taskId.setTaskResult(serRes);
    }
}

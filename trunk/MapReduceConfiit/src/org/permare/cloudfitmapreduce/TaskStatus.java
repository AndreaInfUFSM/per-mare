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

import java.io.Serializable;


public class TaskStatus {
    
    public static int NEW = 0;
    public static int STARTED = 1;
    public static int COMPLETED = 2;
    
    private int jobId;
    private int taskId;
    private int status = 0;
    private Serializable taskResult = null;
    
    public int getJobId() {
        return jobId;
    }

    public void setJobId(int jobId) {
        this.jobId = jobId;
    }
    
    public TaskStatus(int jobId, int num)
    {
        setJobId(jobId);
        setTaskId(num);
    }
    

    public int getTaskId() {
        return taskId;
    }

    public void setTaskId(int taskId) {
        this.taskId = taskId;
    }

    public int getStatus() {
        return status;
    }

    /* general status of the task:
     * 0 - NEW
     * 1 - Locally started
     * 2 - locally finished
     * 3 - globally finished
     */
    public void setStatus(int status) {
        this.status = status;
    }

    public Serializable getTaskResult() {
        return taskResult;
    }

    public void setTaskResult(Serializable taskResult) {
        this.taskResult = taskResult;
    }
    
    
    
}

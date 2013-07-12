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


public class TaskStatusMessage implements Serializable{
    private int jobId = -1;
    private int taskId = -1;
    private Serializable TaskValue = null;
    
    public TaskStatusMessage(int jobId, int taskId, Serializable value)
    {
        this.jobId = jobId;
        this.taskId = taskId;
        this.TaskValue = value;
    }
    
    public int getJobId()
    {
        return jobId;
    }
    
        public int getTaskId()
    {
        return taskId;
    }
        
    public Serializable getTaskValue()
    {
        return TaskValue;
    }
}

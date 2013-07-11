/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.permare.cloudfitmapreduce;

import java.io.Serializable;

/**
 *
 * @author Luiz Angelo STEFFENEL <Luiz-Angelo.Steffenel@univ-reims.fr>
 */
public class CommunityMessage {
    private int jobId = -1;
    private int taskId = -1;
    private Serializable TaskValue = null;
    
    public CommunityMessage(int jobId, int taskId, Serializable value)
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

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

import cloudfit.core.ApplicationInterface;
import java.io.Serializable;


public class JobMessage implements Serializable {
    private int jobId = -1;
    private String[] args = null;
    private ApplicationInterface jobClass = null;
    
    public JobMessage(int jobId, ApplicationInterface obj, String[] args)
    {
        this.jobId = jobId;
        this.args = args;
        this.jobClass = obj;
    }
    
    public int getJobId()
    {
        return jobId;
    }
    
        public String[] getArgs()
    {
        return args;
    }
        
    public ApplicationInterface getJobClass()
    {
        return jobClass;
    }
}

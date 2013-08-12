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
package org.permare.context;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;

/**
 * Free memory collector using Sigar library
 * @author kirsch
 */
public class SigarFreeMemoryCollector implements Collector {

    @Override
    public List collect() {
        List<Double> list = new ArrayList<>();
        Sigar sigar = new Sigar();
        try {
            list.add(new Double(sigar.getMem().getFree()/1024));
            list.add(new Double(sigar.getMem().getFreePercent()));
        } catch (SigarException ex) {
            Logger.getLogger(SigarFreeMemoryCollector.class.getName()).log(Level.INFO,
                    "Free memory information unavailable", ex);
        }
        
        return list;
    }

    @Override
    public String getCollectorName() {
        return "Free Memory";
    }

    @Override
    public String getCollectorDescription() {
        return "Free physical memory and percentual of free memory using Sigar";
    }
    
}

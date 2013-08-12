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
 *
 * @author kirsch
 */
public class SigarCPUCollector implements Collector<Double> {

    @Override
    public List<Double> collect() {
        List<Double> list = new ArrayList<>();
        Sigar sigar = new Sigar();
        try {
            list.add(new Double(sigar.getCpuPerc().getCombined()));
        } catch (SigarException ex) {
            Logger.getLogger(SigarCPUCollector.class.getName()).log(Level.INFO, 
                    "CPU information unavailble", ex);
        }
        
        return list;
    }

    @Override
    public String getCollectorName() {
        return "Sigar CPU Load";
    }

    @Override
    public String getCollectorDescription() {
        return "Sigar CPU combined load";
    }
    
}

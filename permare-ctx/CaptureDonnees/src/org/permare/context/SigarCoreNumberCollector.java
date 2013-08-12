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
import org.hyperic.sigar.CpuInfo;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;

/**
 * collects the number of cores per processor
 * @author kirsch
 */
public class SigarCoreNumberCollector implements Collector {

    @Override
    public List<Double> collect() {
        List<Double> results = new ArrayList<>();
        Sigar sigar = new Sigar();
        try {
            results.add(new Double(sigar.getCpuInfoList()[0].getTotalCores()));
        } catch (SigarException ex) {
            Logger.getLogger(SigarCoreNumberCollector.class.getName()).log(Level.WARNING,
                    "Number of core unavailable", ex);
        }
        return results;
    }

    @Override
    public String getCollectorName() {
        return "Core number";
    }

    @Override
    public String getCollectorDescription() {
        return "Number of cores per processor";
    }
    
}

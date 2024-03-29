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

/**
 * collects the total number of cores observed using the OperatingSystemMXBean
 * and the one observed by the VM Runtime. 
 * @author kirsch
 */
public class TotalProcessorsCollector extends AbstractOSCollector<Double> {

    public static String COLLECTOR_NAME = "#Thing.Device.Processor.Available";
    public static String COLLECTOR_DESCR = "Total nb of available processors (or cores)";
    
    public TotalProcessorsCollector() {
        super.setName(COLLECTOR_NAME);
        super.setDescription(COLLECTOR_DESCR);
    }

    @Override
    public List<Double> collect() {
        List<Double> results = new ArrayList<>(2);
        results.add(new Double(this.getBean().getAvailableProcessors()));
        results.add(new Double(Runtime.getRuntime().availableProcessors()));
        return results;
    }
    
}

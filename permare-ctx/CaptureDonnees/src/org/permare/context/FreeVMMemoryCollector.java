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
 * collects information about the free memory on the Java VM.
 * @author kirsch
 */
public class FreeVMMemoryCollector extends AbstractCollector<Double> {
    public static String COLLECTOR_NAME = "#Thing.VM.Memory.Available";
    public static String COLLECTOR_DESCR = "VM free memory (in Kb)";
    
    public FreeVMMemoryCollector() {
        super.setName(COLLECTOR_NAME); 
        super.setDescription(COLLECTOR_DESCR);
    }

    @Override
    public List<Double> collect() {
        List<Double> results = new ArrayList<>(1);
        results.add(new Double(Runtime.getRuntime().freeMemory()/1024));
        return results;
    }
    
}

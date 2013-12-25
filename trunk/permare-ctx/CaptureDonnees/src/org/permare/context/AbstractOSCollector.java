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

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;

/**
 * abstract class for implementing collectors based on the MXBean framework.
 * Each collector based on the AbstractOSCollector has a OperatingSystemMXBean
 * bean enabling collecting OS information. 
 * @author kirsch
 * @param <T> collects date represented by the class T
 */
public abstract class AbstractOSCollector<T> extends AbstractCollector<T> {

    private OperatingSystemMXBean bean = ManagementFactory.getOperatingSystemMXBean();

    public OperatingSystemMXBean getBean() {
        return this.bean;
    }

    public void setBean(OperatingSystemMXBean bean) {
        this.bean = bean;
    }
}
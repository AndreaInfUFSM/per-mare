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

import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.List;

/**
 * application testing java techniques for collecting system data
 * @author kirsch
 */
public class AcquiringMain {

    private List<Collector<Double>> collectors = new ArrayList<>();
    private ThreadMXBean bean = ManagementFactory.getThreadMXBean( );
    private long startcputime, startusertime, starttime;
    private long totalcputime, totalusertime, totaltime;
        
    
    protected void setStartTime() {
        if (bean.isCurrentThreadCpuTimeSupported()) {
            startcputime = bean.getCurrentThreadCpuTime();
            startusertime = bean.getCurrentThreadUserTime();
        }
        else {
            startcputime = startusertime = -1; //not available
        }
        starttime = System.currentTimeMillis( );
    }
    
    protected void setEndTime() {
        if (startcputime >= 0) {
            totalcputime = bean.getCurrentThreadCpuTime() - startcputime;
            totalusertime = bean.getCurrentThreadUserTime() - startusertime;
        }
        else {
            totalusertime = totalcputime = -1; 
        }
        totaltime = System.currentTimeMillis() - starttime;
    }
    
    public void showTime(PrintStream out) {
        out.println("Total times (ms) :" + totaltime);
        out.println("CPU time (ms) : " + totalcputime/1000);
        out.println("User time (ms) : " + totalusertime/1000);        
    }
    
   
    
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception {
        
        AcquiringMain main = new AcquiringMain();

        main.setCollectors();
        
        main.setStartTime();
        
        main.collectAndShow();
        
        //do something
        System.out.println("couting until 15000 and sleeping 5s");
        main.losttime(15000);
        main.gosleep(5);
        
        main.collectAndShow();
        
        main.setEndTime();
        
        main.showTime(System.out);
        
    }
    
    /** busy wait counting until times */
    public void losttime(long times) {
        for (int i=0; i<times; i++);
    }
     
    /** go sleep for some seconds */
    public void gosleep(long seconds) throws InterruptedException {
        Thread.sleep(seconds*1000);
    }


    public void addCollector(Collector c) {
        this.collectors.add(c);
    }
    
    public void removeCollector(Collector c) {
        this.collectors.remove(c);
    } 
    
    public List<Collector<Double>> getCollectors() {
        return this.collectors;
    }
    
    public void collectAndShow() {
        for (Collector c: this.collectors) {
            System.out.println(c.getCollectorName() + " : " + c.getCollectorDescription());
            for (Object o: c.collect()) {
                System.out.printf(" %.4f ", o);
            }
            System.out.println("");
        }
    }

    
    public void setCollectors() {
        //test ones
//        this.addCollector(new CPULoadCollector());
//        this.addCollector(new SigarCPUCollector());
//        this.addCollector(new CPUAverageLoadCollector());
//        this.addCollector(new FreeMemoryCollector());
//        this.addCollector(new SigarFreeMemoryCollector());
//        this.addCollector(new MemoryCollector());
//        this.addCollector(new SigarCoreNumberCollector());
//        this.addCollector(new AvailableDiskSpaceCollector());
        
        //good ones
        this.addCollector(new CPUSytemLoadAverage());
        this.addCollector(new CPUSystemLoad());
        this.addCollector(new PhysicalMemoryCollector());
        this.addCollector(new FreePhysicalMemoryCollector());
        this.addCollector(new FreeVMMemoryCollector());
        this.addCollector(new TotalProcessorsCollector());
        this.addCollector(new UnallocatedStorageCollector());
    }
}


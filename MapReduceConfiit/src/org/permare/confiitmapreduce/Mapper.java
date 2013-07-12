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
package org.permare.confiitmapreduce;

//import confiit.*;
//import confiit.util.Display;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.permare.util.FileHandler;
import org.permare.util.MultiMap;
import org.permare.wordcounter.CounterExample;

/**
 * Mapper class using Confiit
 * 
 * @author kirsch
 */
public class Mapper extends MapReduceConsumer {

    private final boolean debug = false;
    private List<File> filenames = new ArrayList<File>();
    
    
    public List getFilenames() {
        return this.filenames;
    }

    public File getFilename(int pos) {
        return this.filenames.get(pos);
    }

    public void addFilename(File file) {
        this.filenames.add(file);
    }

    @Override
    public Serializable initializeConsumer() {
        Serializable acc = super.initializeConsumer();

        // définit la liste de fichiers à lire
        this.setBlockParameters();

        return acc;
    }

    
    

    /**
     * Evaluates the task <i>number</i> and returns <i>number</i> + 1.
     *
     * @param number task number (task id)
     * @param required data to be analysed
     * @return next task id
     */
    @Override
    public Serializable produceBlock(int number, Serializable[] required) {
        String line = null;
        FileHandler fhandler = null;
        
        CounterExample counter = new CounterExample();
        MultiMap<String, Integer> map = new MultiMap<String, Integer>();
        File file = this.getFilename(number);

        if (debug) {
            System.out.println(file + " " + file.length());
        }

        fhandler = new FileHandler(file);

        if (!fhandler.open(FileHandler.READ)) {
            if (debug) {
                System.out.println("ERROR : impossible to open file");
            }
            return map;
        }

        while ((line = fhandler.readLine()) != null) {
            MultiMap<String, Integer> linemap = counter.map(Integer.toString(number), line);
            map.putAll(linemap);
        }

        fhandler.close();

        return map;

    }

    /**
     * looks for input files on the arguments. If argument is a directory, it
     * includes all files inside, recursively.
     */
    private void setBlockParameters() {
        if (filenames.isEmpty()) {
            File target = new File(getArgs()[0]);
            if (!this.addDirectoryFiles(target)) {
                filenames.add(target);
     //           Display.info("add file " + target.toString());
            }
        }
    }

    private boolean addDirectoryFiles(File target) {

        if (!target.isDirectory()) {
            return false;
        }

        File[] listOfFiles = target.listFiles();

        if (listOfFiles != null) {
            for (File file : listOfFiles) {
                if (file.isDirectory()) {
                    this.addDirectoryFiles(file);
                } else if (file.isFile()) {
                    this.addFilename(file);
    //                Display.info("add " + file.toString());
                }

            }
        }

        return true;
    }

    @Override
    public int getNumberOfBlocks() {
        return nbBlocks;
    }

    @Override
    public void setNumberOfBlocks(int nbBlocks) {
        this.nbBlocks=nbBlocks;
    }
    
    /**
     * Evaluates the number of blocks in a resource segment. This number must be
     * always greater than 0.
     *
     * @return int number of blocks in the segment
     */
    @Override
    public void initNumberOfBlocks() {
        int nb = 0; // Nombre de taches a renvoyer
        long length = 0;

        //sur le lanceur on n'execute pas initializeConsumer, donc on l'appelle ici
        setBlockParameters();
        // on aura autant de tâches MAP que de fichiers
        nb = filenames.size();
        nbBlocks = nb;
        //System.out.println("on a "+ nb + " blocs à traiter");
        //return nb;
    }
}
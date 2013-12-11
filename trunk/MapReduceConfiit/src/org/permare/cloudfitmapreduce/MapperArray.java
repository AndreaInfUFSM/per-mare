/* *************************************************************** *
 * PER-MARE Project (project number 13STIC07)
 * http://cosy.univ-reims.fr/~lsteffenel/per-mare
 * A CAPES/MAEE/ANII STIC-AmSud collaboration program.
 * All rights reserved to project partners:
 *  - Universite de Reims Champagne-Ardenne, Reims, France 
 *  - Universite Paris 1 Pantheon Sorbonne, Paris, France
 *  - Universidade Federal de Santa Maria, Santa Maria, Brazil
 *  - Universidad de la Republica, Montevideo, Uruguay
 * 
 * *************************************************************** *
 */
package org.permare.cloudfitmapreduce;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.permare.confiitmapreduce.OLDMapReduceConsumer;
import org.permare.util.FileHandler;
import org.permare.util.MultiMap;
import org.permare.wordcounter.CounterExample;

/**
 * Mapper class using Confiit that does not uses internally Lists. No thread
 * safe structures seems to have some problems during serialization process when
 * using Pastry-like libraries.
 *
 * @author kirsch
 */
public class MapperArray extends OLDMapReduceConsumer {

    private static final int ARGS_FILENAME_POSITION = 0;
    private File[] filenames;
    

    /**
     * returns the list of filenames that should be handled.
     *
     * @ return List containg filenames or null if no filename have been given
     * yet
     */
    public List<File> getFilenames() {
        List<File> copy = null;
        if (this.filenames != null) {
            copy = Arrays.asList(this.filenames);
        }

        return copy;
    }

    /**
     * defines the filenames that should be handled by the Mapper.
     *
     * @param target input File to be handled or a directory whose files will be
     * handled.
     */
    public void setFilenames(File target) {
        //target may be null 
        if (target != null) {
            if (!target.isDirectory()) {
                //target is a simple file
                this.filenames = new File[1];
                this.filenames[0] = target;
            } else {
                //target is a directory, need handled it in deep
                ArrayList<File> tmpFiles = new ArrayList<File>();
                this.addDirectory(tmpFiles, target);
                //copying files addDirectory found to the filenames
                this.filenames = tmpFiles.toArray(new File[tmpFiles.size()]);
            }
        }
    }
    
    /**
     * retrieves the filename in the position pos.
     * @param pos position (from 0) of the filename in the filenames list
     * @return File in the position pos or null if no file is available in this position
     */
    public File getFilename(int pos) {
        File target = null;
        
        if (this.hasFilenames()) {
            if (pos>=0 && pos<this.filenames.length) {
                target = this.filenames[pos];
            }
        }
            
        return target;
    }

    /**
     * Evaluates the number of blocks in a resource segment. This number must be
     * always greater than 0.
     *
     * It calculates the number of blocks in the segment based on the number of
     * files found in the first parameter. 
     * If no argument is given (no file to handle), number of blocks will be zero.
     */
    @Override
    public void initNumberOfBlocks() {
        //tries to find out the files we need to handle
        setBlockParameters();
        // we will have as many task as files
        if (this.hasFilenames())
            this.setNumberOfBlocks(this.filenames.length);
        else
            this.setNumberOfBlocks(0);
    }
    

    @Override
    public Serializable produceBlock(int number, Serializable[] required) {
        String line = null;
        FileHandler fhandler = null;
        
        CounterExample counter = new CounterExample();
        MultiMap<String, Integer> map = new MultiMap<String, Integer>();
        File file = this.getFilename(number);

        Logger.getLogger(MapperArray.class.getName()).log(Level.INFO, new String (file + " " + file.length()));


        fhandler = new FileHandler(file);

        if (!fhandler.open(FileHandler.READ)) {
            Logger.getLogger(MapperArray.class.getName()).log(Level.SEVERE, "ERROR : impossible to open file");
            
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
     * initialize the consumer, defining the number of blocks.
     */
    @Override
    public Serializable initializeConsumer() {
        Serializable acc = super.initializeConsumer();
        //set the numeber of blocks
        this.setBlockParameters();
        return acc;
    }


    /**
     * looks for input files on the <i>first</i> argument. If argument is a directory, it
     * includes all files inside, recursively.
     * @see ARGS_FILENAME_POSITION
     */
    private void setBlockParameters() {
        if (!this.hasFilenames()) {
            File target = new File(getArgs()[ARGS_FILENAME_POSITION]);
            this.setFilenames(target);
        }
    }
    
    /**
     * add the files in the target to the files list. It is a recursive method. 
     * If one of these files is a directory, it will include all files on it.
     * 
     * @param files file list that will cumulate all filenames 
     * @param target starting target file (it may be a directory).
     * @return true if target is a directory, false otherwise. 
     */
    private boolean addDirectory(ArrayList files, File target) {
        //if it's not a directory, it's not for me...
        if (!target.isDirectory()) {
            return false;
        }

        //it is a directory, looking for files on it
        File[] listOfFiles = target.listFiles();
        if (listOfFiles != null) {
            for (File tgt : listOfFiles) {
                //if it is a directory, go for a next run
                if (tgt.isDirectory()) {
                    this.addDirectory(files, tgt);
                } else if (tgt.isFile()) {
                    //it is not a directory, just add the file to the list    
                    files.add(tgt);
                }
            }
        }

        return true;
    }
    
    /*
     * test if some filename has been defined.
     */
    private boolean hasFilenames() {
        return (this.filenames != null && this.filenames.length > 0);
    }

}

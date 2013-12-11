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
package org.permare.util;

import java.io.File;

/**
 * FileHandlerIllustration is an application illustrating the use of the
 * FileHandler class.
 * 
 * @see FileHandler
 * @author kirsch
 */
public class FileHandlerIllustration {
    FileHandler handler;
    File file;
    
    public FileHandlerIllustration(String filename) {
        this.file = new File(filename);
        
        this.handler = new FileHandler(file);
    }
    
    
    public void writing() {
        
        if (handler.open(FileHandler.WRITE)) {
            for (int i=0; i<=100; i++) {
                String line = "This file contains " + i + " lines.\n";
                System.out.print("Writing : "+line);
                handler.writeLine(line);
            }
            if (handler.close())
                System.out.println("Closing file...");
        }
        else {
            System.out.println("Impossible to open file "+this.file.getName()+
                    " for writing.");
        }
    }
    
    public void reading() {
        
        if (handler.open(FileHandler.READ)) {
            String line;
            while ((line = handler.readLine()) != null) {
                System.out.println("Reading :" + line);
            }
            if (handler.close())
                System.out.println("Closing file...");
        }
        else {
            System.out.println("Impossible to open file "+this.file.getName()+
                    " for reading.");
        }
    }
    
    public void isOpenTesting() {
        System.out.println("File is open for writing ? " + handler.isOpenForWriting());
        System.out.println("Opening file for writing : " + handler.open(FileHandler.WRITE));
        System.out.println("File is open for writing ? " + handler.isOpenForWriting());
        System.out.println("Closing file : " + handler.close());
        
        System.out.println("File is open for reading ? " + handler.isOpenForReading());
        System.out.println("Opening file for reading : " + handler.open(FileHandler.READ));
        System.out.println("File is open for reading ? " + handler.isOpenForReading());
        System.out.println("Closing file : " + handler.close());
        
    }
    
    
    public static void main (String args[]) {
        String filename = "FileHandlerIllustration.txt";
        
        if (args.length > 0) {
            filename = args[0];
        }
        
        FileHandlerIllustration test = new FileHandlerIllustration(filename);
        
        System.out.println("======== isOpen tests ========");
        test.isOpenTesting();
        
        System.out.println("======== Writing test ========");
        test.writing();
        System.out.println("======== Reading test ========");
        test.reading();
        
    }
    
    
}

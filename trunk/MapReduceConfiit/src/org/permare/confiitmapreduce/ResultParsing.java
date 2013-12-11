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
package org.permare.confiitmapreduce;

import confiit.util.InStream;
import java.io.InputStream;
import java.io.Serializable;

/**
 * Class allowing to parse and retreive results from previous tasks on Confiit.
 * Currently, such results are kept on a file (or transfered by the network) in
 * a XML file format containing serialized objects inside.
 *
 * @todo change this hugly communication pattern from Confiit
 * @author kirsch
 */
public class ResultParsing {

    private InputStream input;
    private InStream parse;

    /**
     * Results from previous task are retrieved from an InputStream (a file or a
     * network connection).
     *
     * @todo explain file format somewhere
     *
     * @param input InputStream containing results from preivous tasks
     */
    public ResultParsing(InputStream input) {
        this.input = input;
        this.parse = null; //open only when needed
    }

    public InputStream getInput() {
        return this.input;
    }

    public void setInput(InputStream input) throws TasksCommunicationException {
        //before setting a new input, cheks if parse is open and close it  
        if (this.parse != null) {
            this.close();
        }
        this.input = input;
        this.parse = null;
    }

    /**
     * retrieves and parses results coming from the input.
     * Parse is performed by Confiit (InStream class).
     * By default, we open and close the parser (InStream) before and after
     * retrieving a result object. 
     * 
     * @return Serializable object contained in the input
     * @throws TasksCommunicationException  if wasn't possible to read the results from the input
     */
    public Serializable getResults() throws TasksCommunicationException {
        Serializable result = null;
        
        if (this.parse == null){
            this.open();
        }

        
        if (!this.parse.waitMessage()) {
            this.close();
            throw new TasksCommunicationException("Exception on waitMessage: no message available",
                    TasksCommunicationException.BAD_FILE_FORMAT);
        }
        
        if (!this.parse.getType().equals("result") ) {
            this.close();
            throw new TasksCommunicationException("Exception on getType: no \'result\' message available",
                    TasksCommunicationException.BAD_MESSAGE_FORMAT);
        }

        try {
            result = this.parse.getStringObject();
        } catch (Exception ex) { //  @todo : peaufiner le type d'exception 
            throw new TasksCommunicationException("Exception on getStringObject: no object available",
                    TasksCommunicationException.BAD_MESSAGE_FORMAT);
        }
        
        this.close();
        
        return result;
    }

    /**
     * opens a new parse session using the input
     *
     * @throws TasksCommunicationException if input not available for parse
     */
    void open() throws TasksCommunicationException {

        //before opening a new parse, cheks if parse is open and close it  
        if (this.parse != null) {
            this.close();
        }

        try {
            this.parse = InStream.New(this.input);
        } catch (Exception ex) { //  @todo : peaufiner le type d'exception 
            throw new TasksCommunicationException(TasksCommunicationException.BAD_FILE_FORMAT);
        }

    }

    /**
     * closes a open parse
     *
     * @throws TasksCommunicationException if close wasn't possible
     */
    void close() throws TasksCommunicationException {
        //if parse is openned 
        if (this.parse != null) {
            try {
                this.parse.close();
            } catch (Exception ex) { //  @todo : peaufiner le type d'exception 
                throw new TasksCommunicationException(TasksCommunicationException.BAD_FILE_FORMAT);
            }
        }
    }
}
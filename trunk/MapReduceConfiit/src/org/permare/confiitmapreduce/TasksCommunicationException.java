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

/**
 * Exception caused by communication Confiit problems when parsing results from
 * previous taks.
 *
 * @author kirsch
 */
public class TasksCommunicationException extends RuntimeException {

    /**
     * indicates that exception rises because previous results are not
     * available. Default type. *
     */
    public static final int BAD_FILE_FORMAT = 0;
    /**
     * indicates that exception rises because previous results does not present
     * the correct message ("result")
     */
    public static final int BAD_MESSAGE_FORMAT = 1;
    
    
    private int type = BAD_FILE_FORMAT;

    public TasksCommunicationException(int type) {
        this.type = type;
    }

    public TasksCommunicationException(String message, int type) {
        super(message);
        this.type = type;
    }

    /**
     * default constructor, using default type (BAD_FILE_FORMAT).
     *
     * @see #BAD_FILE_FORMAT
     */
    public TasksCommunicationException() {
        super();
        this.type = BAD_FILE_FORMAT;
    }

    public TasksCommunicationException(String message) {
        this(message, BAD_FILE_FORMAT);
    }
}
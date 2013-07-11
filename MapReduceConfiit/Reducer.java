/* ****************************************************************************
 *
 * REDUCER
 *
 * ***************************************************************************/

import confiit.*;
import confiit.daemon.Component;
import confiit.daemon.Program;
import confiit.util.Context;
import confiit.util.InStream;
import confiit.util.Util;
import java.io.File;
import java.io.FileInputStream;
import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;


public class Reducer extends Distributed {

    
    private final boolean debug = false;
    

    /* ****************************************************************************
     * Methodes protegees
     */
    /* ------------------------------------------------------------------------- */
    /**
     * Consommation d'un bloc du segment de ressource.
     *
     * @param number Numero du bloc dans le segment (0 a <i>n</i> - 1).
     * @param value Contenu du bloc.
     * @return Segment modifie.
     */
    protected Serializable consumeBlock(int number, Serializable value) {
        //int res;                        // Resultat intermediaire
        if (debug) {
            System.out.println("## consumeBlock " + number + ", " + value);
        }

        if (number >= getNumberOfBlocks()) {
            return getAccumulator();
        }

        

        MultiMap<String, Integer> accumulator;
        try {
            accumulator = (MultiMap<String, Integer>) getAccumulator();
        } catch (ClassCastException ex) {
            accumulator = new MultiMap<String, Integer>();
        }
        
        Set<String> keys = ((MultiMap<String, Integer>) value).getKeys();
        Iterator ikeys = keys.iterator();
        while (ikeys.hasNext()) {
            String key = (String) ikeys.next();
            Iterator it = ((MultiMap<String, Integer>) value).keyIterator(key);
            while (it.hasNext()) {
                accumulator.add(key, (Integer) it.next());
            }
        }
        return accumulator;
    }

    
    /* ------------------------------------------------------------------------- */
    /**
     * Initialisation de la partie consommateur du composant.
     *
     * @return Valeur d'initialisation de l'accumulateur.
     */
    protected Serializable initializeConsumer() {
        if (debug) {
            System.out.println("## initializeConsumer");
        }
        return new Integer(0);
    }

    

    /* ------------------------------------------------------------------------- */
    /**
     * Evaluation du nombre de blocs dans le segment de ressource. Ce nombre
     * doit toujours etre superieur a 0.
     *
     * @return Nombre de blocs dans le segment de ressource.
     */
    protected int numberOfBlocks() {
        int nb = 1;
        try {
            nb = Integer.parseInt(getArgs()[1]);                         // Nombre de taches a renvoyer
            //nb = 2;
        } catch (Exception ex) {
            nb = 1;
        }


        return nb;
    }

    /* ------------------------------------------------------------------------- */
    /**
     * Calcul de la tache <i>number</i>. Renvoie simplement <i>number</i> + 1.
     *
     * @param number Indice de la tache dans le calcul (0 a <i>nbTask</i> - 1).
     * @param required Tableau des blocs requis.
     * @return <i>number</i>.
     */
    
    // ATTENTION : produceBlock fait le REDUCE sur chaque tâche. Le regroupement des résultats est fait dans consumeBlock
    
    protected Serializable produceBlock(int number, Serializable[] required) {
        MultiMap<String, Integer> map = new MultiMap<String, Integer>();
        Integer sum = null;
        String key = null;

        try {
            MultiMap<String, Integer> accumulator = (MultiMap<String, Integer>) getResult(getArgs()[0]);

            int step = (int) Math.ceil(accumulator.getKeys().size()/numberOfBlocks());
            for (int i = number*step; i < Math.min((number+1)*step,accumulator.getKeys().size()); ++i) {
            
                key = accumulator.getKey(i);
                Collection<Integer> values = accumulator.get(key);

                sum = new Integer(0);
                Iterator val = values.iterator();
                while (val.hasNext()) {
                    sum += (Integer) val.next();
                }


                map.add(key, sum);
            }
        } 
        catch (Exception ex) {
            Logger.getLogger(Reducer.class.getName()).log(Level.SEVERE, null, ex);
        }
        return map;


    }

    
/* méthodes privées d'usage interne
 * 
 */
    
    private Serializable[] getBlocks(String iid) throws Exception {
        try {
//      Verifier la presence du fichier des blocs
            File file = Context.getBlocksFile(iid);
            if (!file.exists()) {
                System.out.println("No result available for " + iid + ".");
            }
//      Ouverture du fichier des taches   
            InStream in = InStream.New(new FileInputStream(file));
            if (!in.waitMessage(1000)) {
                in.close();
                System.out.println("Bad application format.");
            }
            if (!in.getType().equals("application")) {
                in.close();
                System.out.println("Bad application format.");
            }
//      Recuperation des parametres d'execution
            String mainClass = Util.StringNew(in.getString());
            String[] args = Util.StringArrayNew(in.getInt());
            for (int i = 0; i < args.length; i++) {
                args[i] = Util.StringNew(in.getString());
            }
            int blocksNumber = in.getInt();
//      Instanciation de la classe d'execution
            Object obj = (Class.forName(mainClass)).newInstance();
            Program prog = Program.New((Component) obj);
            prog.setArgs(args);
            prog.setNumberOfBlocks(blocksNumber);
            prog.initializeConsumer();
            Serializable[] result = new Serializable[blocksNumber];
//      Lecture et affichage des taches connues
            while (in.waitMessage()) {
                if (!in.getType().equals("block")) {
                    in.close();
                    throw new Exception("Bad block format");
                }
                result[in.getInt()] = in.getStringObject();
            }
            in.close();
            System.out.println(iid + "nbblocks = " + blocksNumber);
            System.out.println(iid + "result = " + result[1]);
            return result;
        } catch (Error e) {
            throw new Exception("Error reading result file", e);
        }
    }

    private Serializable getResult(String iid) throws Exception {
        File file = Context.getResultFile(iid);
        try {
//       Recuperation du resultat   
            InStream in = InStream.New(new FileInputStream(file));
            if (!in.waitMessage()) {
                in.close();
                throw new Exception("Bad result format.");
            }
            if (!in.getType().equals("result")) {
                in.close();
                throw new Exception("Bad result format.");
            }
            Serializable result = in.getStringObject();
            in.close();
            return result;
        } catch (Error e) {
            throw new Exception("Error reading result file", e);
        }
    }
}
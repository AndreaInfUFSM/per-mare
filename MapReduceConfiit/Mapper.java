/* ****************************************************************************
 *
 * MAPPER
 *
 * ***************************************************************************/

import confiit.*;
import confiit.util.Display;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;


public class Mapper extends Distributed {

    
    private final boolean debug = true;
    private ArrayList<File> filenames = new ArrayList();
    private final static Integer one = new Integer(1);


    
    /**
     * Consommation d'un bloc du segment de ressource.
     *
     * @param number Numero du bloc dans le segment (0 a <i>n</i> - 1).
     * @param value Contenu du bloc.
     * @return Segment modifie.
     */
    
    // ATTENTION : consumeBlock fait un rôle similaire aux Combiners, il regroupe les résultats d'un block et les met dans 
    //             la variable partagée value
    protected Serializable consumeBlock(int number, Serializable value) {
        
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
     * Finalisation de la partie consommateur du composant.
     *
     * @return Accumulateur finalise.
     */
    protected Serializable finalizeConsumer() {
        if (debug) {

            System.out.println("## finalizeConsumer ");

        }
        return getAccumulator();
    }

    

    /* ------------------------------------------------------------------------- */
    /**
     * Initialisation de la partie consommateur du composant.
     * 
     * lancée sur les tasks
     *
     * @return Valeur d'initialisation de l'accumulateur.
     */
    protected Serializable initializeConsumer() {
        if (debug) {
            System.out.println("## initializeConsumer");
        }

        // définit la liste de fichiers à lire
        setBlockParameters();

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
        int nb = 0; // Nombre de taches a renvoyer
        long length = 0;
        
        //sur le lanceur on n'execute pas initializeConsumer, donc on l'appelle ici
        setBlockParameters();
        // on aura autant de tâches MAP que de fichiers
        nb = filenames.size();

        
        return nb;
    }

    /* ------------------------------------------------------------------------- */
    /**
     * Calcul de la tache <i>number</i>. Renvoie simplement <i>number</i> + 1.
     * 
     * ICI ON FAIT L'EQUIVALENT A MAP
     *
     * @param number Indice de la tache dans le calcul (0 a <i>nbTask</i> - 1).
     * @param required Tableau des blocs requis.
     * @return <i>number</i>.
     */
    protected Serializable produceBlock(int number, Serializable[] required) {
        FileInputStream fs;
        String line = null;

        MultiMap<String, Integer> map = new MultiMap<String, Integer>();
      
        try {
            File file = filenames.get(number);

            System.out.println(file + " " + file.length());
            fs = new FileInputStream(file);
            BufferedReader br = new BufferedReader(new InputStreamReader(fs));

            String[] tokens;
            String token;
            StringTokenizer tokenizer;
            while ((line = br.readLine()) != null) {
                tokenizer = new StringTokenizer(line);
                while (tokenizer.hasMoreTokens()) {
                    token = tokenizer.nextToken();
                    map.add(token, one);
                }

            }
            br.close();
            fs.close();
            
        } catch (FileNotFoundException ex) {
            Logger.getLogger(Mapper.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(Mapper.class.getName()).log(Level.SEVERE, null, ex);
        }
        return map;
    }



    /* --------------------------------------------------------------------------*/
    /* Methodes privees utilisees en interne
     * 
     */
    private void setBlockParameters() {
        if (filenames.isEmpty()) {
            File target = new File(getArgs()[0]);
            if (target.isDirectory()) {
                File[] listOfFiles = target.listFiles();

                if (listOfFiles != null) {
                    for (File file : listOfFiles) {
                        if (file.isFile()) {
                            filenames.add(file);
                            Display.info("add " + file.toString());
                        }
                    }
                }
            } else {
                filenames.add(target);
                Display.info("add file " + target.toString());
            }
        }
    }
//
//    private int count(String filename) throws IOException {
//        InputStream is = new BufferedInputStream(new FileInputStream(filename));
//        try {
//            byte[] c = new byte[1024];
//            int count = 0;
//            int readChars = 0;
//            boolean empty = true;
//            while ((readChars = is.read(c)) != -1) {
//                empty = false;
//                for (int i = 0; i < readChars; ++i) {
//                    if (c[i] == '\n') {
//                        ++count;
//                    }
//                }
//            }
//            return (count == 0 && !empty) ? 1 : count;
//        } finally {
//            is.close();
//        }
//    }
//
//    private int countf(File filename) throws IOException {
//        InputStream is = new BufferedInputStream(new FileInputStream(filename));
//        try {
//            byte[] c = new byte[1024];
//            int count = 0;
//            int readChars = 0;
//            boolean empty = true;
//            while ((readChars = is.read(c)) != -1) {
//                empty = false;
//                for (int i = 0; i < readChars; ++i) {
//                    if (c[i] == '\n') {
//                        ++count;
//                    }
//                }
//            }
//            return (count == 0 && !empty) ? 1 : count;
//        } finally {
//            is.close();
//        }
//    }
//
//    private int count2(String filename) throws IOException {
//        LineNumberReader lnr = new LineNumberReader(new FileReader(new File(filename)));
//        lnr.skip(Long.MAX_VALUE);
//
//        return lnr.getLineNumber();
//    }
//
//    private int count3(String filename) throws IOException {
//        LineNumberReader reader = new LineNumberReader(new FileReader(filename));
//        int cnt = 0;
//        String lineRead = "";
//        while ((lineRead = reader.readLine()) != null) {
//        }
//
//        cnt = reader.getLineNumber();
//        reader.close();
//        return cnt;
//    }
}
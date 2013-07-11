/* ****************************************************************************
 *
 * WORDCOUNT
 *
 * ***************************************************************************/

import confiit.*;
import java.io.File;
import java.io.PrintStream;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Set;



public class WordCounter {

    /* ****************************************************************************
     * Methodes publiques
     */
    /* ------------------------------------------------------------------------- */
    /**
     * Point d'entree de l'application.
     *
     * @param args Tableau des parametres d'execution.
     */
    public static void main(String[] args) {
        Serializable[] mblocks;           // Blocs calcules
        Serializable[] rblocks;           // Blocs calcules
        Community community;            // Communaute Confiit
        int i;                          // Variable de boucle
        String mapper;// Identifiant de l'instance Mapper
        String reducer;// Identifiant de l'instance Reducer
        Iterator ikeys = null;
        int count = 0;

        MultiMap<String, Integer> intRes = null;
        Set<String> keys = null;
        long start;
        long end;
        start = System.currentTimeMillis();
        try {
            community = new Community();
            // ici on indique la classe qui fera le MAP
            mapper = community.plug("Mapper", args);
            System.out.println(mapper);
            community.wait(mapper);
            
 /* Cette partie sert à imprimer à l'écran le résultat intérmédiaire du MAP
            
            mblocks = community.getBlocks(mapper);
            for (i = 0; i < mblocks.length; i++) {
                if (mblocks[i] != null) {
                    System.out.println("Block[" + i + "] = " + ((MultiMap<String, Integer>) mblocks[i]).size());
                }
            }
            intRes = (MultiMap<String, Integer>) community.getResult(mapper, 10000, true);
            System.out.println("Sum = " + intRes.size());
            System.out.println(community.getNodes().length);

            keys = intRes.getKeys();
            ikeys = keys.iterator();
            count = 0;
            while (ikeys.hasNext()) {
                String key = (String) ikeys.next();
                System.out.print("Key "+key + " = ");
                Iterator it = ((MultiMap<String, Integer>) intRes).keyIterator(key);
                while (it.hasNext()) {
                    System.out.print((Integer) it.next());
                    count++;
                }
                System.out.println("");
            }
            System.out.println("Count = "+count);
*/            
            String[] reduceargs = new String[2];
            reduceargs[0] = mapper;
            // reduceargs[1] indique combien de tasks REDUCE seront créées
            reduceargs[1] = Integer.toString((community.getNodes()).length);
            // ici on indique la classe qui fera le REDUCE
            reducer = community.plug("Reducer", reduceargs);
            community.wait(reducer);

/* Cette partie sert à imprimer à l'écran le résultat du REDUCE
 */            
            //rblocks = community.getBlocks(reducer);
            //for (i = 0; i < rblocks.length; i++) {
            //    if (rblocks[i] != null) {
            //       System.out.println("Block[" + i + "] = " + ((MultiMap<String, Integer>) rblocks[i]).size());
            //   }
            //}
            
 
            intRes = (MultiMap<String, Integer>) community.getResult(reducer, true);
 
            /* Cette partie sert à envoyer le résultat de REDUCE dans un ficher */
            keys = intRes.getKeys();
            ikeys = keys.iterator();
            count = 0;
            Integer group = new Integer(0);
            File outdir = new File(args[1]);
            if (!outdir.exists()) {
                outdir.mkdir();
            }
            File outfile = new File(args[1].concat("/part-00000"));
            PrintStream fs = new PrintStream(outfile);
            while (ikeys.hasNext()) {
                String key = (String) ikeys.next();
                Iterator it = ((MultiMap<String, Integer>) intRes).keyIterator(key);
                while (it.hasNext()) {
                    group = (Integer) it.next();
                    fs.printf("%s = %d\n", key, group);
                    count += group.intValue();
                }
            }
            fs.flush();


            System.out.println("Count = " + count);


        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
        end = System.currentTimeMillis();

        System.out.println("Total time = " + (end - start));
    }
}

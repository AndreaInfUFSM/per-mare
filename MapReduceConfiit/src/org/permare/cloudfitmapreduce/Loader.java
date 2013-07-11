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

package org.permare.cloudfitmapreduce;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;


public class Loader extends ClassLoader {

/* ****************************************************************************
 * Proprietes privees
 */
/**
 * Repertoire de base des classes utilisateur. */
private String classHome;
/**
 * Tableau des repertoires de recherche des classes. */
private static String[] classPath;
/**
 * Cha&icirc;ne separateur de nom de fichiers. */
private static String fSep;
/**
 * Caractere separateur de cheminn d'acces. */
private static char pSep;

/* ****************************************************************************
 * Constructeur de classe
 */
/* ------------------------------------------------------------------------- */
static {
    int i, j;							// Variables de boucle
    int nbPath = 0;						// Nombre de chemins definis
    String str;							// Chaine de travail
    String tmp;							// Chemin temporaire
    
//  Obtention du separateur de fichiers
    try {
        str = System.getProperty ("path.separator");
        pSep = str.charAt (0);
        fSep = System.getProperty ("file.separator");
        }
    catch (Exception e) {
        pSep = ':';
        fSep = new String ("/");
        }
//	Construction du tableau des chemins d'acces aux classes
    try {
        str = System.getProperty ("java.class.path");
        }
    catch (Exception e) {
        str = new String ("");
        }
    if (str.length () > 0)
        nbPath = 1;
    for (i = 0; i < str.length (); i++)
        if (str.charAt (i) == pSep)
            nbPath++;
    classPath = new String[nbPath];
    i = 0;
    tmp = "";
    for (j = 0; j < str.length (); j++)
        if (str.charAt (j) == pSep) {
            classPath[i] = new String(tmp);
            i++;
            tmp = "";
        	}
        else 
            tmp = tmp + str.charAt (j);
    classPath[i] = new String(tmp);
	}
    
/* ****************************************************************************
 * Constructeurs
 */
/* ------------------------------------------------------------------------- */
/**
 * Constructeur de base.
 * @param directory Identificateur de l'instance.
 */
public Loader (String directory) {
//	Construction du chemin d'acces aux classes utilisateur
    classHome = new String(directory);
    }

/* ****************************************************************************
 * Methodes publiques
 */
/* ------------------------------------------------------------------------- */
/**
 * Chargement d'une classe utilisateur.
 * @param name Nom de la classe a charger.
 * @return La classe chargee.
 */
public byte[] getClassData (String name) {
    byte[] data = null;             // Code de la classe
    File fc;						// Descripteur du fichier des classes
    InputStream in;					// Flux de lecture
    
	try {
	    fc = new File (classHome + fSep + name + ".class");
	    if (!fc.exists ())
	        return null;
        in = new FileInputStream (fc);
        data = new byte[in.available ()];
        in.read (data);
        in.close ();
		}
	catch (Exception e) {
	           System.out.println("Unable to load class" + e);
		}
	catch (Error e) {
	           System.out.println("Unable to load class"+ e);
		}
    return data;
    }

/* ------------------------------------------------------------------------- */
/**
 * Obtention du repertoire de base d'une classe.
 * @param name Nom de la classe.
 * @return Repertoire de la classe, ou <b>null</b> si pas trouvee.
 */
public static String getDirectory (String name) {
    File file;                      // Structure de fichier de classe
    String fileName;                // Nom du fichier de classe
    int i = 0;                      // Variable de boucle
    
    fileName = name.replace ('.', fSep.charAt (0)) + ".class";
    file = new File (System.getProperty ("user.dir") + fSep + fileName);
    if (file.exists ())
        return new String (System.getProperty ("user.dir"));
    while (i < classPath.length) {
        file = new File (classPath[i] + fSep + fileName);
        if (file.exists ())
            return new String (classPath[i]);
        i++;
        }
    return null;
    }

/* ------------------------------------------------------------------------- */
/**
 * Chargement d'une classe.
 * @param name Nom de la classe a charger.
 * @param resolve Indicateur si resolution demandee de la classe.
 * @throws ClassNotFoundException Si la classe n'a pas ete trouvee.
 * @return La classe chargee.
 */
public Class loadClass (String name, boolean resolve) 
        throws ClassNotFoundException {
    Class theClass = null;			// Classe chargee

//  Essai de chargement depuis le cache systeme
    theClass = findLoadedClass (name);
//  Essai de chargement utilisateur
    if (theClass == null) 
        theClass = findUserClass (name);
//	Essai de chargement local
    if (theClass == null)
        theClass = findLocalClass (name);
//	Essai de chargement systeme        
    if (theClass == null)
        theClass = findSystemClass (name);
//	Resolution conditionnelle
    if (resolve && theClass != null)
        resolveClass (theClass);
//	Exception si aucune methode n'a fonctionne
    if (theClass == null)
        throw new ClassNotFoundException (name);
    return theClass;
    }

/* ------------------------------------------------------------------------- */
/**
 * Chargement du code binaire d'une classe.
 * @param directory Repertoire de la classe.
 * @param className Nom de la classe a charger.
 * @return Code binaire de la classe, ou <b>null</b> si une erreur s'est 
 * produite.
 */
public static byte[] loadData (String directory, String className) {
    String fileName;                // Nom du fichier a charger
    InputStream in;                 // Flux de lecture de la classe
    byte[] data;                    // Code de la classe
    
    try {
        fileName = directory + "/" + className.replace('.','/') +  ".class";
        in = new FileInputStream (fileName);
        data = new byte[in.available ()];
        in.read (data);
        in.close ();
        return data;
        }
    catch (Exception e) {
        System.out.println("Can't load file " + className + ".class");
        return null;
        }
    }

/* ****************************************************************************
 * Methodes privees
 */
/* ------------------------------------------------------------------------- */
/**
 * Chargement local d'une classe.
 * @param name Nom de la classe a charger.
 * @return La classe chargee.
 */
private Class findLocalClass (String name) {
    File file;					// Structure de fichier de classe
    String fileName;				// Nom du fichier de classe
    FileInputStream fis;
    int i = 0;						// Variable de boucle
    byte[] raw;						// Donnees brutes lues
    Class theClass = null;			// Classe chargee
    
    fileName = name.replace ('.', fSep.charAt (0)) + ".class";
    while (theClass == null && i < classPath.length) {
        file = new File (classPath[i] + fSep + fileName);
    	if (file.exists ()) {
    	    try {
    	        fis = new FileInputStream (file);
    	        raw = new byte[(int)file.length ()];
    	        if (fis.read (raw) == file.length ())
    	            theClass = defineClass (name, raw, 0, raw.length);
    	        fis.close ();
        		}
    	    catch (IOException ie) {
//              Rien a faire ici
                }
    		}
    	i++;
    	}
    return theClass;
    }

/* ------------------------------------------------------------------------- */
/**
 * Chargement d'une classe utilisateur.
 * @param name Nom de la classe a charger.
 * @return La classe chargee.
 */
private Class findUserClass (String name) {
    byte[] raw;						// Donnees brutes de la classe
    
    raw = getClassData (name);
    if (raw == null) 
        return null;
    return defineClass (name, raw, 0, raw.length);
    }

}

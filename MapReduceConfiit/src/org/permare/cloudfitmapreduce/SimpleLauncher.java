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

import cloudfit.application.ApplicationInterface;
import java.io.*;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.Iterator;
import java.util.Set;
import java.util.Vector;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.xmlrpc.XmlRpcException;
import org.apache.xmlrpc.client.XmlRpcClientConfigImpl;
import org.apache.xmlrpc.client.util.ClientFactory;
import org.permare.util.FileHandler;
import org.permare.util.MultiMap;

public class SimpleLauncher<K, V> {

    private ApplicationInterface mapperClass;
    private ApplicationInterface reducerClass;
    private String[] mapargs;
    private String outputDirectory;

    public SimpleLauncher() {
        this.mapperClass = null;
        this.reducerClass = null;
    }

    public void setReducer(ApplicationInterface classname) {
        this.reducerClass = classname;
    }

    public void setMapper(ApplicationInterface classname) {
        this.mapperClass = classname;
    }

    public ApplicationInterface getMapper() {
        return this.mapperClass;
    }

    public ApplicationInterface getReducer() {
        return this.reducerClass;
    }

    public String[] getMapArguments() {
        return mapargs;
    }

    public void setMapArguments(String[] args) {
        this.mapargs = args;
    }

    public String getOutputDirectory() {
        return outputDirectory;
    }

    public void setOutputDirectory(String output) {
        this.outputDirectory = output;
    }

    public MultiMap<K, V> runJob() {
        Serializable mapper;                  // Result de l'instance Mapper
        MultiMap<K, V> intRes = null;

        intRes = (MultiMap<K, V>) this.runMapper();

        String[] reduceargs = new String[2];
        //System.out.println("Fini !");
        //this.saveMapOutput(intRes);
//        TDTR.save("map", intRes);

////    
        System.out.println("Starting reduce");
        reduceargs[0] = "map";
        //reduceargs[1] indique combien de tasks REDUCE seront créées
        //reduceargs[1] = Integer.toString(community.getNodes());
        reduceargs[1] = Integer.toString(16);

//        intRes = (MultiMap<K, V>) this.runReducer(reduceargs);
        //TDTR.save("reduce", intRes);
//        this.saveOutput(intRes);
        return intRes;
    }

    private Serializable runMapper() {
        Integer mapperId = new Integer(0);// Identifiant de l'instance Mapper
        Serializable result = null;
        //String serverUrl = "http://localhost:8080/RPC2";
        System.out.println("init runmapper");
        try {
            XmlRpcClientConfigImpl config = new XmlRpcClientConfigImpl();
            config.setServerURL(new URL("http://127.0.0.1:8080/xmlrpc"));
            config.setEnabledForExtensions(true);
            config.setConnectionTimeout(60 * 1000);
            config.setReplyTimeout(60 * 1000);
            org.apache.xmlrpc.client.XmlRpcClient client = new org.apache.xmlrpc.client.XmlRpcClient();
            client.setConfig(config);
            ClientFactory factory = new ClientFactory(client);
            
//            Vector params = new Vector();
//            params.addElement(Base64Coder.toString(this.getMapper()));
//            params.addElement(this.getMapArguments());
            Object[] params = new Object[]
              { this.getMapper().getClass().getCanonicalName(), this.getMapArguments()};
            System.out.println("Parameter ok");
            System.out.println(client.execute("Submitter.plugI", params));
            //mapperId = (Integer)toServer.execute("Submitter.plugI", params);
            System.out.println("mapperId = " + mapperId);
//                Vector params2 = new Vector();
//                params2.addElement(mapperId);
//                result = (Serializable) toServer.execute("Submitter.waitJob", params2);
//                             
        } catch (XmlRpcException exception) {
            System.err.println("JavaClient: XML-RPC Fault #"
                    + Integer.toString(exception.code) + ": "
                    + exception.toString());
        } catch (Exception exception) {
            System.err.println("JavaClient: " + exception.toString());
        }

        return result;

    }

    private Serializable runReducer(String[] reduceargs) {
        Serializable res = null;
        String serverUrl = "http://localhost:8080/RPC2";
        Integer reducerId = null;
        try {
            
            XmlRpcClientConfigImpl config = new XmlRpcClientConfigImpl();
            config.setServerURL(new URL("http://127.0.0.1:8080/xmlrpc"));
            config.setEnabledForExtensions(true);
            config.setConnectionTimeout(60 * 1000);
            config.setReplyTimeout(60 * 1000);
            org.apache.xmlrpc.client.XmlRpcClient client = new org.apache.xmlrpc.client.XmlRpcClient();
            client.setConfig(config);
            ClientFactory factory = new ClientFactory(client);
            
            Object[] params = new Object[]
              { this.getReducer(), reduceargs};
            
            reducerId = (Integer) client.execute("Submitter.plug", params);
            System.out.println("reducerId = " + reducerId);
            Vector params2 = new Vector();
            params2.addElement(reducerId);
            res = (Serializable) client.execute("Submitter.waitJob", params2);

        } catch (XmlRpcException exception) {
            System.err.println("JavaClient: XML-RPC Fault #"
                    + Integer.toString(exception.code) + ": "
                    + exception.toString());
        } catch (Exception exception) {
            System.err.println("JavaClient: " + exception.toString());
        }
        return res;
    }

    private void saveMapOutput(MultiMap<K, V> intRes) {
        Set<K> keys = intRes.getKeys();
        FileHandler fhandler;
        File outfile, outdir;

        outdir = new File(this.getOutputDirectory());
        if (!outdir.exists()) {
            outdir.mkdir();
        }
        outfile = new File(this.getOutputDirectory().concat("/temp-0000"));

        fhandler = new FileHandler(outfile);
        if (fhandler.open(FileHandler.WRITE)) {

            Iterator<K> ikeys = keys.iterator();
            while (ikeys.hasNext()) {
                K key = ikeys.next();
                Iterator<V> it = intRes.keyIterator(key);

                while (it.hasNext()) {
                    V group = it.next();
                    String line = String.format("%s = %s\n", key.toString(), group.toString());
                    fhandler.writeLine(line);
                }
            }

            fhandler.flushing();
            fhandler.close();
        }
    }

    private void saveOutput(MultiMap<K, V> intRes) {
        Set<K> keys = intRes.getKeys();
        FileHandler fhandler;
        File outfile, outdir;

        outdir = new File(this.getOutputDirectory());
        if (!outdir.exists()) {
            outdir.mkdir();
        }
        outfile = new File(this.getOutputDirectory().concat("/part-00000"));

        fhandler = new FileHandler(outfile);
        if (fhandler.open(FileHandler.WRITE)) {

            Iterator<K> ikeys = keys.iterator();
            while (ikeys.hasNext()) {
                K key = ikeys.next();
                Iterator<V> it = intRes.keyIterator(key);

                while (it.hasNext()) {
                    V group = it.next();
                    String line = String.format("%s = %s\n", key.toString(), group.toString());
                    fhandler.writeLine(line);
                }
            }

            fhandler.flushing();
            fhandler.close();
        }
    }

    public static void main(String[] args) {
        long start;
        long end;

        Options options = new Options();
        Option help = new Option("help", "print this message");
        Option sourceDir = OptionBuilder.withArgName("src")
                .hasArg()
                .withDescription("Source directory for map-reduce data")
                .create("src");
        //sourceDir.setRequired(true);
        Option destDir = OptionBuilder.withArgName("dst")
                .hasArg()
                .withDescription("Destination directory for map-reduce data")
                .create("dst");
        //destDir.setRequired(true);
        Option node = OptionBuilder.withArgName("node")
                .hasArg()
                .withDescription("Optional address to join the P2P network")
                .create("node");
        Option port = OptionBuilder.withArgName("port")
                .hasArg()
                .withDescription("Optional port to join the P2P network")
                .create("port");

        options.addOption(sourceDir);
        options.addOption(destDir);
        options.addOption(node);
        options.addOption(port);
        options.addOption(help);

        // create the parser
        CommandLineParser parser = new PosixParser();
        CommandLine line = null;
        try {
            // parse the command line arguments
            line = parser.parse(options, args);
        } catch (ParseException exp) {
            // oops, something went wrong
            System.err.println("Parsing failed.  Reason: " + exp.getMessage());
            usage(options);
            return;
        }

        // OPTION PARSING
        // is there a "node" and "port" option ?
        InetSocketAddress peer = null; // the defaut value = discovery

        if (line.hasOption("node")) {
            if (line.hasOption("port")) {
                peer = new InetSocketAddress(line.getOptionValue("node"), Integer.parseInt(line.getOptionValue("port")));
            } else {
                peer = new InetSocketAddress(line.getOptionValue("node"), 7777);
            }

        }

        boolean master = false;

        String[] initArgs = new String[2];
        if (line.hasOption("src") && line.hasOption("dst")) {
            initArgs[0] = line.getOptionValue("src");
            initArgs[1] = line.getOptionValue("dst");
            master = true;
        }

        try {
            SimpleLauncher<String, Integer> job = new SimpleLauncher<String, Integer>();

            job.setOutputDirectory(initArgs[1]);
            job.setMapper(new Mapper());
            job.setMapArguments(initArgs);

            job.setReducer(new Reducer());

            //job.setReducer("Reducer");
            if (master) {
                start = System.currentTimeMillis();

                MultiMap<String, Integer> res = job.runJob();

                end = System.currentTimeMillis();

                Thread.sleep(3000);

                System.err.println("Total time = " + (end - start));

                System.out.println("Total time = " + (end - start));

                System.exit(0);
            }

        } catch (Exception ex) {
            System.err.println(ex.getMessage());
        }
    }

    private static void usage(Options options) {

        // Use the inbuilt formatter class
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("MRLauncher", options);

    }

    private static void countTotal(MultiMap<String, Integer> res) {
        Iterator<String> ikeys = res.getKeys().iterator();
        int count = 0;
        while (ikeys.hasNext()) {
            String key = (String) ikeys.next();
            Iterator<Integer> iValues = res.keyIterator(key);
            while (iValues.hasNext()) {
                Integer v = iValues.next();
                count += v.intValue();
            }
        }
        System.out.println("Count = " + count);
    }
}

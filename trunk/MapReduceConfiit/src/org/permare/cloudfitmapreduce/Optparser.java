/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.permare.cloudfitmapreduce;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

/**
 *
 * @author Luiz Angelo STEFFENEL <Luiz-Angelo.Steffenel@univ-reims.fr>
 */
public class Optparser {

    public static void main(String[] args) {
        Options options = new Options();
        Option help = new Option("help", "print this message");
        Option sourceDir = OptionBuilder.withArgName("src")
                .hasArg()
                .withDescription("Source directory for map-reduce data")
                .create("src");
        Option destDir = OptionBuilder.withArgName("dst")
                .hasArg()
                .withDescription("Destination directory for map-reduce data")
                .create("dst");
        Option node = OptionBuilder.withArgName("node")
                .hasArg()
                .withDescription("Optional address:port to join the P2P network")
                .create("node");

        options.addOption(sourceDir);
        options.addOption(destDir);
        options.addOption(node);


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

        if (line.hasOption("src")) {

            System.out.println("You have given argument is src");
            System.err.println(line.getOptionValue("src"));
        }

        if (line.hasOption("dst")) {
            System.out.println("You have given argument is dest");
            System.err.println("Nice to meet you: " + line.getOptionValue("dst"));
        }
        
        if (line.hasOption("node")) {
            System.out.println("You have given argument is node");
            System.err.println("Nice to meet you: " + line.getOptionValue("node"));
        }

    }

    private static void usage(Options options) {

// Use the inbuilt formatter class
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("CLIDemo", options);

    }
}

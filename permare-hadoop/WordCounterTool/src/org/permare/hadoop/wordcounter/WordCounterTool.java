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

package org.permare.hadoop.wordcounter;

import java.io.IOException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * WordCounterTool launches a Word Counter example using a ToolRunner.  
 * @author kirsch
 */
public class WordCounterTool extends Configured implements Tool {

    /**
     * @param args input output directories 
     */
    public static void main(String[] args) throws Exception {
        long start;
        long end;
        
        start = System.currentTimeMillis();
        
        int exitCode = ToolRunner.run(new WordCounterTool(), args);
        
        end = System.currentTimeMillis();

        System.out.println("Total time = " + (end - start));
    }

    @Override
    public int run(String[] args) throws IOException {
        JobConf conf = new JobConf(getClass());
        conf.setJobName("Word count with hadoop and ToolRunner");

        conf.setMapperClass(org.permare.hadoop.wordcounter.CounterExampleHadoop.class);
        conf.setReducerClass(org.permare.hadoop.wordcounter.CounterExampleHadoop.class);
        
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        Path outputpath = new Path(args[1]);
        FileOutputFormat.setOutputPath(conf, outputpath);
        
        JobClient.runJob(conf);
        
        return 0;
    }
}

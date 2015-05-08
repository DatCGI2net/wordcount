/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.wordcount;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 *
 * @author dat
 */
public class WCMain extends Configured implements Tool{

    @Override
    public int run(String[] args) throws Exception {
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        JobConf conf = new JobConf(WCMain.class);
        conf.setJobName("WordCount");
        
        // key value
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);
        
        // mapper and reducer
        
        conf.setMapperClass(WCMapper.class);
        conf.setReducerClass(WCReducer.class);
        
        // input output format
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
                
        FileInputFormat.addInputPath(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        
        JobClient.runJob(conf);
        return 0;
    }
    
    public static class WCMapper extends MapReduceBase implements org.apache.hadoop.mapred.Mapper<LongWritable, Text, Text, IntWritable>{

    private static IntWritable one = new IntWritable(1);
    private org.apache.hadoop.io.Text word = new  org.apache.hadoop.io.Text();

    @Override
    public void map(LongWritable k1, Text v1, OutputCollector<Text, IntWritable> oc, Reporter rprtr) throws IOException {
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        
        String line =v1.toString();
        // remove unwanted chars
        line=line.replaceAll("\r", "");
        line=line.replaceAll("\n", " ");
        StringTokenizer tokenizers = new StringTokenizer(line);
        while(tokenizers.hasMoreTokens()){
            word.set(tokenizers.nextToken());
            
            oc.collect(word, one);
            //String w = tokenizers.nextToken().toString();
            //oc.collect(new org.apache.hadoop.io.Text(w), one);
            
        }        
        
    }
    
}
    
    public static class WCReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>{

    @Override
    public void reduce(Text k2, Iterator<IntWritable> itrtr, OutputCollector<Text, IntWritable> oc, Reporter rprtr) throws IOException {
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        int sum=0;
        while(itrtr.hasNext())
            sum+=itrtr.next().get();
        
        oc.collect(k2, new IntWritable(sum));
        
        
    }
    
}
    
    
    public static void main(String[] args) throws IOException{
        
        try {
            int exitCode=ToolRunner.run(new WCMain(), args);
            
            System.exit(exitCode);
        } catch (Exception ex) {
            Logger.getLogger(WCMain.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        
       
    }
            
    
}

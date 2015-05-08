/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.wordcount;

import java.io.IOException;
import java.util.StringTokenizer;
import javafx.scene.text.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 *
 * @author dat
 */
public class WCMapper extends MapReduceBase implements org.apache.hadoop.mapred.Mapper<LongWritable, Text, Text, IntWritable>{

    private static IntWritable one = new IntWritable(1);
    

    @Override
    public void map(LongWritable k1, Text v1, OutputCollector<Text, IntWritable> oc, Reporter rprtr) throws IOException {
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        
        String line =v1.toString();
        StringTokenizer tokenizers = new StringTokenizer(line);
        while(tokenizers.hasMoreTokens()){
            Text word = new Text(tokenizers.nextToken());
            oc.collect(word, this.one);
            
        }        
        
    }
    
}

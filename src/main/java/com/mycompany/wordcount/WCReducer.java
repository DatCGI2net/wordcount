/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.wordcount;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 *
 * @author dat
 */
public class WCReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>{

    @Override
    public void reduce(Text k2, Iterator<IntWritable> itrtr, OutputCollector<Text, IntWritable> oc, Reporter rprtr) throws IOException {
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        int sum=0;
        while(itrtr.hasNext())
            sum+=itrtr.next().get();
        
        oc.collect(k2, new IntWritable(sum));
        
        
    }
    
}

package com.hehe.tf_idf;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FirstReduce extends Reducer<Text, IntWritable,Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for(IntWritable i : values){
            sum +=  i.get();
        }
        if(key.equals(new Text("count"))){
            System.out.println(key.toString()+"-------------------");
        }
        context.write(key,new IntWritable(sum));
    }
}

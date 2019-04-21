package com.hehe.weather;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TQMR {


    public static void main(String[] args) throws Exception {
        //1,conf
        Configuration conf = new Configuration(true);
        conf.set("fs.defaultFS", "hdfs://node02:8020");
        conf.set("yarn.resourcemanager.hostname", "node03:8088");

        //2,job
        Job job = Job.getInstance(conf);
        job.setJarByClass(TQMR.class);
        //3,input,output

        Path input = new Path("/input/tq");
        FileInputFormat.addInputPath(job, input);

        Path output = new Path("/data/weather/output");
        if(output.getFileSystem(conf).exists(output)){
            output.getFileSystem(conf).delete(output,true);
        }
        FileOutputFormat.setOutputPath(job, output );


        //4,map
        job.setMapperClass(TqMapper.class);
        job.setMapOutputKeyClass(TQ.class);
        job.setMapOutputValueClass(Text.class);

        //5,reduce

        job.setReducerClass(TqReducer.class);
        job.setNumReduceTasks(3);

        //6,other:sort,part..,group...
        job.setPartitionerClass(TqPartitioner.class);

//		二次排序--对温度倒序排序
        job.setSortComparatorClass(TqSortComparator.class);
        job.setGroupingComparatorClass(TqGroupingComparator.class);

//		job.setCombinerClass(TqReducer.class);
//	    map阶段的聚合
        job.setCombinerKeyGroupingComparatorClass(TqGroupingComparator.class);
        //7,submit

        job.waitForCompletion(true);
        System.out.println("success ~");








    }


}

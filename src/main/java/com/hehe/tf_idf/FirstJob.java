package com.hehe.tf_idf;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FirstJob {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1 配置环境
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://node02:8020");
        configuration.set("yarn.resourcemanager.hostname", "node02:8088");

        //2 创建job
        FileSystem fileSystem = FileSystem.get(configuration);
        Job job = Job.getInstance(configuration);
        job.setJarByClass(FirstJob.class);
        job.setJobName("weibo_1");

        //3 Map
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(FirstMapper.class);
        //4 Reduce
        job.setNumReduceTasks(4);
        job.setPartitionerClass(FirstPartition.class);
        job.setReducerClass(FirstReduce.class);
        //5 文件输入
        FileInputFormat.addInputPath(job,new Path("/data/tfidf/input/weibo.txt"));
        // 6 文件输出
        Path path = new Path("/data/tfidf/output/weibo_out");
        if(fileSystem.exists(path)){
            fileSystem.delete(path,true);
        }
        FileOutputFormat.setOutputPath(job,path);
        //7等待结束
        boolean flag = job.waitForCompletion (true);
        if (flag){
            System.out.println("第一步成功");
        }
    }
}

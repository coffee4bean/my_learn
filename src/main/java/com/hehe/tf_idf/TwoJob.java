package com.hehe.tf_idf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import javax.security.auth.login.AppConfigurationEntry;
import java.io.IOException;

public class TwoJob {
    public static void main(String[] args) throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://node01:8020");
        configuration.set("yarn.resourcemanager.hostname", "node02:8088");

        Job job = Job.getInstance(configuration);
        job.setJarByClass(TwoJob.class);
        job.setJobName("werino_2");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(TwoMapper.class);
        job.setReducerClass(TwoReduce.class);

    }
}

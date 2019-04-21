package com.hehe.tf_idf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;


public class LastJob {
    public static void main(String[] args) throws IOException {
        Configuration configuration = new Configuration() ;
        FileSystem fileSystem = FileSystem.get(configuration);
        Job job = Job.getInstance(configuration);
        job.setJarByClass(LastJob.class);
        job.setJobName("webio_3");
        //微博总数加载
        job.addCacheFile(new Path("/data/tfidf/output/weibo1/part-r-00003").toUri());
        //df加载
        job.addCacheFile(new Path("/data/tfidf/output/weibo2/part-r-00000").toUri());

        //设置map任务的输出类型,value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(LastMapper.class);
    }
}

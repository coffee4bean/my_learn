package com.hehe.my_weather;


import com.hehe.weather.TqSortComparator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import javax.xml.soap.Text;
import java.io.IOException;

public class TqMapReduce {
    public static void main(String[] args) throws IOException {
        //1 配置环境
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://node02:8020");
        configuration.set("yarn.resourcemanager.hostname","node03:8088");
        //2 创建job
        Job job = Job.getInstance(configuration);
        job.setJarByClass(TqMapReduce.class);
        job.setJobName("weather");
        //3 Map
        job.setMapperClass(Tq_mapper.class);
        job.setMapOutputKeyClass(Tq.class);
        job.setOutputValueClass(Text.class);
        //4 分区
        job.setNumReduceTasks(3);
        job.setPartitionerClass(TqPartitioner.class);
        //5 排序
        job.setSortComparatorClass(Tq_SortComparator.class);//组内排序
        job.setGroupingComparatorClass(Tq_SortComparator.class);//归并排序
        //6 Reduce
        job.setReducerClass(Tq_Reducer.class);
        //3 设置输入输出
        Path pathInput = new Path("/input/tq");
        FileInputFormat.addInputPath(job,pathInput);
        FileSystem fileSystem =FileSystem.get(configuration);
        Path pathOutput = new Path("/output/");
        if(fileSystem.exists(pathOutput)){
            fileSystem.delete(pathInput,true);
        }
        FileOutputFormat.setOutputPath(job,pathOutput);

    }
}

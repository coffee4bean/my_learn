package com.hehe.my_weather;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Tq_mapper extends Mapper<LongWritable, Text,Tq,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //   1949-10-01 14:21:02	34c
        //	 1949-10-01 19:21:02	38c
            String[] strings = value.toString().split("\t");
            String[] strs01 = strings[0].trim().split(" ")[0].trim().split("-");
            String str02 = strings[1].trim();
            String wd = str02.substring(0,str02.length()-1);
            Tq tq = new Tq();
            tq.setYear(Integer.parseInt(strs01[0]));
            tq.setMonth(Integer.parseInt(strs01[1]));
            tq.setDay(Integer.parseInt(strs01[2]));
            tq.setWd(Integer.parseInt(wd));
            Text wd_text = new Text(wd);
            context.write(tq,wd_text);
    }
}

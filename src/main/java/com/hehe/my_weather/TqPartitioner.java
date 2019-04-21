package com.hehe.my_weather;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class TqPartitioner extends Partitioner<Tq, Text> {
    @Override
    public int getPartition(Tq tq, Text text, int numPartitions) {
        return tq.getYear()%numPartitions;
    }
}

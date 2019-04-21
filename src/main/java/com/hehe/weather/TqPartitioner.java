package com.hehe.weather;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class TqPartitioner  extends HashPartitioner<TQ, Text> {

    @Override
    public int getPartition(TQ key, Text value, int numPartitions) {
        return key.getYear() % numPartitions;
    }


}

package com.hehe.my_weather;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.awt.*;
import java.io.IOException;

public class Tq_Reducer extends Reducer<Tq, Text,Text, Text> {
    @Override
    protected void reduce(Tq key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

    }
}

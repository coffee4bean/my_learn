package com.hehe.my_weather;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class Tq_GroupingCompxarator extends WritableComparator {
    public Tq_GroupingCompxarator() {
        super(Tq.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        Tq ta = (Tq) a;
        Tq tb = (Tq) b;
        return ta.getYear()-tb.getYear();
    }
}

package com.hehe.my_weather;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class Tq_SortComparator extends WritableComparator {
    public Tq_SortComparator() {
        super(Tq.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        Tq ta = (Tq)a;
        Tq tb = (Tq)b;
        if(ta.getYear().equals((tb.getYear()))){
            if(ta.getMonth().equals(tb.getMonth())){
                return tb.getWd()-ta.getWd();
            }
            return ta.getMonth()-tb.getMonth();
        }
        return ta.getYear() - tb.getYear();
    }
}

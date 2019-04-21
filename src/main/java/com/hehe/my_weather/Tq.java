package com.hehe.my_weather;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Tq implements WritableComparable<Tq> {
    private Integer year;
    private Integer month;
    private Integer day;
    private Integer wd;

    @Override
    public int compareTo(Tq o) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(year);
        dataOutput.writeInt(month);
        dataOutput.writeInt(day);
        dataOutput.writeInt(wd);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.year= dataInput.readInt();
        this.month = dataInput.readInt();
        this.day = dataInput.readInt();
        this.wd = dataInput.readInt();
    }

    public Integer getYear() {
        return year;
    }

    public void setYear(Integer year) {
        this.year = year;
    }

    public Integer getMonth() {
        return month;
    }

    public void setMonth(Integer month) {
        this.month = month;
    }

    public Integer getDay() {
        return day;
    }

    public void setDay(Integer day) {
        this.day = day;
    }

    public Integer getWd() {
        return wd;
    }

    public void setWd(Integer wd) {
        this.wd = wd;
    }
}

package com.hehe.tf_idf;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class LastMapper extends Mapper<LongWritable, Text,Text,Text> {
        //存放 微博总数
    public static Map<String,Integer>  cmap =null;
        //存放df
    public  static Map<String,Integer> df = null;

    //在map 方法执行之前
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        System.out.println("****************");
        if(cmap == null||cmap.size() == 0||df  == null||df.size() == 0){
            URI[] ss = context.getCacheFiles();
            if(ss !=  null){
                for (int i = 0; i <ss.length ; i++) {
                    URI uri = ss[i];
                    if (uri.getPath().endsWith("part-r-00003")) {//微博总数
                        Path path =  new Path(uri.getPath());
                        BufferedReader bufferedReader = new BufferedReader(new FileReader(path.getName()));
                        String line = bufferedReader.readLine();
                        if(line.startsWith("count")) {
                            String[] ls = line.split("\t");
                            cmap = new HashMap<String,Integer>();
                            cmap.put(ls[0],Integer.parseInt(ls[1].trim()));
                        }
                        bufferedReader.close();
                    }  else if (uri.getPath().endsWith("Part-r-00000")){//词条的DF
                        df = new HashMap<String,Integer>();
                        Path path = new Path(uri.getPath());
                        BufferedReader bufferedReader = new BufferedReader(new FileReader(path.getName()));
                        String line;
                        while ((line = bufferedReader.readLine()) != null){
                            String[] ls = line.split("\t");

                        }
                    }
                }
            }

        }
    }
}

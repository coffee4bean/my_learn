package com.hehe.weather;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TqReducer extends Reducer<TQ, Text, Text, Text> {

    Text rkey = new Text();
    Text rval = new Text();
//	1949-10-01         34
//	1949-10-01         38
//	1949-10-01         37
//	1949-10-02         39

    @Override
    protected void reduce(TQ key, Iterable<Text> vals, Context context)
            throws IOException, InterruptedException {
        int flg=0;
        int day=0;

//		key: 1949-10-01        vals:{34,38,37}
//		key: 1949-10-02        vals:{39}
//		==>shuffle--merge（合并 二次排序）==> 最高温度
//		此时温度经过二次排序（<年月,温度>）之后温度就是最高
//		1949-10(-1)        38
//		1949-10(-1)        34
//		1949-10(-2)        36

//		1950-1(-1)         32
//

        for (Text v : vals) {
            if(flg==0){
                day=key.getDay();
                rkey.set(key.getYear()+"-"+key.getMonth()+"-"+key.getDay());
                rval.set(key.getWd()+"");
                System.out.println("*****"+key.getYear()+"-"+key.getMonth()+"-"+key.getDay());
                System.out.println("####"+key.getWd());
//				<年月,温度>
                context.write(rkey,rval );
                flg++;
            }
//			flg==1
            if(flg!=0 && day != key.getDay()){
                rkey.set(key.getYear()+"-"+key.getMonth()+"-"+key.getDay());
                rval.set(key.getWd()+"");
                context.write(rkey,rval );
                break;
            }

        }


    }

}

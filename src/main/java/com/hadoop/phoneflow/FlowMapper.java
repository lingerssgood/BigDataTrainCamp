package com.hadoop.phoneflow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//表示map的输入：keyin:LongWritable；valuein:TEXT;
//表示map的输出：keyout:TEXT;valueout:FlowBean
public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    Text k = new Text();
    FlowBean v = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //第一步：从文件中获取一行数据
        String line = value.toString();
        //第二步：按照制表符切分数据
        String[] splits = line.split("\t");
        //第三步：封装耀输出的对象的key值，key值为电话号码取每一行的第二个字符。
        k.set(splits[1]);
        //第四步：取每一行的倒数第2、3的数值.
        long downFlow = Long.parseLong(splits[splits.length - 2]);
        long upFlow = Long.parseLong(splits[splits.length - 3]);
        //第五步：封装输出的对象道value值
        v.setUpFlow(upFlow);
        v.setDownFlow(downFlow);
        //第六步：将经过map处理后的数据写出
        context.write(k, v);
    }
}


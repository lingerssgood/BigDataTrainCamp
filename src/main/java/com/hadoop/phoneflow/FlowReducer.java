package com.hadoop.phoneflow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
//表示reduce的输入：keyin:TEXT；valuein:FlowBean;
//表示reduce的输出：Text:TEXT;valueout:Text
public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    FlowBean v = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        //第一步：初始化变量值
        long sum_upflow = 0;
        long sum_downflow = 0;
        //第二步：对相同key值计算上行流量之和，下行流量之和
        for (FlowBean flowBean : values) {
            sum_downflow += flowBean.getDownFlow();
            sum_upflow += flowBean.getUpFlow();
        }
        //第三步：输出每个key的上行流量、下行流量、上下行流量之和
        v.set(sum_upflow, sum_downflow);
        //第四步：reduce输出
        context.write(key, v);
    }
}



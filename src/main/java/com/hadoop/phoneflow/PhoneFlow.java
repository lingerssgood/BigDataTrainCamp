package com.hadoop.phoneflow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//运行map与reduce的任务
public class PhoneFlow {
    public static void main(String[] args) throws Exception {
        args = new String[] { "/Users/qianyiming/IdeaProjects/phoneFlowDemo/src/main/resources/input/HTTP_20130313143750.dat", "/Users/qianyiming/IdeaProjects/phoneFlowDemo/src/main/resources/output" };

        //第一步：创建配置对象
        Configuration conf = new Configuration();
        //第二步：创建job对象
        Job job = Job.getInstance(conf);
        //第三步：设置jar的路径
        job.setJarByClass(PhoneFlow.class);
        //第四步：关联mapper和reduce
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);
        //第五步：设置mapper输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        //第六步：设置终端输出的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        //第七步：判断output文件夹是否存在，如果存在则删除
        Path path = new Path(args[1]);// 取第1个表示输出目录参数（第0个参数是输入目录）
        FileSystem fileSystem = path.getFileSystem(conf);// 根据path找到这个文件
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);// true的意思是，就算output有东西，也一带删除
        }
        //第八步：设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //第九步：提交作业
        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);
    }
}



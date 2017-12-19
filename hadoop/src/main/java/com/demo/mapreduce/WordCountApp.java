package com.demo.mapreduce;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

//作业：HDFS中有一个目录，里面有2个文件，要求对目录中的所有文件进行单词计数。如果目录中有子目录，子目录中可能有很多文件哪？
public class WordCountApp {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, WordCountApp.class.getSimpleName());
		job.setJarByClass(WordCountApp.class);//打成jar包执行，需要此行
		
		FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.200.100:9000/mapreduce-test.txt"));
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		String OUT_DIR = "hdfs://192.168.200.100:9000/mapreduce-test.out1";
		FileOutputFormat.setOutputPath(job, new Path(OUT_DIR));
		job.setOutputFormatClass(TextOutputFormat.class);
		
		deleteOutDir(conf, OUT_DIR);
		
		job.waitForCompletion(true);
	}

	public static void deleteOutDir(Configuration conf, String OUT_DIR)
			throws IOException, URISyntaxException {
		FileSystem fileSystem = FileSystem.get(new URI(OUT_DIR), conf);
		if(fileSystem.exists(new Path(OUT_DIR))){
			fileSystem.delete(new Path(OUT_DIR), true);
		}
	}
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
		//<k1,v1>是<0, hello	you>,<10,hello	me>
		
		Text k2 = new Text(); 
		LongWritable v2 = new LongWritable();
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			String[] words = value.toString().split(" ");
			for (String word : words) {
				//word表示每一行中的每个单词，即k2
				k2.set(word);
				v2.set(1L);
				
				context.write(k2, v2);
			}
		}
	}
	
	//map函数执行完的输出<hello,1><you,1><hello,1><me,1>
	//排序后的结果是<hello,1><hello,1><me,1><you,1>
	//分组后的结果是<hello,{1,1}><me,{1}><you,{1}>

	//<k3,v3>是<hello, 2>,<me, 1>,<you, 1>
	public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
		LongWritable v3 = new LongWritable();
		@Override
		protected void reduce(Text k2, Iterable<LongWritable> v2s,
				Reducer<Text, LongWritable, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			long count = 0L;
			for (LongWritable v2 : v2s) {
				count += v2.get();
			}
			v3.set(count);
			context.write(k2, v3);
		}
	}
}


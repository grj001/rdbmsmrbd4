package com.zhiyou.bd14.mysql;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.zhiyou.bd14.mysql.WriteToDB.WordCountDBWritable;

//把数据库中的某张表(word_count) 的数据抽取到hdfs上
public class ReadFromDB {
	
	//ReadFromDB
	public static class ReadFromDBMap
	extends Mapper
	<LongWritable, WordCountDBWritable, 
	Text, NullWritable>{
		
		private Text outKey = new Text();
		
		private NullWritable outValue = 
				NullWritable.get();
		
		private WordCountDBWritable wordCountDBWritable = 
				new WordCountDBWritable();

		@Override
		protected void map(
				LongWritable key, WordCountDBWritable value,
				Mapper<
					LongWritable, WordCountDBWritable, 
					Text, NullWritable
				>.Context context)
				throws IOException, InterruptedException {
			
			outKey.set(value.toString());
			
			context.write(outKey, outValue);
		}
	}
	
	
	//不需要Reduce
	public static class ReadFromReduce
	extends Reducer
	<Text, NullWritable, 
	Text, NullWritable>{
		
	}
	
	
	
	public static void main(String[] args) 
			throws 
			IOException, 
			ClassNotFoundException, 
			InterruptedException {
		
		Configuration conf = 
				new Configuration();
		
		//设置数据库驱动, 写在job上面
		DBConfiguration.configureDB(
				conf
				, "com.mysql.jdbc.Driver"
				, "jdbc:mysql://192.168.58.1:3306/test"
				, "root"
				, "root");
		
		//也可以这样设置数据库驱动连接
		/*
			conf.set("mapreduce.jdbc.driver.class", "com.mysql.jdbc.Driver");
			conf.set("mapreduce.jdbc.url", "jdbc:mysql://localhost:3306/test");
			conf.set("mapreduce.jdbc.username", "root");
			conf.set("mapreduce.jdbc.password", "root");
		*/
		
		Job job = Job.getInstance(conf, "读取数据库");
		job.addFileToClassPath(
				new Path("/read/mysql-connector-java-5.1.39.jar"));
		
		job.setJarByClass(ReadFromDB.class);
		
		job.setMapperClass(ReadFromDBMap.class);
		job.setNumReduceTasks(0);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		/*
		 *  (job		job
		 *  , WordCountDBWritable.class
		 *  , "word_count"
		 *  , ""
		 *  , "wc_count"
		 *  , "wc_word, wc_count"
		 *  //或者是"wc_word", "wc_count"
		 *  )
		 */
		DBInputFormat.setInput(
				job
				, WordCountDBWritable.class
				, "word_count"
				, ""
				, "wc_count"
				, "wc_word, wc_count");
		
		Path outputDir = 
				new Path("/user/out/DB/ReadFromDB");
		
		outputDir.getFileSystem(conf).delete(outputDir,true);
		
		FileOutputFormat.setOutputPath(job, outputDir);
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
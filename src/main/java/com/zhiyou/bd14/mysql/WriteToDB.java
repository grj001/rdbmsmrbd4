package com.zhiyou.bd14.mysql;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

//把 wordCount的结果写入到mysql
public class WriteToDB {
	
	//对应表
	//create table word_count
	// (wc_word varchar(255), wc_count integer)
	public static class WordCountDBWritable
	implements DBWritable,
	Writable{
		
		private String word;
		private int count;
		@Override
		public String toString() {
			return "WordCountDBWritable [word=" + word + ", count=" + count + "]";
		}
		public String getWord() {
			return word;
		}
		public void setWord(String word) {
			this.word = word;
		}
		public int getCount() {
			return count;
		}
		public void setCount(int count) {
			this.count = count;
		}
		
		//序列化
		//把数据写到数据库
		//insert into word_count(wc_word, wc_count) value(?, ?)
		@Override
		public void write(PreparedStatement statement) throws SQLException {
			statement.setString(1, this.word);
			statement.setInt(2, this.count);
		}
		
		//反序列化
		//从数据库中读取数据
		@Override
		public void readFields(ResultSet resultSet) throws SQLException {
			this.word = resultSet.getString("wc_word");
			this.count = resultSet.getInt("wc_count");
		}
		
		
		
		
		@Override
		public void write(DataOutput out) throws IOException {
			out.writeUTF(this.word);
			out.writeInt(this.count);
		}
		@Override
		public void readFields(DataInput in) throws IOException {
			this.word = in.readUTF();
			this.count = in.readInt();
		}
	}
	
	
	//WriteToDBMap
	public static class WriteToDBMap extends Mapper
	<LongWritable, Text, 
	Text, IntWritable>{
		
		private final IntWritable ONE = 
				new IntWritable(1);
		
		private String[] infos;
		private Text outKey = new Text();
		//key为一行的偏移量, value为一行的文本内容
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			infos = value.toString().split("\\s");
			
			//对一行的文字进行解析, 输出每一个单词, 和value:1
			for(String word : infos){
				outKey.set(word);
				context.write(outKey, ONE);
			}
		}
	}
	
	
	public static class WriteToDBReduce 
	extends Reducer
	<Text, IntWritable, 
	WordCountDBWritable, NullWritable>{
		
		private WordCountDBWritable outKey = 
				new WordCountDBWritable();
		
		private  NullWritable outValue = 
				NullWritable.get();
		
		private int sum;

		@Override
		protected void reduce(
				Text key, Iterable<IntWritable> values,
				Reducer<
				Text, IntWritable,
				WordCountDBWritable, NullWritable>.Context context)
				throws IOException, 
				InterruptedException {
			
			sum = 0;
			
			for(IntWritable value : values){
				sum += value.get();
			}
			
			//outKey为自己创建的对象
			//实现了DBWritable, 和Writable接口
			//reduce将这个对象作为键输出
			outKey.setCount(sum);
			outKey.setWord(key.toString());
			
			context.write(outKey, outValue);
		}	
	}
	
	
	//main
	public static void main(String[] args) 
			throws 
			IOException, 
			ClassNotFoundException, 
			InterruptedException {
		
		Configuration conf = 
				new Configuration();
		
		//设置数据库连接
		/*
		 * mysql需要开启数据库连接
		 * grant all privileges *.* to 'root'@'%'
		 * identified by 'root' with grant option;
		 * 
		 * flush privileges;
		 * 
		 */
		DBConfiguration.configureDB(
				conf
				, "com.mysql.jdbc.Driver"
				, "jdbc:mysql://192.168.58.1:3306/test"
						+ "?userSSL=false"
				, "root"
				, "root");
		
		Job job = Job.getInstance(conf, "写入mysql");
		
		job.setJarByClass(WriteToDB.class);
		
		//将job发布到节点运行时
		//reduce不知道会分到哪个节点上去
		//或者将mysql的驱动包放到
		//hadoop安装文件夹下share/common/lib/文件夹下
		job.addFileToClassPath(
				new Path(
				"/read/mysql-connector-java-5.1.39.jar"));
		
		job.setMapperClass(WriteToDBMap.class);
		job.setReducerClass(WriteToDBReduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(WordCountDBWritable.class);
		job.setOutputValueClass(NullWritable.class);
		
		//设置输入
		//master:9000/user/output/Acro/part-r-00000.avro
		FileInputFormat.addInputPath(
				job, 
				new Path("/user/user-logs-large.txt"));
		
		//设置输出
		DBOutputFormat.setOutput(job, "word_count", 2);
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
package com.zhiyou.bd14.mysql;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

//把wordCount的结果写入到mysql
public class WriteToDB {

	//对应表: 
	//word_count create table(wc_word varchar(255),
	//wc_count integer
	public static class WordCountDBWritable 
	implements DBWritable,
	Writable{
		
		private String word;
		private int count;
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
		//insert into word_count(wc_word, wc_count) value(?,?)
		@Override
		public void write(PreparedStatement statement) 
				throws SQLException{
			
			statement.setString(1, this.word);
			statement.setInt(2, this.count);
			
		}
		
		//反序列化
		//从数据库中读取数据
		@Override
		public void readFields(ResultSet resultSet) 
				throws SQLException {
			this.word = 
					resultSet.getString("wc_word");
			this.count = 
					resultSet.getInt("wc_count");
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
	
	public static class WriteToDBMap
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}

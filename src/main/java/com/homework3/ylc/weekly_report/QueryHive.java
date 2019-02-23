package com.homework3.ylc.weekly_report;

import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hive.HiveContext;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class QueryHive {
	
	private final SparkSession spark;
	private final JavaSparkContext jsc;
	private final HiveContext hiveContext;
	
	// 周报存放的本地路径
	private final static String basepath = "/home/hadoop";
	// hdfs的存放路径
	public static final String hdfs_url = "hdfs://cluster1:9000";
	
	public QueryHive(String appName)
	{
//    	//in dev mode
//      spark = SparkSession
//      	      .builder()
//      	      .appName("HbaseDemo")
//      	      .master("spark://cluster1:7077")
//      	      .config("spark.testing.memory", "471859200")
//      	      .getOrCreate();
      //in deploy mode
      spark = SparkSession
    	      .builder()
    	      .config("fs.defaultFS", "hdfs://cluster1:9000")
    	      .appName(appName)
    	      .getOrCreate();
      jsc = new JavaSparkContext(spark.sparkContext());
      hiveContext = new org.apache.spark.sql.hive.HiveContext(jsc);
		
	}
	
	static Function<Row, String> myFunc = new Function<Row, String>(){
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public String call(Row v1) throws Exception {
			// TODO Auto-generated method stub
			// 倒数第二个字段是日期
			int length = v1.length();
			return v1.getString(length - 2);
		}
		
	};
	
    public String make_filename(String start, String end)
    {
//    	String start = "2015-01-01";
//    	String end = "2015-01-07";
    	String filename = String.format("%s/weekly_report_%s_%s", basepath, start, end);
    	filename = filename.replace("-", "");
    	return filename;
    }
	
	public void upload2hdfs(String filename, String url)
    {
	    //
//	    String url = "hdfs://cluster1:9000";
	    Configuration conf = new Configuration(true);
		conf.set("fs.defaultFS", url);
		conf.setBoolean("dfs.support.append", true);
		
		System.err.println(String.format("Upload local: {%s} to hdfs: {%s}", filename, url + "/"));
		
		try {
			FileSystem fs = FileSystem.get(new URI(url),conf,"hadoop");
			fs.copyFromLocalFile(false, true, new Path(filename), new Path("/"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace(System.err);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace(System.err);
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace(System.err);
		}
    }
	
	public List<Row> run_sql(String sql)
	{
		Dataset<Row> results = hiveContext.sql(sql);
		JavaRDD<Row> rdd = results.toJavaRDD();
		List<Row> rows = rdd.collect();
		return rows;
	}
	
	public void make_report(String startdate, String enddate)
	{
	    try {
		String filename = make_filename(startdate, enddate);
	    FileWriter fw = new FileWriter(filename,false); //overwrite
	    //write content
	    String line = "";
	    String sepLine = "**********\n";
	    fw.write(sepLine);
	    fw.write(String.format("*****%s*****\n", "Weekly Report"));
	    fw.write(String.format("*From %s To %s*\n", startdate, enddate));
	    fw.write("\n");
	    fw.write("*****By yuliuchuan******\n");
	    fw.write("*17127141@bjtu.edu.cn*\n");
	    fw.write(sepLine);
		//std
	    fw.write("**Std Report**\n");
	    fw.write("Uid,Std\n");
		System.err.println("std");
		String sql = String.format("select uid,STD(behavior_total) from userbehaviorhour_uid where day_time >= '%s'"
				+ " and day_time <= '%s' group by uid", startdate, enddate);
		List<Row> std_rows = run_sql(sql);
		for(Row r: std_rows)
		{
			String uid = r.getString(0);
			String std = String.valueOf(r.getDouble(1));
			fw.write(uid + "," + std + "\n");
		}
		//avg
		System.err.println("avg");
		fw.write(sepLine);
	    fw.write("**Avg Report**\n");
	    fw.write("Uid,Avg\n");
		sql = String.format("select uid,AVG(behavior_total) from userbehaviorhour_uid where day_time >= '%s'"
				+ " and day_time <= '%s' group by uid", startdate, enddate);
		List<Row> avg_rows = run_sql(sql);
		for(Row r: avg_rows)
		{
			String uid = r.getString(0);
			String avg = String.valueOf(r.getDouble(1));
			fw.write(uid + "," + avg + "\n");
		}
		//max, min
		System.err.println("max min");
		fw.write(sepLine);
	    fw.write("**Max-Min Report**\n");
	    fw.write("Date:" + startdate + "\n");
		sql = String.format("select hour_time, sum(behavior_total) as s from "
				+ "userbehaviorhour_uid where day_time = '%s' group by hour_time", startdate);
		List<Row> sum_rows = run_sql(sql);
		int minHour = 0, maxHour = 0;
		int max = 0, min = 0;
		int i = 0;
		for(Row r: sum_rows)
		{
			int hour_time = r.getInt(0);
			int sum = (int)r.getLong(1);
			if(i == 0)
			{
				minHour = hour_time;
				maxHour = hour_time;
				max = sum;
				min = sum;
			}
			else
			{
				if(sum > max)
				{
					max = sum;
					maxHour = hour_time;
				}
				if(sum < min)
				{
					min = sum;
					minHour = hour_time;
				}
			}
			i += 1;
		}
	    fw.write("Hour,Max\n");
	    fw.write(maxHour + "," + max + "\n");
	    fw.write("Hour,Min\n");
	    fw.write(minHour + "," + min + "\n");
		
		// top 10 uid
		System.err.println("top 10 uid");
		fw.write(sepLine);
	    fw.write("**Active User Top 10 Report**\n");
	    fw.write("Uid,Hot\n");
		sql = String.format("select uid, sum(behavior_total) as s from "
				+ "userbehaviorhour_uid where day_time >= '%s' and "
				+ "day_time <= '%s' group by uid order by s desc limit 10", startdate, enddate);
		
		List<Row> top10_rows = run_sql(sql);
		for(Row r: top10_rows)
		{
			String uid = r.getString(0);
			int sum = (int)r.getLong(1);
			fw.write(uid + "," + sum + "\n");
		}
		//top 10 aid
		System.err.println("top 10 aid");
		fw.write(sepLine);
	    fw.write("**Active Article Top 10 Report**\n");
	    fw.write("Aid,Hot\n");
		sql = String.format("select aid, sum(behavior_total) as s "
				+ "from userbehaviorhour_aid where day_time >= '%s' "
				+ "and day_time <= '%s' group by aid order by s desc limit 10", startdate, enddate);
		top10_rows = run_sql(sql);
		
		for(Row r: top10_rows)
		{
			String aid = r.getString(0);
			int sum = (int)r.getLong(1);
			fw.write(aid + "," + sum + "\n");
		}
		fw.write(sepLine);
		fw.write(sepLine);
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		fw.write(String.format("*Finished@%s\n*", df.format(new Date())));
		fw.write(sepLine);
	    fw.close();
		upload2hdfs(filename, hdfs_url);
	    System.err.println("Finish");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(System.err);
		}
	}
}

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class Qs7 {
	
	public static class MapClass extends Mapper<LongWritable,Text,Text,IntWritable>{
		
		public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException{
			

            String[] str = value.toString().split("\t");
            
            String year =str[7];
            
            context.write(new Text(year),new IntWritable(1));
			
		
		}
		
	}
	
	public static class ReduceClass extends Reducer<Text,IntWritable,Text,IntWritable>{
		
		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
			
			int sum=0;
			
			
			String yr = key.toString();
			
			
			for(IntWritable val :values){
				
				
				if(yr.contains("2011")){
					
					sum +=val.get();
					
					
				}
				else if(yr.contains("2012")){
					
					sum +=val.get();
					
					
				}else if(yr.contains("2013")){
				
					sum +=val.get();
					
				}else if(yr.contains("2014")){
				
					sum +=val.get();
					
				}else if(yr.contains("2015")){
				
					sum +=val.get();
					
				}
				else  if(yr.contains("2016")){
				
					sum +=val.get();
					
				}
			
			}
			context.write(new Text(yr), new IntWritable(sum));
			
		
		}	
		
	}
	
	public static void main(String args[]) throws Exception{
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Question 7");
		
		job.setJarByClass(Qs7.class);
		job.setMapperClass(MapClass.class);
	    job.setReducerClass(ReduceClass.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	    
		
		
		
		
	}

}

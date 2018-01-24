import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Qs10 {

public static class MapClass extends Mapper<LongWritable,Text,Text,Text>{
		
		public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException{
			

            String[] str = value.toString().split("\t");
            
            String job_title =str[4];
            
            String case_status =str[1];
            
            context.write(new Text(job_title),new Text(case_status));
			
		
		}
		
	}




	
public static class ReduceClass extends Reducer<Text,Text,Text,Text>{
	
	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
	
		int count = 0;
		
		int c = 0;
	
		 for(Text val : values){
			 
			 String case_status = val.toString();
			 
			 count++;
			 
			 if(case_status.contains("CERTIFIED") || case_status.contains("CERTIFIED-WITHDRAWN")){
				 
				 c++;
				 
				 
			 }
			 
			
		
		 }
		 
		 
		 double success_rate = (c*100)/count;
		 
		 if(success_rate > 70 && c >= 1000){
			 
			 String job = key.toString();
			 
			 String my_val= job+","+ count+","+success_rate;
		 
			 context.write(new Text(job), new Text(my_val));
		 }
	
	
	
	}
}
	
	public static void main(String args[]) throws Exception{
		
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf , "Question 10");
	    job.setJarByClass(Qs10.class);
	    job.setMapperClass(MapClass.class);
	    job.setReducerClass(ReduceClass.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	    
		
		
		
		
		
		
	}
	
	


	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}

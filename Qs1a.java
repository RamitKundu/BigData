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




public class Qs1a {
	
public static class MapClass extends Mapper<LongWritable,Text,Text,Text>{
		
		public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException{
			

            String[] str = value.toString().split("\t");
            
            String year =str[7];
            //String case_status =str[1];
            if(str[4].equals("DATA ENGINEER")){
            	
            	String val = str[1]+","+str[4];
            	
            	 context.write(new Text(year),new Text(val));
            	
            }
            
           
			
		
		}
		
	}
	
public static class ReduceClass extends Reducer<Text,Text,Text,IntWritable>{
	
	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
		
		
		int count = 0;
		
		//String yr = key.toString();
		
		
		for(Text val :values){
			
			count++;
			
		
		}
		//String my_val = casestatus+","+count;
		context.write(new Text(key), new IntWritable(count));
		
	
	}	
	
}

public static void main(String args[]) throws Exception{
	 Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "Question 1a ");
	    job.setJarByClass(Qs1a.class);
	    job.setMapperClass(MapClass.class);
	    job.setReducerClass(ReduceClass.class);
	    //job.setNumReduceTasks(0);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	    
	    
	 
}
	
	
	

}

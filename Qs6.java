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





public class Qs6 {
	
 public static class MapClass extends Mapper<LongWritable,Text,Text,Text>
 { 
	 public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException
     {	    	  
        
            String[] str = value.toString().split("\t");
            String case_status= str[1];
            String year =str[7];
            
            context.write(new Text(year),new Text(case_status));
        
     }
 }
	 public static class ReduceClass extends Reducer<Text,Text,Text,Text>
	   {
		 public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
	    
		String  yr= key.toString();
		
		//String casestatus ="";
		 
		 double count = 0;
		 double certified = 0;
		 double certified_withdrawn = 0;
		 double withdrawn = 0;
		 double denied = 0;
		
		 
		 double c_percent = 0;
		 double cw_percent = 0;
		 double w_percent = 0;
		 double d_percent = 0;
		 
		 
		 
		 
		 for(Text val : values){
			 
			 String casestatus = val.toString();
			
			 if(yr.contains("2011")){
				 count++;
				 
				 if(casestatus.contains("CERTIFIED")){
					 certified++;
				 }
				
			    else if(casestatus.contains("CERTIFIED-WITHDRAWN")){
			    	certified_withdrawn++;
				 }

				 else  if(casestatus.contains("WITHDRAWN")){
					 withdrawn++;
				 }

				 else {
				 denied++;
				 
				 }
				 
				 c_percent =Math.round((certified/count)*100);
				  cw_percent =Math.round((certified_withdrawn/count)*100);
			      w_percent =Math.round((withdrawn/count)*100);
				  d_percent =Math.round((denied/count)*100);
				
				
				 
			 }
			 
			 if(yr.contains("2012")){
				 count++;
				 
				  if(casestatus.contains("CERTIFIED")){
					 certified++;
				 }
				
				  else if(casestatus.contains("CERTIFIED-WITHDRAWN")){
					  certified_withdrawn++;
				 }

				  else if(casestatus.contains("WITHDRAWN")){
					 withdrawn++;
				 }

				  else if(casestatus.contains("DENIED")){
					 denied++;
				 }
				  c_percent =Math.round((certified/count)*100);
				  cw_percent =Math.round((certified_withdrawn/count)*100);
			      w_percent =Math.round((withdrawn/count)*100);
				  d_percent =Math.round((denied/count)*100);
				
			  
				 
			 }
			 if(yr.contains("2013")){
				 count++;
				 
				 if(casestatus.contains("CERTIFIED")){
					 certified++;
				 }
				
				 else  if(casestatus.contains("CERTIFIED-WITHDRAWN")){
					 certified_withdrawn++;
				 }

				 else if(casestatus.contains("WITHDRAWN")){
					 withdrawn++;
				 }

				 else if(casestatus.contains("DENIED")){
					 denied++;
				 }
				 c_percent =Math.round((certified/count)*100);
				  cw_percent =Math.round((certified_withdrawn/count)*100);
			      w_percent =Math.round((withdrawn/count)*100);
				  d_percent =Math.round((denied/count)*100);
				
				  
			 }
			 
			 if(yr.contains("2014")){
				 count++;
				 
				 if(casestatus.contains("CERTIFIED")){
					 certified++;
				 }
				
				 else  if(casestatus.contains("CERTIFIED-WITHDRAWN")){
					 certified_withdrawn++;
				 }

				 else if(casestatus.contains("WITHDRAWN")){
					 withdrawn++;
				 }

				 else if(casestatus.contains("DENIED")){
					 denied++;
				 }
				 c_percent =Math.round((certified/count)*100);
				  cw_percent =Math.round((certified_withdrawn/count)*100);
			      w_percent =Math.round((withdrawn/count)*100);
				  d_percent =Math.round((denied/count)*100);
				
				 
			 }
			 
			 if(yr.contains("2015")){
				 count++;
				 
				 if(casestatus.contains("CERTIFIED")){
					 certified++;
				 }
				
				 else  if(casestatus.contains("CERTIFIED-WITHDRAWN")){
					 certified_withdrawn++;
				 }

				 else if(casestatus.contains("WITHDRAWN")){
					 withdrawn++;
				 }

				 else if(casestatus.contains("DENIED")){
					 denied++;
				 }
				 c_percent =Math.round((certified/count)*100);
				  cw_percent =Math.round((certified_withdrawn/count)*100);
			      w_percent =Math.round((withdrawn/count)*100);
				  d_percent =Math.round((denied/count)*100);
				

				 
			 }
			 
			 if(yr.contains("2016")){
				 count++;
				 
				 if(casestatus.contains("CERTIFIED")){
					 certified++;
				 }
				
				 else  if(casestatus.contains("CERTIFIED-WITHDRAWN")){
					 certified_withdrawn++;
				 }

				 else if(casestatus.contains("WITHDRAWN")){
					 withdrawn++;
				 }

				 else if(casestatus.contains("DENIED")){
					 denied++;
				 }
				 c_percent =Math.round((certified/count)*100);
				  cw_percent =Math.round((certified_withdrawn/count)*100);
			      w_percent =Math.round((withdrawn/count)*100);
				  d_percent =Math.round((denied/count)*100);
				

				 
			 }
			 
	 }
		 
		  
		 

String my_val= yr+","+count+","+certified+","+certified_withdrawn+","+withdrawn+","+denied+","
+c_percent+","+cw_percent+","+w_percent+","+d_percent;


context.write(new Text(yr), new Text(my_val));
			 
   } 
	   }

	 public static void main(String args[]) throws Exception{
		 Configuration conf = new Configuration();
		    Job job = Job.getInstance(conf, "Percentile and count case status for each year");
		    job.setJarByClass(Qs6.class);
		    job.setMapperClass(MapClass.class);
		    job.setReducerClass(ReduceClass.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		    
		    
		 
	 }
	   }




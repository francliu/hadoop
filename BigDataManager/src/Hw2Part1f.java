/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Modified by Shimin Chen to demonstrate functionality for Homework 2
// April-May 2015


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Hdfs;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;






public class Hw2Part1 {

public static class InfoWritable implements Writable{  
	  
    int count;  
    double time;
      
    InfoWritable(){}  
      
    InfoWritable(int count,double time){  
        this.count = count;  
        this.time = time;  
    }  
    
    public void SetValue(int count,double time){  
        this.count = count;  
        this.time = time ;  
    }  
    
    @Override  
      
    public void readFields(DataInput in) throws IOException {  
        // TODO Auto-generated method stub  
        this.count = in.readInt();  
        this.time = in.readDouble();  
    }  
  
    @Override  
    public void write(DataOutput out) throws IOException {  
        // TODO Auto-generated method stub  
        out.writeInt(count);  
        out.writeDouble(time);  
    }  
    public String toString(){  
        return count+" "+time;  
    }  
} 
  // This is the Mapper class
  // reference: http://hadoop.apache.org/docs/r2.6.0/api/org/apache/hadoop/mapreduce/Mapper.html
  //
  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, InfoWritable>{
    
    private Text source_destination = new Text();
    final InfoWritable info=new InfoWritable();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      
      // if the number of tokens is not 3,skip it
      if(itr.countTokens()!=3){
    	  System.out.println("number of tokens is not 3");
    	  return;
      }
    	  
      source_destination.set(itr.nextToken()+" "+itr.nextToken());//get the source_destination

      String timestr=itr.nextToken();
      
      // if the 3rd token is not double,skip it
      if(!timestr.matches("^[.\\d]*$")){
          System.out.println("the 3rd token is not double");
          return;
      }


      
      Double time=Double.parseDouble(timestr);//get the time
      
      
      int one=1;
      info.SetValue(one, time);
      
      context.write(source_destination, info);
      System.out.println(source_destination+"\t"+one+"\t"+time);
      
    }
  }
  
  public static class IntSumCombiner
       extends Reducer<Text,InfoWritable,Text,InfoWritable> {
    private InfoWritable result = new InfoWritable();

    public void reduce(Text key, Iterable<InfoWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int count = 0;
      double time=0;
      for (InfoWritable val : values) {
        count += val.count;
        time += val.time;
  	    System.out.println(key+":	"+val.count+",	"+val.time);
      }
      result.SetValue(count, time);
      context.write(key, result);
      System.out.println(key+":	"+count+",	"+time);
    }
  }

  // This is the Reducer class
  // reference http://hadoop.apache.org/docs/r2.6.0/api/org/apache/hadoop/mapreduce/Reducer.html
  //
  // We want to control the output format to look at the following:
  //
  // count of word = count
  //
  public static class IntSumReducer
       extends Reducer<Text,InfoWritable,Text,InfoWritable> {

    private Text result_key= new Text();
    private InfoWritable result_value= new InfoWritable();

    public void reduce(Text key, Iterable<InfoWritable> values, Context context) throws IOException, InterruptedException {
      int count = 0;
      double time=0;
      for (InfoWritable val : values) {
    	  count+=val.count;
    	  time+=val.time;
      }
      
      
      System.out.println(key+":"+count+","+time);
      
      time=time/(double)count;//get the average time
      
      time=(double)(Math.round(time*1000)/1000.0);//convert to the needed format
      
      System.out.println(time);


      result_key=key;

      // generate result value
//      result_value.set(Integer.toString(count)+" "+Double.toString(time));
      result_value.SetValue(count, time);
      
      context.write(result_key, result_value);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: <input file> <output directory>");
      System.exit(2);
    }
    
//    FileSystem hdfs = FileSystem.get(conf);
    String target= "hdfs://localhost:9000/";  
    FileSystem fs=FileSystem.get(URI.create(target),conf);//is diffrent
    Path outputpath = new Path(otherArgs[otherArgs.length - 1]);
    if(fs.exists(outputpath)){
    	fs.delete(outputpath, true);
    }


    Job job = Job.getInstance(conf, "Hw2Part1");

    job.setJarByClass(Hw2Part1.class);

    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumCombiner.class);
    job.setReducerClass(IntSumReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(InfoWritable.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(InfoWritable.class);

    // add the input paths as given by command line
    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }

    // add the output path as given by the command line
    FileOutputFormat.setOutputPath(job,new Path(otherArgs[otherArgs.length - 1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}



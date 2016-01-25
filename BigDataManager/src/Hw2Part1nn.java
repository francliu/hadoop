/** 201428013229111,liujianfei */

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.text.DecimalFormat;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class Hw2Part1 {
	public static class PairValue implements Writable {
		private long n;
		private double sum;
		public PairValue(){
			n = 1;
			sum = 0;
		}		
		void setNum(long c){
			n = c;
		}
		void setsum(double s){
			sum = s;
		}		
		long  getNum(){
			return n;
		}
		double getValue(){
			return sum;
		}
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			out.writeLong(n);
			out.writeDouble(sum);
		}
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			n = in.readLong();
			sum = in.readDouble();
		}
	}
  // This is the Mapper class
  // reference: http://hadoop.apache.org/docs/r2.6.0/api/org/apache/hadoop/mapreduce/Mapper.html
  //
  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, PairValue>{
    
   
    private PairValue pairValue= new PairValue();
    private Text word = new Text();
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

    	BufferedReader Reader = new BufferedReader(new StringReader(value.toString()));
		String line = null;
		String time = null;
		Pattern pattern = Pattern.compile("^[-\\+]?[\\d]*[.]?[\\d]*$");
		while((line = Reader.readLine()) != null){
			StringTokenizer itr = new StringTokenizer(line);
			if(itr.countTokens() == 3){
			      String source = itr.nextToken()+" ";
			      source+=itr.nextToken();
			      time = itr.nextToken();
			      word.set(source);
				if(pattern.matcher(time).matches()){
					if(Double.parseDouble(time)>=0)
					{
						pairValue.setsum(Double.parseDouble(time));
				    	context.write(word, pairValue);
					}
				}
			}
		}
    }
  }
  
  public static class IntSumCombiner
       extends Reducer<Text,PairValue,Text,PairValue> {
	private PairValue result = new PairValue();

	public void reduce(Text key, Iterable<PairValue> values,
			Context context) throws IOException, InterruptedException {
		long n = 0;
		double sum= 0;
		for (PairValue val : values) {
			n += val.getNum();
			sum += val.getValue();
		}
		result.setNum(n);
		result.setsum(sum);
		context.write(key, result);
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
    extends Reducer<Text,PairValue,Text,Text> {
	  
    private Text result_key= new Text();
    private Text result_value= new Text();
    private Text word = new Text();
    public void reduce(Text key, Iterable<PairValue> values, 
                       Context context
                       ) throws IOException, InterruptedException {
		long count = 0;
		double avg = 0;
		for (PairValue val : values) {
			count += val.getNum();
			avg += val.getValue();
		}

		// generate result key
		result_key.set(key);

		// generate result value
		avg /= count;
		DecimalFormat dFormat = new DecimalFormat("#.000");
		result_value.set(Long.toString(count) + " " + dFormat.format(avg));

		context.write(result_key, result_value);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: wordcount <in> [<in>...] <out>");
      System.exit(2);
    }

    Job job = Job.getInstance(conf, "hw2");

    job.setJarByClass(Hw2Part1.class);

	job.setMapperClass(TokenizerMapper.class);
	job.setCombinerClass(IntSumCombiner.class);
	job.setReducerClass(IntSumReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(PairValue.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    //delete the  output file 
    FileSystem hdfs = FileSystem.get(URI.create(args[0]),conf);
    hdfs.delete(new Path(otherArgs[otherArgs.length - 1]),true);
    // add the input paths as given by command line
    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }
    // add the output path as given by the command line
    FileOutputFormat.setOutputPath(job,
      new Path(otherArgs[otherArgs.length - 1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

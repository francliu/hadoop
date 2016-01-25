/*3,201428013229115,NiuGuocheng*/
import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.StringReader;
import java.util.StringTokenizer;
import java.text.DecimalFormat;
import java.net.URI;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FileSystem;
public class Hw2Part1 {
  
  public static class Durval implements Writable{
      private long cnt;
      private double sum;
      public Durval(){
      }
      void setCnt(long _cnt){
          cnt = _cnt;
      }
      long getCnt(){
          return cnt;
      }
      void setSum(double _sum){
          sum = _sum;
      }
      double getSum(){
          return sum;
      }
      public void write(DataOutput out) throws IOException {
        out.writeLong(cnt);
        out.writeDouble(sum);
      }
      public void readFields(DataInput in) throws IOException {
        cnt = in.readLong();
        sum = in.readDouble();
      }
  }
  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, Durval>{
    
    private Text word = new Text();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      
      BufferedReader buf = new BufferedReader(new StringReader(value.toString()));
      String line = null;
      while( (line = buf.readLine())!= null ){
        StringTokenizer itr = new StringTokenizer(value.toString());
        if(itr.countTokens() != 3) continue;
        String info = itr.nextToken();
        info += " ";
        info += itr.nextToken();
        Double tur;
        try{
            tur = Double.parseDouble(itr.nextToken());
        }catch(Exception e){
            continue;
        }
        if(tur < 0) continue;
        word.set(info);
        Durval t = new Durval();
        t.setSum(tur);
        t.setCnt(1);
        context.write(word,t);
      }
      
    }
  }
  
  public static class SumCombiner
       extends Reducer<Text,Durval,Text,Durval> {
    private Durval result = new Durval();

    public void reduce(Text key, Iterable<Durval> values,
                       Context context
                       ) throws IOException, InterruptedException {
      double sum = 0;
      long cnt = 0;
      for (Durval val : values) {
        cnt += val.getCnt();
        sum += val.getSum();
      }
      result.setCnt(cnt);result.setSum(sum);
      context.write(key, result);
    }
  }

  public static class SumReducer
       extends Reducer<Text,Durval,Text,Text> {

    private Text result_key= new Text();
    private Text result_value= new Text();
    
    public void reduce(Text key, Iterable<Durval> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      double sum = 0;
      long cnt = 0;
      for (Durval val : values) {
        sum += val.getSum();
        cnt += val.getCnt();
      }
      double avg = sum / cnt;
      DecimalFormat f = new DecimalFormat("#.000");
      result_key.set(key);
      result_value.set(Long.toString(cnt) + " " + f.format(avg));
      context.write(result_key, result_value);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: Hw2Part1 <in> [<in>...] <out>");
      System.exit(2);
    }

    Job job = Job.getInstance(conf, "hw2");

    job.setJarByClass(Hw2Part1.class);

    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(SumCombiner.class);
    job.setReducerClass(SumReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Durval.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
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


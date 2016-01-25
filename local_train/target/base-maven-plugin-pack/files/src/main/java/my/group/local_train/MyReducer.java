package my.group.local_train;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Reducer;

import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reducer模板。请用真实逻辑替换模板内容
 */
public class MyReducer implements Reducer {
    private Record result;

    public void setup(TaskContext context) throws IOException {
        result = context.createOutputRecord();
    }

    public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
    	/**
        long count = 0;
        
        */
    	
        result.set(0, key.get(0).toString()); 
        result.set(1, key.get(1).toString());
        result.set(2, key.get(2).toString());
        while (values.hasNext()) {
	            Record val = values.next();
	            String blog = val.get(0).toString();
	            Pattern patternfrist = Pattern.compile("转发");
	            double tranfer=0 ;
	            Matcher p1 = patternfrist.matcher(blog);
	            if(p1.find())
	            {
	           	 tranfer=1; 
	            }
	             result.set(3, tranfer);
	             Pattern patternsecond = Pattern.compile("赞");
	             double praise=0 ;
	             Matcher p2 = patternsecond.matcher(blog);
		         if(p2.find())
		         {
		            	praise=1; 
		          }
	            result.set(4, praise);
	            Pattern patternthrid = Pattern.compile(" 红包");
	            double hongbao=0 ;
	            Matcher p3 = patternthrid.matcher(blog);
		         if(p3.find())
		         {
		        	 hongbao=1; 
		          }
	           	result.set(5, hongbao);
	           	Pattern patternforth= Pattern.compile(" 快的");
	           	double kuaidi=0;
	           	Matcher p4 = patternforth.matcher(blog);
		         if(p4.find())
		         {
		        	 kuaidi=1; 
		          }
		          result.set(6, kuaidi);
	          	//@
	          	Pattern patternfive= Pattern.compile("@");
	          	double at_tag=0 ;
	          	Matcher p5 = patternfive.matcher(blog);
		         if(p5.find())
		         {
		        	 at_tag=1; 
		          }
		         result.set(7, at_tag);
		       //http
	          	Pattern patternhttp= Pattern.compile("htttp");
	          	double http_tag=0 ;
	          	Matcher p6 = patternhttp.matcher(blog);
		         if(p6.find())
		         {
		        	 http_tag=1; 
		          }
		         result.set(8, http_tag);
	         	//#
	         	Pattern patternsix= Pattern.compile("＃");
	         	double share_tag=0 ;
	         	Matcher p7 = patternsix.matcher(blog);
		         if(p7.find())
		         {
		        	 share_tag=1; 
		          }
		        result.set(9, share_tag);
		        result.set(10, val.get(1));
        }
        context.write(result);
        
    }

    public void cleanup(TaskContext arg0) throws IOException {

    }
}

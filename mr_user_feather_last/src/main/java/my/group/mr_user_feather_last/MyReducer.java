package my.group.mr_user_feather_last;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Reducer;
import com.aliyun.odps.mapred.Reducer.TaskContext;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Iterator;

/**
 * Reducer模板。请用真实逻辑替换模板内容
 */
public class MyReducer implements Reducer {
    private Record result;

    public void setup(TaskContext context) throws IOException {
        result = context.createOutputRecord();
    }
    public static double classify(double num){
    	double value = 1;
    	if(num<5)value = 1;
    	else if(num<=10)value = 2;
    	else if(num<=50)value = 3;
    	else if(num<=100)value = 4;
    	else value = 5;
		return value;
    }
    public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
    	double sum_tranfer_num = 0,sum_comment_num=0,sum_praise_num=0;
    	double max_tranfer_num = 0,max_comment_num=0,max_praise_num=0;
    	double min_tranfer_num = 50,min_comment_num=50,min_praise_num=50;
    	double max_interactive_level =0,min_interactive_level=0,mean_interactive_level=0;
    	long cnt=1;
    	double sum_level = 0;
    	 double level_mean =0;
        while (values.hasNext()) {
            Record val = values.next();
            double a,b,c;
            a = val.getDouble(0);b = val.getDouble(1);c = val.getDouble(2);
            sum_tranfer_num +=  a;
            sum_comment_num +=  b;
            sum_praise_num += c;
            double level_max = val.getDouble(12);
            double level_min = val.getDouble(13);
            level_mean = val.getDouble(14);
            sum_level +=level_mean;
            if(max_tranfer_num<a)max_tranfer_num=a;
            if(max_comment_num<b)max_comment_num=b;
            if(max_praise_num<c)max_praise_num=c;
            if(min_tranfer_num>a)min_tranfer_num=a;
            if(min_comment_num>b)min_comment_num=b;
            if(min_praise_num>c)min_praise_num=c;
            if(min_interactive_level>level_min)min_interactive_level=level_min;
            if(max_interactive_level<level_max)max_interactive_level=level_max;
            cnt++;
        }
        double means_transfer_num = classify(sum_tranfer_num/cnt);
        double  means_comment_num = classify(sum_comment_num/cnt);
        double means_praise_num =classify (sum_praise_num/cnt);
        mean_interactive_level = classify(sum_level/cnt);
        DecimalFormat f = new DecimalFormat("#.000");
        result.set(0,key.get(0).toString());
        result.set(1, max_tranfer_num);
        result.set(2, min_tranfer_num);
        result.set(3,means_transfer_num);
        result.set(4, sum_tranfer_num);
        result.set(5, max_comment_num);
        result.set(6, min_comment_num);
        result.set(7, means_comment_num);
        result.set(8, sum_comment_num);
        result.set(9, max_praise_num);
        result.set(10, min_praise_num);
        result.set(11,means_praise_num);
        result.set(12, sum_praise_num);
        result.set(13,max_interactive_level);
        result.set(14,min_interactive_level);
        result.set(15,mean_interactive_level);
        result.set(16,level_mean);
        context.write(result);
    }

    public void cleanup(TaskContext arg0) throws IOException {

    }
}

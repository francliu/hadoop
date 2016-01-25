package my.group.mr_user_feather_last;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Combiner模板。请用真实逻辑替换模板内容
 */
public class MyCombiner implements Reducer {
    private Record result;

    public void setup(TaskContext context) throws IOException {
    	result = context.createMapOutputValueRecord();
    }

    public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
    	/**
    	double sum_tranfer_num = 0,sum_comment_num=0,sum_praise_num=0;
    	double max_tranfer_num = 0,max_comment_num=0,max_praise_num=0;
    	double min_tranfer_num = 50,min_comment_num=50,min_praise_num=50;
    	long cnt=0;
    	*/
        while (values.hasNext()) {
            Record val = values.next();
            context.write(key, val);
            /**
            double a,b,c;
            a = val.getDouble(0);b = val.getDouble(1);c = val.getDouble(2);
            sum_tranfer_num +=  a;
            sum_comment_num +=  b;
            sum_praise_num += c;
            if(max_tranfer_num<a)max_tranfer_num=a;
            if(max_comment_num<b)max_comment_num=b;
            if(max_praise_num<c)max_praise_num=c;
            if(min_tranfer_num>a)min_tranfer_num=a;
            if(min_comment_num>b)min_comment_num=b;
            if(min_praise_num>c)min_praise_num=c;
            cnt++;
            */
        }
        /**
        result.set(0, sum_tranfer_num);
        result.set(1, sum_comment_num);
        result.set(2, sum_praise_num);
        result.set(3, max_tranfer_num);
        result.set(4, min_tranfer_num);
        result.set(5, sum_tranfer_num/cnt);
        result.set(6, max_comment_num);
        result.set(7, min_comment_num);
        result.set(8, sum_comment_num/cnt);
        result.set(9, max_praise_num);
        result.set(10, min_praise_num);
        result.set(11,sum_praise_num/cnt);
        context.write(key, result);
        */
    }

    public void cleanup(TaskContext arg0) throws IOException {

    }
}

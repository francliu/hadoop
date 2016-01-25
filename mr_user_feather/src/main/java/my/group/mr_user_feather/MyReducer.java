package my.group.mr_user_feather;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Reducer;

import java.io.IOException;
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
    	double tranfer_num = 0,comment_num=0,praise_num=0;
    	 while (values.hasNext()) {
             Record val = values.next();
             tranfer_num +=  (double)val.getDouble(0);
             comment_num += (double)val.getDouble(1);
             praise_num += (double)val.getDouble(2);
         }
    	 
        result.set(0, key.get(0).toString());
        result.set(1, key.get(1).toString());
        result.set(2, (tranfer_num));
        result.set(3, (comment_num));
        result.set(4, (praise_num));
        context.write(result);
    }

    public void cleanup(TaskContext arg0) throws IOException {

    }
}

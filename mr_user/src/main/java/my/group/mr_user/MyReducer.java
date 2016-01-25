package my.group.mr_user;

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
    	double tranfer_flag=0,comment_flag=0,praise_flag = 0;
    	result.set(0,key.get(0).toString());
    	result.set(1,key.get(1).toString());
    	//result.set(2,key.get(2).toString());
        while (values.hasNext()) {
        	Record val = values.next();
        	tranfer_flag+=val.getDouble(0);
        	comment_flag+=val.getDouble(1);
        	praise_flag+=val.getDouble(2);
        }
        result.set(2,tranfer_flag);
        result.set(3,comment_flag);
        result.set(4,praise_flag);
        result.set(5,classify(tranfer_flag+praise_flag+comment_flag));
        context.write(result);
    }

    public void cleanup(TaskContext arg0) throws IOException {

    }
}

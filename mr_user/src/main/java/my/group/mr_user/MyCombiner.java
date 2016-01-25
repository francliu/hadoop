package my.group.mr_user;

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
       // long c = 0;
    	double tranfer_flag=0,comment_flag=0,praise_flag = 0;
        while (values.hasNext()) {
        	Record val = values.next();
        	tranfer_flag+=val.getDouble(0);
        	comment_flag+=val.getDouble(1);
        	praise_flag+=val.getDouble(2);
        }
        result.set(0,tranfer_flag);
        result.set(1,comment_flag);
        result.set(2,praise_flag);
        context.write(key, result);
    }

    public void cleanup(TaskContext arg0) throws IOException {

    }
}

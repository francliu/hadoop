package my.group.test_local_true_result;

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
        double tranfer_num = 0,comment_num=0,praise_num=0;
        while (values.hasNext()) {
            Record val = values.next();
            tranfer_num +=  (double)val.getDouble(0);
            comment_num +=  (double)val.getDouble(1);
            praise_num += (double)val.getDouble(2);
        }
        result.set(0, tranfer_num);
        result.set(1, comment_num);
        result.set(2, praise_num);
        context.write(key, result);
    }

    public void cleanup(TaskContext arg0) throws IOException {

    }
}

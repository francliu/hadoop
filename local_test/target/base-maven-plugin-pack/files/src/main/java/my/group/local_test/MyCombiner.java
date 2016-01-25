package my.group.local_test;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Combiner模板。请用真实逻辑替换模板内容
 */
public class MyCombiner implements Reducer {
    private Record value;

    public void setup(TaskContext context) throws IOException {
        value = context.createMapOutputValueRecord();
    }

    public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {

    	//long c = 0;
        while (values.hasNext()) {
            value = values.next();
        }
        context.write(key, value);
        
    }

    public void cleanup(TaskContext arg0) throws IOException {

    }
}

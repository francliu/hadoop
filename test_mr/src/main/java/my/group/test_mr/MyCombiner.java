package my.group.test_mr;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Combiner模板。请用真实逻辑替换模板内容
 */
public class MyCombiner implements Reducer {
    private Record sum;

    public void setup(TaskContext context) throws IOException {
        sum = context.createMapOutputValueRecord();
    }

    public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
        double c = 0,cnt=0;
        while (values.hasNext()) {
            Record val = values.next();
            c += val.getDouble(0);
            cnt+= val.getDouble(1);
        }
        sum.set(0, c);
        sum.set(1,cnt);
        context.write(key, sum);
    }

    public void cleanup(TaskContext arg0) throws IOException {

    }
}
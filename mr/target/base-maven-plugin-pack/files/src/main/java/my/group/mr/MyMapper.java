package my.group.mr;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Mapper;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 * Mapper模板。请用真实逻辑替换模板内容
 */
public class MyMapper implements Mapper {
    private Record mapkey;
    private Record mapvalue;

    public void setup(TaskContext context) throws IOException {
    	mapkey = context.createMapOutputKeyRecord();
        mapvalue = context.createMapOutputValueRecord();
       // one.setBigint(0, 1L);
    }

    public void map(long recordNum, Record record, TaskContext context) throws IOException {
        String uid = record.get(0).toString();
        String mid = record.get(1).toString();
        mapkey.set(0, uid);
        mapkey.set(1, mid);
        // uid,mid,blog_time,blog,fans_id
        String blogtime = record.get(2).toString();
        mapkey.set(2, blogtime);
        //mapvalue.set(1, weightsecond)
        String blog = record.get(3).toString();;
        double blog_len = blog.length();
        mapvalue.set(0, blog);
        if(blog_len>280)
        {
        	blog_len = 3;
        }
        else if(blog_len>140)
        {
        	blog_len = 2;
        }
        else
        {
        	blog_len = 1;
        }
        mapvalue.set(1, blog_len);
        context.write(mapkey, mapvalue);
    }

    public void cleanup(TaskContext context) throws IOException {

    }
}
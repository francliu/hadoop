package my.group.mr_user_feather;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Mapper;

import java.io.IOException;

/**
 * Mapper模板。请用真实逻辑替换模板内容
 */
public class MyMapper implements Mapper {
    private Record mapkey;
    private Record mapvalue;

    public void setup(TaskContext context) throws IOException {
        mapkey = context.createMapOutputKeyRecord();
        mapvalue = context.createMapOutputValueRecord();
    }
    //transfer_num:double,max_transfer_num:double,min_transfer_num:double,
    //comment_num:double,max_comment_num:double,min_comment_num:double,
    //praise_num:double,max_praise_num:double,min_praise_num:double</
    public void map(long recordNum, Record record, TaskContext context) throws IOException {
        String uid = record.getString(0);
        String mid = record.getString(1);
        mapkey.set(0,uid);
        mapkey.set(1,mid);
        //double max_transfer_num = 0,min_transfer_num=0;
       // double max_comment_num = 0,min_comment_num=0;
        //double max_praise_num = 0,min_praise_num=0;
        double transfer_num = record.get(3).toString().matches("1")?1:0;
        double comment_num = record.get(4).toString().matches("1")?1:0;
        double praise_num = record.get(5).toString().matches("1")?1:0;
        mapvalue.set(0,transfer_num);
       // mapvalue.set(1,max_transfer_num);
       // mapvalue.set(2,min_transfer_num);
        mapvalue.set(1, comment_num);
       // mapvalue.set(4,max_comment_num);
        //mapvalue.set(5,min_comment_num);
        mapvalue.set(2,praise_num);
        //mapvalue.set(7,max_praise_num);
        //mapvalue.set(8,min_praise_num); 
        context.write(mapkey, mapvalue);
    }

    public void cleanup(TaskContext context) throws IOException {

    }
}
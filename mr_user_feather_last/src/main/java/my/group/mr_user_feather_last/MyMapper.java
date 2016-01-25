package my.group.mr_user_feather_last;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Mapper;
import com.aliyun.odps.mapred.Mapper.TaskContext;

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
    public static double classify(double num){
    	double value = 1;
    	if(num<5)value = 1;
    	else if(num<=10)value = 2;
    	else if(num<=50)value = 3;
    	else if(num<=100)value = 4;
    	else value = 5;
		return value;
    }
    //transfer_num:double,max_transfer_num:double,min_transfer_num:double,
    //comment_num:double,max_comment_num:double,min_comment_num:double,
    //praise_num:double,max_praise_num:double,min_praise_num:double</
    public void map(long recordNum, Record record, TaskContext context) throws IOException {
        String uid = record.getString(0);
       // String mid = record.getString(1);
        mapkey.set(0,uid);
        double max_transfer_num = 0,min_transfer_num=0;
        double max_comment_num = 0,min_comment_num=0;
        double max_praise_num = 0,min_praise_num=0;
        double means_transfer_num = (Double) record.get(2);
        double means_comment_num =  (Double) record.get(3);
        double means_praise_num =  (Double) record.get(4);
        double max_interactive_level =0,min_interactive_level=0,mean_interactive_level=0;
        max_interactive_level = min_interactive_level = mean_interactive_level = classify(means_transfer_num+means_praise_num+max_interactive_level);
        max_transfer_num = min_transfer_num = classify(means_transfer_num);
        max_comment_num = min_comment_num = classify(means_comment_num);
        max_praise_num = min_praise_num = classify(means_praise_num);
        //mapvalue.set(0,mid);
        mapvalue.set(0,means_transfer_num);
        mapvalue.set(1,means_comment_num);
        mapvalue.set(2,means_praise_num);
        mapvalue.set(3,max_transfer_num);
        mapvalue.set(4,min_transfer_num);
        mapvalue.set(5,means_transfer_num);
        mapvalue.set(6,max_comment_num);
        mapvalue.set(7,min_comment_num);
        mapvalue.set(8,means_comment_num);
        mapvalue.set(9,max_praise_num);
        mapvalue.set(10,min_praise_num); 
        mapvalue.set(11,means_praise_num);
        mapvalue.set(12,max_interactive_level);
        mapvalue.set(13,min_interactive_level);
        mapvalue.set(14,mean_interactive_level);
        context.write(mapkey, mapvalue);
    }


    public void cleanup(TaskContext context) throws IOException {

    }
}
package my.group.mr_user;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

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

    public void map(long recordNum, Record record, TaskContext context) throws IOException {
        String action_id = record.get(0).toString();
       //String action_time = record.get(1).toString();
		String uid = record.get(2).toString();
		String mid = record.get(3).toString();
		String blog_time = record.get(4).toString();
		String action_type = record.get(5).toString();
		double praise = 0,tranfser=0,comment=0;
		if(action_type.matches("3"))praise = 1;
		if(action_type.matches("1"))tranfser = 1;
		if(action_type.matches("2"))comment = 1;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		//Date date = null;
		double spilt_time = 0;
		double timeStemp = 0;
		try {
			timeStemp = sdf .parse(blog_time).getTime();
			spilt_time = sdf.parse("2015-01-01 00:00:00").getTime();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
		}
		//if(timeStemp>=spilt_time)
		{
			mapkey.set(0,uid);
			mapkey.set(1,mid);
			//mapkey.set(2,action_id);
			mapvalue.set(0,tranfser);
			mapvalue.set(1,comment);
			mapvalue.set(2,praise);
			context.write(mapkey,mapvalue);
		}
    }
    public void cleanup(TaskContext context) throws IOException {

    }
}
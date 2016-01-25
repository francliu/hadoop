package my.group.mr_Predict_result;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Matcher;
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
    }
    public static double compute(Double value,Double pro){
		double sum = 0;
    	if(value==1)sum+=pro;
    	else if(value==2)sum+=10*pro;
    	else if(value==3)sum+=50*pro;
    	else if(value==4)sum+=100*pro;
    	else if(value==5)sum+=200*pro;
    	return sum;
    }
    public static double classify(long value){
    	double sum = 0;
    	if(value<=5)sum=1;
    	else if(value<=10)sum=2;
    	else if(value<=50)sum=3;
    	else if(value<=100)sum=4;
    	else sum=5;
    	return sum;
    }
    public void map(long recordNum, Record record, TaskContext context) throws IOException {
        String uid = record.getString(0);
        String mid= record.getString(1);
        String transfer_detail = record.get(2).toString();
        String comment_detail = record.get(3).toString();
        String praise_detail=record.get(4).toString();
        mapkey.set(0,uid);
        mapkey.set(1,mid);
        //String transfer_detail="{\"1\":0.8,\"2\":0.2}";
        long tran_sum=0,comm_sum=0,pra_sum=0;
        Pattern p = Pattern.compile("[0-5]+\":\\s[0-5]+[\\.]{0,1}[0-9]{0,}");
        Matcher m = p.matcher(transfer_detail);
        while (m.find()) {
            //strs.add(m.group(1));    
        	String[] temp = m.group(0).split("\"|:");
            tran_sum+=compute(Double.parseDouble(temp[0]),Double.parseDouble(temp[2]));
        } 
        Matcher comment = p.matcher(comment_detail);
        while (comment.find()) {
            //strs.add(m.group(1));    
        	String[] temp = comment.group(0).split("\"|:");
        	comm_sum+=compute(Double.parseDouble(temp[0]),Double.parseDouble(temp[2]));
        } 
        Matcher praise = p.matcher(praise_detail);
        while (praise.find()) {
            //strs.add(m.group(1));    
        	String[] temp = praise.group(0).split("\"|:");
        	pra_sum+=compute(Double.parseDouble(temp[0]),Double.parseDouble(temp[2]));
        } 
        double result = classify(tran_sum+comm_sum+pra_sum);
        mapvalue.set(0,result);
        context.write(mapkey, mapvalue);
    }

    public void cleanup(TaskContext context) throws IOException {

    }
}
package split_test;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class test {
	 public static double compute(Double value,Double pro){
			double sum = 0;
	    	if(value==1)sum+=1*pro;
	    	else if(value==2)sum+=30*pro;
	    	else if(value==3)sum+=100*pro;
	    	else if(value==4)sum+=100*pro;
	    	else if(value==5)sum+=200*pro;
	    	return sum;
	    }
	 public static double classify(double value){
	    	double sum = 0;
	    	if(value<=5)sum=1;
	    	else if(value<=10)sum=2;
	    	else if(value<=50)sum=3;
	    	else if(value<=100)sum=4;
	    	else sum=5;
	    	return sum;
	    }
	public static void main(String[] args) throws Exception {
		String transfer_detail="{ \"1\": 0.2222222,\"2\": 0.002}";
        Pattern p = Pattern.compile("[0-5]+\":\\s[0-5]+[\\.]{0,1}[0-9]{0,}");
        Matcher m = p.matcher(transfer_detail);
        ArrayList<String> strs = new ArrayList<String>();
        double tran_sum=0;
        while (m.find()) {
            //strs.add(m.group(1));    
        	String[] temp = m.group(0).split("\":\\s");
        	tran_sum +=compute(Double.parseDouble(temp[0]),Double.parseDouble(temp[1]));
        	//System.out.println(m.group(0));
            System.out.println(temp[0]);
            System.out.println(temp[1]);
           // System.out.println(classify(tran_sum));
        }
        Pattern patternfrist = Pattern.compile("转发");
        double tranfer=0 ;
        Matcher p1 = patternfrist.matcher("转发");
         if(p1.find())
         {
        	 tranfer=1; 
         }
        System.out.println(tranfer);
       // System.out.println("1");
		
		
	}
}

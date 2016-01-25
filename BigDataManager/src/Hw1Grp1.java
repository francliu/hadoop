
/* 1,201428013229111,刘建飞*/
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;



public class Hw1Grp1 {

    //声明静态配置 HBaseConfiguration
    static Configuration configuration=HBaseConfiguration.create();
    //创建一张表，通过HBaseAdmin HTableDescriptor来创建
    public static void creat(String tablename,String columnFamily) throws Exception {
    	
		HBaseAdmin admin = new HBaseAdmin(configuration);
        if (admin.tableExists(tablename)) {
            try
            {
                    admin.disableTable(tablename);
                    admin.deleteTable(tablename);
            }catch(Exception ex){
                    ex.printStackTrace();
            }  
        }
        HTableDescriptor tableDesc = new HTableDescriptor(tablename.valueOf(tablename));
        tableDesc.addFamily(new HColumnDescriptor(columnFamily));
        admin.createTable(tableDesc);
        admin.close();
        System.out.println("create table success!");
    }
    //添加一条数据，通过HTable Put为已经存在的表来添加数据
    public static void put(String tablename,String row, String columnFamily,String column,String data) throws Exception {
    	
		HTable table = new HTable(configuration, tablename);
        Put p1=new Put(Bytes.toBytes(row));
        p1.add(columnFamily.getBytes(), column.getBytes(),data.getBytes());
        table.put(p1);
        table.close();
        System.out.println("put successfully");
        //System.out.println("put '"+row+"','"+columnFamily+":"+column+"','"+data+"'");
    }
    //读HDFS文件
	public static String[][] read(String file) throws Exception {
    	Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(file), conf);
		Path path = new Path(file);
		FSDataInputStream in_stream = fs.open(path);
		BufferedReader in = new BufferedReader(new InputStreamReader(in_stream));
		String s;
		ArrayList<String[]> str =  new ArrayList<String[]>();
		int i=0;
		while ((s=in.readLine())!=null)
		{
			String[] tmp =  s.split("[|]");
			str.add(tmp);
		}
		String[][]  t = new String[str.size()][str.get(0).length];
		for(i=0;i<str.size();i++)
		{
			t[i]=str.get(i);
		}
		in.close();
		fs.close();
		return t;
    }
    //二维数组排序
	private static void sort(String[][] s,int n) {
		
		for (int j = 0; j < s.length ; j++) {
			for (int i = 0; i < s.length - 1; i++) {
				String[] ss;
				if (s[i][n].compareTo(s[i + 1][n]) > 0){
					ss = s[i];
					s[i] = s[i + 1];
					s[i + 1] = ss;
				}
			}
		}
	}
    //处理数据
    @SuppressWarnings("unchecked")
	public static ArrayList<ArrayList<String> > sort_merge(String[][] str1,String[][] str2,int a,int b,ArrayList<Integer> res_r,ArrayList<Integer> res_s) throws Exception {
    	ArrayList<ArrayList<String> > t =  new ArrayList<ArrayList<String> >();
    	HashMap<String,Integer> map = new HashMap<String,Integer>();
    	for (int i=0;i < str1.length; i++) 
    	{
    		for(int j=0;j < str2.length;j++)
    		{
	    		ArrayList<String> temp =  new ArrayList<String>();
				if(str1[i][a].compareTo(str2[j][b])==0)
				{
					int l=0,m=0;
					temp.add(str1[i][a]);
					String str = str1[i][a];
					while(l<res_r.size())
					{
						if(res_r.get(l)<str1[i].length)
						{
							str+="R"+res_r.get(l);
				    		if(!map.containsKey(str))
				    		{
				    			map.put(str,1);
				    			temp.add("R"+res_r.get(l));
				    		}
				    		else
				    		{
				    			int pos = map.get(str);
				    			temp.add("R"+res_r.get(l)+"."+pos);
				    			map.remove(str);
				    			map.put(str,pos+1);
				    		}
							temp.add(str1[i][res_r.get(l)]);
							t.add((ArrayList<String>)temp.clone());
							temp.clear();
							temp.add(str1[i][a]);
							l++;
						}
					}
					while(m<res_s.size())
					{
						if(res_s.get(m)<str2[j].length)
						{
							str+="S"+res_s.get(m);
				    		if(!map.containsKey(str))
				    		{
				    			map.put(str,1);
				    			temp.add("S"+res_s.get(m));
				    		}
				    		else
				    		{
				    			int pos = map.get(str);
				    			temp.add("S"+res_s.get(m)+"."+pos);
				    			map.remove(str);
				    			map.put(str,pos+1);
				    		}
							temp.add(str2[j][res_s.get(m)]);
							t.add((ArrayList<String>)temp.clone());
							temp.clear();
							temp.add(str2[j][b]);
							m++;
						}
					}
				}
			else if(str1[i][a].compareTo(str2[j][b])<0)break;
    		}
		}
		return t;
    }
    
    //对重复数据的处理
    public static void  repeat_handle(ArrayList<ArrayList<String> > t)
    {
    	ArrayList<ArrayList<Integer> > hash = new ArrayList<ArrayList<Integer> >();
    	HashSet<String> h = new HashSet<String>();
    	HashMap<String,Integer> map = new HashMap<String,Integer>();
    	int p=0;
    	for(int i =0;i<t.size();i++)
    	{
    		String temp = "";
    		for(int j=0;j<2;j++)
    		{
    			temp+=t.get(i).get(j);
    		}
    		if(h.add(temp))
    		{
    			ArrayList<Integer> a = new ArrayList<Integer>(); 
    			a.add(i);
    			hash.add(a);
    			map.put(temp,p++);
    		}
    		else
    		{
    			int pos = map.get(temp);
    			hash.get(pos).add(i);
    		}
    	}
    	for(int i =0;i<hash.size();i++)
    	{
    		for(int j=1;j<hash.get(i).size();j++)
    		{
    			ArrayList<String> m= t.get(hash.get(i).get(j));
    			m.set(1, m.get(1)+'.'+Integer.toString(j));
    		}
    	}
    }
    
	private static String[][] read_file(String[] file) throws Exception {
		for(int i=0;i<file.length;i++)
		{
			if(file[i].length()>1)
			{
				String[][] str=read("hdfs://localhost:9000"+file[i]);
				return str;
			}
		}
		return null;
	}
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		String[] file1 = args[0].split("[<=>\"\']");
		String[] file2 = args[1].split("[<=>\"\']");
		String[] join_R = args[2].split("[:=]");
		String[] join_S = args[3].split("[:,]");
		//获取join列序号
		char r = join_R[1].toCharArray()[1];
		char s = join_R[2].toCharArray()[1];
		Integer r1 =  Integer.parseInt(""+r);
		Integer s1 =  Integer.parseInt(""+s);
		//存放最终columnFamily列表项序号
		ArrayList<Integer> res_r =  new ArrayList<Integer>(); 
		ArrayList<Integer> res_s =  new ArrayList<Integer>(); 
		int i,j=0;
		for(i=1;i<join_S.length;i++)
		{
			if(join_S[i].toCharArray()[0]=='R')
				res_r.add(Integer.parseInt(""+join_S[i].toCharArray()[1]));
			else if(join_S[i].toCharArray()[0]=='S')
				res_s.add(Integer.parseInt(""+join_S[i].toCharArray()[1]));
			else
			{
				System.out.println("输入格式应为java Hw1Grp1 R=\"/user/input/file01\" S=/user/input/file02 join:RX=SX res:RX,SX");
				return;
			}
		}
		//读取hdfs文件记录
		String[][] str1 = read_file(file1);
		String[][] str2 = read_file(file2);
		//对文件里的记录进行排序
	    //Hw1Grp1.sort(str1, r1);
		//Hw1Grp1.sort(str2, s1);
		//对排好序的记录进行合并
		ArrayList<ArrayList<String>> result = Hw1Grp1.sort_merge(str1,str2,r1,s1,res_r,res_s);
		//对重复数据的处理
		//repeat_handle(result);
		//把join后的数据存入hbase中
    	String tablename="Result";
        String columnFamily=join_S[0];                 
    	Hw1Grp1.creat(tablename, columnFamily);
		for(i=0;i<result.size();i++)
		{
			Hw1Grp1.put(tablename,result.get(i).get(0), columnFamily,result.get(i).get(1),result.get(i).get(2));
		}		
	}
}
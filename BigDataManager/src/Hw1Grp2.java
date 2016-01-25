/* 2,201428013229136,赵秀锋*/

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class Hw1Grp2 {
	
	/*
	 * 将字符串进行分割,得到要记录到Hashtable中的value的一条记录	
	 */
	private static List<String> GetOneRecord (String s,String splitchar){
		 String[] SplitArray=s.split(splitchar);
		 List<String>oneRecord=new ArrayList<String>();
		 for(int i=0;i<SplitArray.length;i++)
		 {
			oneRecord.add(SplitArray[i]);
		 }
		 return oneRecord;
	}
	
	/*
	 * 得到value值	
	 */
	private static double [] GetValue (Hashtable hashTable,String s,String splitchar,int groupIndex,List<String []>columns){
		String key=s.split(splitchar)[groupIndex];
		List<String>oneRecord=GetOneRecord(s, splitchar);
		//要返回的值，先初始化
		double [] value =new double[columns.size()+1];	//Hashtable的列数比要求的输入命令多一列，最后一列value[columns.size()]用来存放count计数
		double Count=0.0,Sum=0.0,Avg=0.0,Max=0.0;
		//如果当前key已存在
		if(hashTable.containsKey(key)){
			value=(double []) hashTable.get(key);
			Count=value[columns.size()]+1;	//原来的总数目加1，得到新的总数目	
			for (int i=0;i<columns.size();i++){
				String columnType=columns.get(i)[0];	//类型
				int index=Integer.parseInt(columns.get(i)[1]);//列位置
				double n=Double.parseDouble(oneRecord.get(index));	//当前新得到的数值				
				switch(columnType){
					case "count":
						Count=value[columns.size()]+1;	//原来的总数目加1，得到新的总数目
						value[i]=Count;
						break;
					case "avg":
						Sum=value[columns.size()]*value[i]+n;	//原来的总数目乘以原来的平均值得到原来的总和，加上当前的新数值,得到新的总和
						Avg=Sum/Count;	//新的总和除以新的总数目，得到新的平均数
						value[i]=Avg;
						break;
					case "max":
						//将当前的新数值与原来的最大值进行比较
						Max=n>value[i] ? n:value[i];	//如果当前新数值比原来的最大值大，新的最大值等于新数值，否则等于原来的最大值	
						value[i]=Max;
						break;					
				}
			}
			value[columns.size()]+=1;//最后一个元素value[columns.size()]存放count计数，加1
		}
		//遇到的新key
		else {
			for (int i=0;i<columns.size();i++){
				String columnType=columns.get(i)[0];	//类型
				int index=Integer.parseInt(columns.get(i)[1]);//列位置
				double n=Double.parseDouble(oneRecord.get(index));	//当前新得到的数值				
				switch(columnType){
					case "count":
						Count=1;	//总数目设为1
						value[i]=Count;
						break;
					case "avg":
						Sum=n;	//总和设为当前的数值
						Avg=n;	//平均数设为当前数值
						value[i]=Avg;
						break;
					case "max":
						//将当前的新数值与原来的最大值进行比较
						Max=n;	//最大值设为当前的数值
						value[i]=Max;
						break;					
				}
			}
			value[columns.size()]=1;//最后一个元素value[columns.size()]存放count计数，设为1
		}
		return value;
	}

	
	/*
	 * 从HDFS上读取文件，并put到Hashtable中
	 */
	public static Hashtable ReadHdfsFile(String file, int groupIndex,List<String[]> columns) throws IOException{
		// 新建Hashtable
        Hashtable table = new Hashtable();
        
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(file),conf);
		Path path = new Path(file);
		FSDataInputStream in_stream = fs.open(path);
		BufferedReader in = new BufferedReader(new InputStreamReader(in_stream));
		String s;
		while ((s=in.readLine())!=null) {
//			System.out.println(s);
			//得到Hashtable用的key
			String key=s.split( "\\|")[groupIndex];
			//得到Hashtable用的value
			double [] value=GetValue(table, s, "\\|", groupIndex,columns);
			//将key-value写入到Hashtable中
			table.put(key, value);
		}
		in.close();
		fs.close();
		return table;
	}
	
    /*
     * 创建一个HTable，并返回结果
     */
   private static HTable CreateHtable(String tableName,String columnFamily) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		// create table descriptor
		HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
		// create column descriptor
		HColumnDescriptor cf = new HColumnDescriptor(columnFamily);
		htd.addFamily(cf);
		// configure HBase
		Configuration configuration = HBaseConfiguration.create();
		HBaseAdmin hAdmin = new HBaseAdmin(configuration);
		
		//判断是否存在同名的表
		if (hAdmin.tableExists(tableName)) {
           try
           {
           	hAdmin.disableTable(tableName);
           	hAdmin.deleteTable(tableName);
           }catch(Exception ex){
                   ex.printStackTrace();
           }  
       }
		//创建表
		hAdmin.createTable(htd);
		hAdmin.close();
		HTable hTable = new HTable(configuration,tableName);
		return hTable;
}

	/*
	 * 将hashTable中的数据导入到Hbase中
	 */
	public static void PutToHBase(Hashtable hashTable,String tableName,String columnFamily,List<String[]> columns, String[] columnsStr) throws MasterNotRunningException,ZooKeeperConnectionException, IOException {
		//得到创建的HTable
		HTable hTable =CreateHtable(tableName, columnFamily);
        //批量写入的过程
		List<Put> putList=new ArrayList<Put>();//用于批量写入的put列表
		//遍历hashTable，获得用于写入的数据
        Enumeration enu = hashTable.keys();
        while(enu.hasMoreElements()) {
        	String key=enu.nextElement().toString();
        	double [] value=(double []) hashTable.get(key); 
    		//插入一行记录
        	for (int i=0;i<columns.size();i++){
				Put put = new Put(Bytes.toBytes(key));	//指定row key
	    		put.add(Bytes.toBytes(columnFamily),Bytes.toBytes(columnsStr[i]),Bytes.toBytes(value[i]+""));	//指定column family是res,column是count\avg\max
	    		putList.add(put);//添加到队列中
        	}
        }
		//执行批量写入
		hTable.put(putList);//通过put这个putList列表，实现批量写入
		hTable.close();
		System.out.println("put successfully");
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws IOException, URISyntaxException{
		System.out.println("输入参数格式为：R=/input/orders.tbl groupby:R2 'res:count,avg(R1)'");
		//获取文件路径
		String path="hdfs://localhost:9000";
		String file =path+args[0].split("[=]")[1];
		
		//获取groupby列序号
		String groupbyR=(args[1].split("[:]")[1]);
		int groupIndex = Integer.parseInt(groupbyR.toCharArray()[1]+"");
		//获取column Family
		System.out.println(args[2].split("[:]")[0]);
		String columnFamily=args[2].split("[:]")[0];
		
		//获取columns
		String[] columnsStr=args[2].split("[:]")[1].split("\\'")[0].split(",");//列名的字符串内容
		
		//处理输入的统计类型
		List<String []>columns=new ArrayList<String []>();//用作HashTable的统计处理
		for (int i=0;i<columnsStr.length;i++){
			String columnType=columnsStr[i].split("[(]")[0];	//获取统计类型
			int index=0;
			if(!columnType.equalsIgnoreCase("count")){
				String indexStr=columnsStr[i].split("[()]")[1];	
				index=Integer.parseInt(indexStr.toCharArray()[1]+"");//获取列序号
			}
			String []column={columnType,index+""};
			columns.add(column);
		}
		
		Hashtable hashTable=new Hashtable();	//新建hashTable
		hashTable=ReadHdfsFile(file,groupIndex,columns);//读取文件，统计数据写入到hashTable中
		String tableName="Result";	//指定Hbase表名
		PutToHBase(hashTable,tableName,columnFamily,columns,columnsStr);//将hashTable数据写入到Hbase
	}
}



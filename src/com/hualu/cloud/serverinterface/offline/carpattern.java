package com.hualu.cloud.serverinterface.offline;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.ehl.itgs.interfaces.bean.BackResult;
import com.ehl.itgs.interfaces.bean.ResultCar;
import com.ehl.itgs.interfaces.bean.analysis.CarPatternInfo;
import com.ehl.itgs.interfaces.bean.analysis.CarPatternResult;
import com.ehl.itgs.interfaces.bean.analysis.CarPatternResult.CarPatternRecord;
import com.ehl.itgs.interfaces.bean.QueryInfo;
import com.hualu.cloud.db.memoryDB;

public class carpattern {
	
	static int count = 0;
    static String key ="";
    static int m = 1;
    static String photoURL="http://10.2.111.106/photo";
    static int expireInterval=86400;
    static memoryDB memorydb=new memoryDB();
    static ArrayList<String> resultList = new ArrayList<String>();
    static List<String> newList = new ArrayList<String>();
	
	public int getFromHBase(CarPatternInfo query,QueryInfo queryInfo) throws Exception
	{
		Configuration conf = HBaseConfiguration.create();
        HTable table = new HTable(conf, "car");
        Scan scan = new Scan();
        
        String plateType = query.getPlateType();
        String plateNumber = query.getPlateNumber();
        String begTime = query.getBegTime();
        String endTime = query.getPlateNumber();
        int patternType = query.getPatternType();
        int minOccurrence = query.getMinOccurrence();
        key = getKey(query,queryInfo);
        
        List[][] array1 = new ArrayList[7][];
        for(int i = 0;i <= 6;i++)//初始化二维对象数组
    	{
    		array1[i] = new ArrayList[1];
    		array1[i][0] = new ArrayList();
    	}
        
        List[][] array2 = new ArrayList[31][]; 
        for(int i = 0;i <= 30;i++)//初始化二维对象数组
    	{
    		array2[i] = new ArrayList[1];
    		array2[i][0] = new ArrayList();
    	}
        
        List[][] arraySort1 = new ArrayList[7][]; 
       for(int i = 0;i <= 6;i++)//初始化二维对象数组
    	{
    		arraySort1[i] = new ArrayList[1];
    		arraySort1[i][0] = new ArrayList();
    	}
        
        List[][] arraySort2 = new ArrayList[31][];
       for(int i = 0;i <= 30;i++)//初始化二维对象数组
    	{
    		arraySort2[i] = new ArrayList[1];
    		arraySort2[i][0] = new ArrayList<String>();
    	}
        scan.setStartRow(Bytes.toBytes(plateType + "," + plateNumber + "," + begTime));
        scan.setStopRow(Bytes.toBytes(plateType + "," + plateNumber + "," + endTime));
        ResultScanner r = table.getScanner(scan);
        for(Result rr=r.next();rr!=null;rr=r.next())
        {   
        	int w;
        	int n;
        	count++;
        	String result = new String(rr.getRow())
        	+ "," + new String(rr.getValue(Bytes.toBytes("carfamily"),Bytes.toBytes("CARSTATE")))
        	+ "," + new String(rr.getValue(Bytes.toBytes("carfamily"),Bytes.toBytes("SPEED")))
            + "," + new String(rr.getValue(Bytes.toBytes("carfamily"),Bytes.toBytes("PLATECOLOR")))
            + "," + new String(rr.getValue(Bytes.toBytes("carfamily"),Bytes.toBytes("LOCATIONID")))
            + "," + new String(rr.getValue(Bytes.toBytes("carfamily"),Bytes.toBytes("DRIVEWAY")))
            + "," + new String(rr.getValue(Bytes.toBytes("carfamily"),Bytes.toBytes("DRIVEDIR")))
            + "," + new String(rr.getValue(Bytes.toBytes("carfamily"),Bytes.toBytes("CAPTUREDIR")))
            + "," + new String(rr.getValue(Bytes.toBytes("carfamily"),Bytes.toBytes("CARCOLOR")))
            + "," + new String(rr.getValue(Bytes.toBytes("carfamily"),Bytes.toBytes("CARBRAND")));
        	
        	//System.out.println(result);
            resultList.add(result);
        	
        	w = base.week(result);
        	array1[w - 1][0].add(result);
        	
        	
            n = base.month(result);
            array2[n - 1][0].add(result);
        	
        }
        newList = base.sort1(resultList);
        
        for(int i = 0;i <= 6;i++)
        {
        	arraySort1[i][0] = (base.sort1(array1[i][0]));
        }
        
        for(int i = 0;i <= 30;i++)
        {
        	arraySort2[i][0] = (base.sort1(array2[i][0]));
        }
    
        
        if(patternType == -1)
        {
        	Iterator iterator = newList.iterator();
        	while(iterator.hasNext())
        	{
        		String element = (String) iterator.next();
        		String s = memorydb.add(key + base.getNumber(m),element);
        		m++;
        		System.out.println(element);
        	}
        	String s1 = memorydb.add(key,count + "");
        }
        
        if(patternType == 1)
        {
        	for(int i = 0;i <= 6;i++)
        	{
        		Iterator iterator= arraySort1[i][0].iterator();
            	while(iterator.hasNext())
            	{
            		String element = (String) iterator.next();
            		String s = memorydb.add(key + base.getNumber(m) + i,element);
            		m++;
            		System.out.println(element);
            	}
        	}
        	String s1 = memorydb.add(key,count + "");
        }
        
        if(patternType == 2)
        {
        	for(int i = 0;i <= 30;i++)
        	{
        		Iterator iterator= arraySort2[i][0].iterator();
            	while(iterator.hasNext())
            	{
            		String element = (String) iterator.next();
            		String s = memorydb.add(key + base.getNumber(m) + i,element);
            		System.out.println(element);
            		
            		m++;
            	}
        	}
        	String s1 = memorydb.add(key,count + "");
        	System.out.println(memorydb.get(key));
        }
        
        m = 1;
        table.close();
        return count;
	}
	
	public BackResult<CarPatternResult> getFromMemoryDB(CarPatternInfo query,int begNum,int num,QueryInfo queryInfo) throws Exception
    {
		String plateType = query.getPlateType();
        String plateNumber = query.getPlateNumber();
        String begTime = query.getBegTime();
        String endTime = query.getPlateNumber();
        int patternType = query.getPatternType();
        int minOccurrence = query.getMinOccurrence();
        
        
        BackResult br = new BackResult();
        ResultCar rc = new ResultCar();
        CarPatternResult cpr = new CarPatternResult();
        CarPatternResult.CarPatternRecord cpr1 = new CarPatternResult.CarPatternRecord();
        ArrayList<CarPatternResult> brList = new ArrayList<CarPatternResult>();
        
        CarPatternRecord[][] records = new CarPatternRecord[1][];
        records[0] =new CarPatternRecord[1];
        records[0][0] = new CarPatternRecord();
        
        CarPatternRecord[][] records_w = new CarPatternRecord[7][];
        for(int i = 0;i <= 6;i++)
        {
        	records_w[i] =new CarPatternRecord[1];
            records_w[i][0] = new CarPatternRecord();
        }
        
        CarPatternRecord[][] records_m = new CarPatternRecord[31][];
        for(int i = 0;i <= 30;i++)
        {
        	records_m[i] = new CarPatternRecord[1];
            records_m[i][0] = new CarPatternRecord();
        }
        
        int k = 0;
        key = getKey(query,queryInfo);
        
        String s = memorydb.get(key);
        System.out.println(s);
        if (s == null)
        {
           
            getFromHBase(query,queryInfo);
            s = memorydb.get(key);
            m = base.getCount(Integer.parseInt(s),begNum,num);
        }
        else
        {
            m = base.getCount(Integer.parseInt(s),begNum,num);
            if(m==0)
            {
                System.out.println(key+":"+s+":"+"没有更多的数据！");
                return null;
            }
        }
        
		if(minOccurrence >= Integer.parseInt(s))
		{
			System.out.println("查询出的数据数量少于要求！");
			return null;
		}
		else
		{
			if(patternType == -1)
			{
				for(int i = begNum;i < begNum + m;i++)
                {
					System.out.println(memorydb.get(key + base.getNumber(i)));
                	String split[] = memorydb.get(key + base.getNumber(i)).split(",");
                	rc.setPlateType(split[0]);
                	rc.setPlateNumber(split[1]);
                	rc.setTimeStamp(split[2]);
                	rc.setTgs(split[3]);
                	rc.setCarState(split[4]);
                	rc.setSpeed(split[5]);
                    rc.setPlateColor(split[6]);
                    rc.setLoncatonId(split[7]);
                    rc.setLandIe(split[8]);
                    rc.setTravelOrientation(Integer.parseInt(split[9]));
                    rc.setCaptureDir(split[10]);
                    rc.setCarColor(split[11]);
                    rc.setCarBrand(split[12]);
                    String[] url={""};
                    url[0]=photoURL+split[0]+","+split[1]+","+split[2]+","+split[3];
                    rc.setImgUrls(url);
                    ResultCar[] rc1 = {rc};
                    cpr1.setCars(rc1);
                    cpr1.setTgsId(split[3]);
                    records[0][0] = cpr1;
                	cpr.setPlateType(split[0]);
                	cpr.setPlateNumber(split[1]);
                	cpr.setRecords(records);
                	brList.add(cpr);
                	k++;
                	System.out.println("k="+k+":"+System.currentTimeMillis());
                }
				br.setBeans(brList);
			}
			
			if(patternType == 1)
			{
				
				for(int j = 0;j < 7;j++)
				{
					for(int i = begNum;i < begNum + m;i++)
					{
					  if(memorydb.get(key + base.getNumber(i) + j) != null)
					  {
						
						System.out.println(memorydb.get(key + base.getNumber(i) + j));
						String split[] = memorydb.get(key + base.getNumber(i) + j).split(",");
						rc.setPlateType(split[0]);
	                	rc.setPlateNumber(split[1]);
	                	rc.setTimeStamp(split[2]);
	                	rc.setTgs(split[3]);
	                	rc.setCarState(split[4]);
	                	rc.setSpeed(split[5]);
	                    rc.setPlateColor(split[6]);
	                    rc.setLoncatonId(split[7]);
	                    rc.setLandIe(split[8]);
	                    rc.setTravelOrientation(Integer.parseInt(split[9]));
	                    rc.setCaptureDir(split[10]);
	                    rc.setCarColor(split[11]);
	                    rc.setCarBrand(split[12]);
	                    String[] url={""};
	                    url[0]=photoURL+split[0]+","+split[1]+","+split[2]+","+split[3];
	                    rc.setImgUrls(url);
	                    ResultCar[] rc1 = {rc};
	                    cpr1.setCars(rc1);
	                    cpr1.setTgsId(split[3]);
	                    records_w[j][k] = cpr1;
	                	cpr.setPlateType(split[0]);
	                	cpr.setPlateNumber(split[1]);
	                	cpr.setRecords(records);
	                	brList.add(cpr);

					  }
					  
					}
				}
				br.setBeans(brList);
			}
			
			if(patternType == 2)
			{
				for(int j = 0;j <= 31;j++)
				{
					for(int i = begNum;i < begNum + m;i++)
					{
					  if(memorydb.get(key + base.getNumber(i) + j) != null)
					  {
						
						System.out.println(memorydb.get(key + base.getNumber(i) + j));
						String split[] = memorydb.get(key + base.getNumber(i) + j).split(",");
						rc.setPlateType(split[0]);
	                	rc.setPlateNumber(split[1]);
	                	rc.setTimeStamp(split[2]);
	                	rc.setTgs(split[3]);
	                	rc.setCarState(split[4]);
	                	rc.setSpeed(split[5]);
	                    rc.setPlateColor(split[6]);
	                    rc.setLoncatonId(split[7]);
	                    rc.setLandIe(split[8]);
	                    rc.setTravelOrientation(Integer.parseInt(split[9]));
	                    rc.setCaptureDir(split[10]);
	                    rc.setCarColor(split[11]);
	                    rc.setCarBrand(split[12]);
	                    String[] url={""};
	                    url[0]=photoURL+split[0]+","+split[1]+","+split[2]+","+split[3];
	                    rc.setImgUrls(url);
	                    ResultCar[] rc1 = {rc};
	                    cpr1.setCars(rc1);
	                    cpr1.setTgsId(split[3]);
	                    records_m[j][k] = cpr1;
	                	cpr.setPlateType(split[0]);
	                	cpr.setPlateNumber(split[1]);
	                	cpr.setRecords(records);
	                	brList.add(cpr);
					  }
					}
				}
				br.setBeans(brList);
			}
			
		}
		System.out.println("setbean:"+System.currentTimeMillis());
		return br;
    }
	

	
	public static String getKey(CarPatternInfo query,QueryInfo queryInfo)
	{
		String plateType = query.getPlateType();
        String plateNumber = query.getPlateNumber();
        String begTime = query.getBegTime();
        String endTime = query.getPlateNumber();
        String patterntype=String.valueOf(query.getPatternType());
        String result = plateType + plateNumber + begTime + endTime+patterntype;
        return result;
	}
}

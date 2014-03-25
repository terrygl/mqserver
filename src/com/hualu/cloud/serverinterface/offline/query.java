package com.hualu.cloud.serverinterface.offline;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;

import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.ehl.itgs.interfaces.bean.Layout;
import com.ehl.itgs.interfaces.bean.QueryCar;
import com.ehl.itgs.interfaces.bean.QueryInfo;
import com.ehl.itgs.interfaces.bean.ResultCar;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.hualu.cloud.basebase.staticparameter;
import com.hualu.cloud.db.memoryDB;
import com.hualu.cloud.serverinterface.mymanager;


public class query {
	
    static memoryDB memorydb = new memoryDB();   
    static long m = 1;
    String queueName0="";
    int begNum=0;
    int num=0;

    /**
     * 构造函数：初始化一些信息包括图片访问地址等
     */
    public query(){
//       	base.printMsg("photoURL=",staticparameter.photoURL);
//    	base.printMsg("expireInterval=",Integer.toString(staticparameter.expireInterval));
    }

    /**
     * 用HBase的filter查询满足条件的记录生成bean，转换为json，并插入到内存数据库
     * @param car：要查询的车辆信息
     * @param tgs：要查询的卡口列表信息
     * @param begTime：查询开始时间
     * @param endTime：查询结束时间
     * @param queryInfo：查询信息
     * @param command：查询命令的参数
     * @return：返回查询的总条数
     * @throws Exception
     */
    public long getFromHBase(QueryCar car, java.lang.String[] tgs, java.lang.String begTime,
            java.lang.String endTime, QueryInfo queryInfo, String command)throws Exception{
    	int endNum=begNum+num;
    	Gson gson=new Gson(); 	
        String familyname = "flowfamily"; 
        String key = command;//command=commandname+parameters
        String columnname = "";
        String keynum = "";
    	Configuration conf = HBaseConfiguration.create();
        HTable table = new HTable(conf, "by_flow");
       
        Scan scan = new Scan();
        scan.setCaching(1000);        
        
        int i0=tgs.length;
        for(int i=0; i<i0; i++)
        {
        	//按照卡口编号和起始结束时间做rowkey的start和end
        	String s1 = tgs[i];
            String begin = s1 + "," + begTime;
            String end = s1 + "," + endTime;
            System.out.println("query.java getFromHbase(): startkey:"+begin+"  endkey:"+end);
            scan.setStartRow(Bytes.toBytes(begin));
            scan.setStopRow(Bytes.toBytes(end));
            
            
            //设置多个单列filter来过滤数据
            FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);

            //车辆品牌
            {
	           	columnname="CARBRAND";
	           	String[] value=car.getCarBrand();
	           	if(value==null)
	           		System.out.println("CARBRAND is null");
	           	else
	           		filterList.addFilter(getFilterListSingleColumnIn(familyname,columnname,value));
            }
            
            //车辆颜色
            {
	           	columnname="CARCOLOR";
	           	String[] value=car.getCarColor();
	           	if(value==null)
	           		System.out.println("CARCOLOR is null");
	           	else
	           		filterList.addFilter(getFilterListSingleColumnIn(familyname,columnname,value));
            }
            
            //车牌颜色
            {
            	columnname="PLATECOLOR";
               	String[] value=car.getPlateColor(); 
               	if(value==null)
               		System.out.println("PLATECOLOR is null");
               	else    			
               		filterList.addFilter(getFilterListSingleColumnIn(familyname,columnname,value));
            }
            
            
            //车牌号码，支持模糊查询（正则表达式）
            {
               	String value=car.getPlateNumber();
               	columnname="CARNUMBER";
               	if(value==null);
               	else{
               		value=value.replace("%", ".");
               		value=value.replace("*", ".*");
               		RegexStringComparator comp = new RegexStringComparator(value);   
               		filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes(familyname),Bytes.toBytes(columnname),CompareOp.EQUAL,comp));
               	}
            }
            
            //车牌类型
            {
            	columnname="PLATETYPE";
               	String[] value=car.getPlateType(); 
               	if(value==null)
               		System.out.println("PLATETYPE is null");
               	else    			
               		filterList.addFilter(getFilterListSingleColumnIn(familyname,columnname,value));
            }
            
            //车道编号
            {
            	columnname="DRIVEWAY";
               	String[] value=car.getLandIe(); 
               	if(value==null)
               		System.out.println("LandIe is null");
               	else    			
               		filterList.addFilter(getFilterListSingleColumnIn(familyname,columnname,value));
            }
            
            //速度
            {
	           	columnname="SPEED";
	           	String[] value=car.getSpeed();
	           	if(value==null)
	           		System.out.println("SPEED is null");
	           	else{  
	           		filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes(familyname),Bytes.toBytes(columnname),CompareOp.GREATER_OR_EQUAL,new BinaryComparator(Bytes.toBytes(value[0]))));
	           		filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes(familyname),Bytes.toBytes(columnname),CompareOp.LESS_OR_EQUAL,new BinaryComparator(Bytes.toBytes(value[1]))));
	           	}
            }
            
            //行驶方向
            {
            	columnname="DRIVEDIR";
	           	String[] value=car.getTravelOrientation();
	           	if(value==null)
	           		System.out.println("DRIVEDIR is null");
	           	else
	           		filterList.addFilter(getFilterListSingleColumnIn(familyname,columnname,value));
            }
            
            //车辆状态
            {
            	columnname="CARSTATE";
	           	String[] value=car.getCarState();
	           	if(value==null)
	           		System.out.println("CARSTATE is null");
	           	else
	           		filterList.addFilter(getFilterListSingleColumnIn(familyname,columnname,value));
            }
            
            //区域
            {
	           	columnname="LOCATIONID";
	           	String[] value=car.getLoncatonId();
	           	if(value==null)
	           		System.out.println("LOCATIONID is null");
	           	else
	           		filterList.addFilter(getFilterListSingleColumnIn(familyname,columnname,value));
            }
            
            //捕获方向
            {
	           	columnname="CAPTUREDIR";
	           	String[] value=car.getCaptureDir();
	           	if(value==null)
	           		System.out.println("CAPTUREDIR is null");
	           	else
	           		filterList.addFilter(getFilterListSingleColumnIn(familyname,columnname,value));
            }

            scan.setFilter(filterList);      
	        ResultScanner r = table.getScanner(scan);	    	        
	        //将结果信息插入到内存数据库
	        ResultCar rc=new ResultCar();
	        for(Result rr:r)
	        {
	            if(rr==null)
	            	break;
	           
	            //从rowkey中获取信息
	            String rowkey=new String(rr.getRow());
	            String[] url={""};
	            url[0]=staticparameter.getValue("photoURL","http://172.16.100.51:8888/n/0/0/")+rowkey;
	            String[] s=rowkey.split(",");
	            rc.setTgs(s[0]);
	            rc.setTimeStamp(s[1]);
	            rc.setPlateType(s[2]);
	            rc.setPlateNumber(s[3]);
	            
	            //从column中获取信息
            	if(rr.getValue(Bytes.toBytes(familyname),Bytes.toBytes("CARBRAND")) != null)
            		rc.setCarBrand(new String(rr.getValue(Bytes.toBytes(familyname),Bytes.toBytes("CARBRAND"))));
            	if(rr.getValue(Bytes.toBytes(familyname),Bytes.toBytes("CARCOLOR")) != null)
            		rc.setCarColor(new String(rr.getValue(Bytes.toBytes(familyname),Bytes.toBytes("CARCOLOR"))));
            	if(rr.getValue(Bytes.toBytes(familyname),Bytes.toBytes("PLATECOLOR")) != null)
            		rc.setPlateColor(new String(rr.getValue(Bytes.toBytes(familyname),Bytes.toBytes("PLATECOLOR"))));
	            if(rr.getValue(Bytes.toBytes(familyname),Bytes.toBytes("DRIVEWAY")) != null)
	            	rc.setLandIe(new String(rr.getValue(Bytes.toBytes(familyname),Bytes.toBytes("DRIVEWAY"))));
            	if(rr.getValue(Bytes.toBytes(familyname),Bytes.toBytes("SPEED")) != null)
            		rc.setSpeed(new String(rr.getValue(Bytes.toBytes(familyname),Bytes.toBytes("SPEED"))));
            	if(rr.getValue(Bytes.toBytes(familyname),Bytes.toBytes("DRIVEDIR")) != null)
            		rc.setTravelOrientation(Integer.parseInt(new String(rr.getValue(Bytes.toBytes(familyname),Bytes.toBytes("DRIVEDIR")))));
            	if(rr.getValue(Bytes.toBytes(familyname),Bytes.toBytes("CARSTATE")) != null)
            		rc.setCarState(new String(rr.getValue(Bytes.toBytes(familyname),Bytes.toBytes("CARSTATE"))));
            	if(rr.getValue(Bytes.toBytes(familyname),Bytes.toBytes("LOCATIONID")) != null)
            		rc.setLoncatonId(new String(rr.getValue(Bytes.toBytes(familyname),Bytes.toBytes("LOCATIONID"))));
            	if(rr.getValue(Bytes.toBytes(familyname),Bytes.toBytes("CAPTUREDIR")) != null)
            		rc.setCaptureDir(new String(rr.getValue(Bytes.toBytes(familyname),Bytes.toBytes("CAPTUREDIR"))));
	            rc.setImgUrls(url);
	        
	            //组织插入redis的key和value
	            String element = "";
	            if(rc != null)
	            	element=gson.toJson(rc);//将每条resultcar的json值插入redis
	            System.out.println("query.java:将结果插入内存数据库:"+element);
//	            keynum= key + base.getNumber(m);
	            keynum= key + base.getLongNumber(m);
		        memorydb.add(keynum,element);
		        memorydb.expire(keynum,mymanager.expireInterval);
		        System.out.println("查看是否已经插入内存数据库 key="+keynum+"  value=:"+memorydb.get(keynum));
		        m++;	
		        
		        //如果m=betNum+num则，执行直接在此返会消息
		        if(m==endNum){
			    	//将从内存数据库取数据并发送到消息队列的功能交给SengResultMSG线程去处理
			        System.out.println("**********query.java getFromMemoryDB:begTime="+begTime+";endTime="+endTime+";begNum="+begNum+";num="+num+";msgQueue="+queueName0+";key="+key);        
			        SendLongResultMSG1 ex = new SendLongResultMSG1(begNum,num,key,queueName0);   
			        Thread pThread = new Thread(ex);   
			        pThread.start();   
		        }
		        
	        }
	        r.close();
        }
	    //往redis插入处理总数
        System.out.println("查看是否插入内存数据库总记录数"+command+":"+key);
	    memorydb.add(key,m-1+"");
		memorydb.expire(key, mymanager.expireInterval);
		System.out.println("查看是否插入内存数据库总记录数"+key+":"+memorydb.get(key));
		
		table.close();
        return m-1;
    }
  
    /**
     * 根据输入的QueryCar和开始结束时间以及卡口列表信息组织key，并根据Key判断是否已经有处理结果。如果没有，则调用getFromHBase进行处理。并从内存数据库中取出num和begNum指定的记录并转换为BackResult结果
     * @param car：查询的车辆信息
     * @param tags：查询的卡口列表
     * @param begTime：查询开始时间
     * @param endTime：查询结束时间
     * @param begNum：查询要求从第begNum条记录开始返回
     * @param num：共返回num条
     * @param queryInfo：查询信息
     * @param command：查询命令的参数信息
     * @return：返回BackResult
     * @throws Exception
     */
   	public void getFromMemoryDB(String queueName,String command,String parameter)throws Exception{
    	
    	//解析参数
   	    Gson gson=new Gson();
   	    
   		String[] para=parameter.split(mymanager.parametersplit);
   		QueryCar car=new QueryCar();
   		car=gson.fromJson(para[0], new TypeToken<QueryCar>(){}.getType());
   		String[] bayonets=gson.fromJson(para[1], new TypeToken<String[]>(){}.getType());
   		String begTime=gson.fromJson(para[2], new TypeToken<String>(){}.getType());
   		String endTime=gson.fromJson(para[3], new TypeToken<String>(){}.getType());
		begNum=gson.fromJson(para[4], new TypeToken<Integer>(){}.getType());
		num=gson.fromJson(para[5], new TypeToken<Integer>(){}.getType());
		queueName0=queueName;
		QueryInfo queryInfo=new QueryInfo();
		queryInfo=gson.fromJson(para[6], new TypeToken<QueryInfo>(){}.getType());
   		
   		begTime=base.changeTimeStyle(begTime);
		endTime=base.changeTimeStyle(endTime);
		    	
        //String key = getKey(command,car,tags,begTime,endTime);
		String key=getKey(command,parameter);
		System.out.println("before key="+key);
		
        String s = memorydb.get(key);
        //System.out.println("query.java getFromMemoryDB():key="+key+"  "+"value="+s);
        
        //如果s为空，则调用hbase业务模块
        
        long ll=0l;
        if (s==null){
        	ll=getFromHBase(car,bayonets,begTime,endTime,queryInfo,command+parameter);
        	System.out.println("query.java getFromMemoryDB(),调用HBase处理得到记录数："+ll);
        	if(ll<begNum+num){
    	    	//将从内存数据库取数据并发送到消息队列的功能交给SengResultMSG线程去处理
    	        System.out.println("**********query.java getFromMemoryDB:begTime="+begTime+";endTime="+endTime+";begNum="+begNum+";num="+num+";msgQueue="+queueName+";key="+key);        
    	        SendLongResultMSG ex = new SendLongResultMSG(begNum,num,key,queueName);   
    	        Thread pThread = new Thread(ex);   
    	        pThread.start();   
        	}        		
        }else{
	    	//将从内存数据库取数据并发送到消息队列的功能交给SengResultMSG线程去处理
	        System.out.println("**********query.java getFromMemoryDB:begTime="+begTime+";endTime="+endTime+";begNum="+begNum+";num="+num+";msgQueue="+queueName+";key="+key);        
	        SendLongResultMSG ex = new SendLongResultMSG(begNum,num,key,queueName);   
	        Thread pThread = new Thread(ex);   
	        pThread.start();   
        }
    }
 
    /**
     * 根据输入参数组织Key：车牌号码+车牌类型字符串+开始时间+结束时间
     * @param car
     * @param tgs
     * @param begTime
     * @param endTime
     * @return
     */
    public String getKey(String command,String parameter)
    {
    	int pos=parameter.indexOf("begNum");
    	if(pos>0) 
    		return command+parameter.substring(0, pos-1);
    	else
    		return command+parameter;
     }
    
    /**
     * 对一个column的0-n个值进行过滤，在valuelist中过滤出来。
     * @param familyname
     * @param columnname
     * @param valuelist
     * @return
     */
    public FilterList getFilterListSingleColumnIn(String familyname, String columnname, String[] valuelist){
    	int len0=valuelist.length;
    	if(len0==0) return null;
    	else{
    		List<Filter> filterList=new ArrayList<Filter>();
    		for(int i=0;i<len0;i++){
//    			SubstringComparator comp = new SubstringComparator(valuelist[i]);   
    			BinaryComparator comp = new BinaryComparator(Bytes.toBytes(valuelist[i]));
    			filterList.add(new SingleColumnValueFilter(Bytes.toBytes(familyname),Bytes.toBytes(columnname),CompareOp.EQUAL,comp));
    		}
    		FilterList fl= new FilterList(FilterList.Operator.MUST_PASS_ONE,filterList);
        	return fl;
    	}    	
    }

}

package com.hualu.cloud.serverinterface.offline;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.ehl.itgs.interfaces.bean.QueryInfo;
import com.hualu.cloud.serverinterface.queueListServer;
import com.hualu.cloud.serverinterface.offline.base;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import com.hualu.cloud.basebase.staticparameter;

public class TravelTime4 {
	
	static String startTime; //开始时间	
	static int traveltime = 0; //旅行时间
	static int delayedtime = 0; //延误时间
	static double avgtraveltpeed = 0.0;//平均行驶速度
	
	static String longmsg = "";
	static int cnt = 0;
	
    ConnectionFactory factory = new ConnectionFactory();
    
    static String maxclockskew = staticparameter.getValue("maxclockskew","18000000");
	static String rootdir = staticparameter.getValue("rootdir","hdfs://host61:9000/hbase");
	static String distributed = staticparameter.getValue("distributed","true");
	static String clientPort = staticparameter.getValue("clientPort","2181");
	static String zookeeperquorum = staticparameter.getValue("zookeeperquorum","host51,host52,host53");
	static String regionclasses = staticparameter.getValue("regionclasses","com.ehl.dataselect.business.processor.sync.abnormalBehaviorAnalysis.AbnormalBehaviorEndpoint");
	static String master = staticparameter.getValue("master","host59:60000");
	
	private static Configuration conf = null;
    static {
    	conf = HBaseConfiguration.create();  
    	conf.set("hbase.master.maxclockskew", maxclockskew);
    	conf.set("hbase.rootdir", rootdir);
    	conf.set("hbase.cluster.distributed", distributed);
    	conf.set("hbase.zookeeper.property.clientPort", clientPort);   
    	conf.set("hbase.zookeeper.quorum", zookeeperquorum); 
    	conf.set("hbase.coprocessor.user.region.classes", regionclasses);
    	conf.set("hbase.master", master); 
    }
    
    public static Logger logger = Logger.getLogger(TravelTime4.class);
    
    public TravelTime4(String time) {
    	startTime = time;
    }
    
    public TravelTime4() {

    }
	
    public int rabbittopic(String topic,String msg){
        try {
    		String exchangename=staticparameter.getValue("warnExchange", "warnExchange");
            Connection connection = queueListServer.getConnection(); 
            Channel channel = connection.createChannel();
            channel.exchangeDeclare(exchangename, "topic");
            String message = msg;
            channel.basicPublish(exchangename,topic, MessageProperties.PERSISTENT_TEXT_PLAIN,message.getBytes()); 
            System.out.println("cpbjPE.java rabbittopic():send message " + message);
            channel.close();
            connection.close();
            return 1;
        } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                    return -1;
        } 
    }
	
    /**
	 * 
	 * 分析路线卡口列表
	 * @param tgs 路线[卡口]
	 * @param travelOrientation 方向[方向]
	 * @param endTime 有效时间[时间]
	 * @param travelNo 线路信息
	 * @param bztgsj  应该通过的时间（分）
	 * @return	true：成功,false:失败
	 */	
    public boolean calTravelTime(String[] tgs, String[] travelOrientation, String endTime, String travelNo, String bztgsj, QueryInfo queryInfo) throws Exception {	
    	
    	String[] linkinfo = travelNo.split(",");
    	String linkId = linkinfo[0];
    	String linkName = linkinfo[1];
    	String linkLengthStr = linkinfo[2];
    	String maxSpeed = linkinfo[3];
    	String linknum = linkinfo[4];
    	
		
    	int linkLength = Integer.parseInt(linkLengthStr);
    	int normalTravelTime = Integer.parseInt(bztgsj);   	
    	
    	ArrayList<String> carrecordA = new ArrayList<String>();
    	ArrayList<String> carrecordB = new ArrayList<String>();
    	ArrayList<String> timetravellist = new ArrayList<String>(); //车牌号+过车时间+旅行时间
    	ArrayList<String> timetravellist_available = new ArrayList<String>();
    	int inputORnot = 1; //=1才计算和入库
    	
    	String current1 = base.currentTime();
    	
    	HTable table = new HTable(conf, "by_flow");
    	
    	Scan scan1 = new Scan();
    	scan1.setStartRow(Bytes.toBytes(tgs[1] + "," + String.valueOf(startTime)));
    	System.out.println("A的startrowkey：  "+tgs[1] + "," + String.valueOf(startTime));
    	scan1.setStopRow(Bytes.toBytes(tgs[1] + "," + String.valueOf(endTime)));
    	System.out.println("A的endrowkey：  "+tgs[1] + "," + String.valueOf(endTime));
    	scan1.setMaxVersions(1);
    	
    	ResultScanner rs1 = table.getScanner(scan1);
    	for(Result rr:rs1) {          	
    		String carrecord[] = (new String(rr.getRow())).split(",");
    		carrecordA.add(carrecord[3]+","+carrecord[1]); //车牌号码+过车时间
    	}
    	rs1.close();
    	if(carrecordA.isEmpty()) { //如果有一个卡口在指定时间内没有任何过车记录	
    		System.out.println("carrecordA list为空");
    		inputORnot = 0;
    	}
    	
    	Scan scan2 = new Scan();
    	int timeinterval = staticparameter.getIntValue("timeinterval",30); //单位分钟
    	scan2.setStartRow(Bytes.toBytes(tgs[0] + "," + base.setString(startTime, timeinterval)));
    	System.out.println("B的startrowkey：  "+tgs[0] + "," + base.setString(startTime, timeinterval));
    	scan2.setStopRow(Bytes.toBytes(tgs[0] + "," + String.valueOf(endTime)));
    	System.out.println("B的endrowkey：  "+tgs[0] + "," + String.valueOf(endTime));
    	scan2.setMaxVersions(1);
    	
    	ResultScanner rs2 = table.getScanner(scan2);
    	for(Result rr:rs2) {           	
    		String carrecord[] = (new String(rr.getRow())).split(",");
    		carrecordB.add(carrecord[3]+","+carrecord[1]); //车牌号码+过车时间
    	}
    	rs2.close();
    	if(carrecordB.isEmpty()) { //如果有一个卡口在指定时间内没有任何过车记录	
    		System.out.println("carrecordB list为空");
    		inputORnot = 0;
    	}
    	
    	if(inputORnot == 1) { //两个卡口都在特定时间段内有过车记录才计算旅行时间
	    	for(int i=0; i<carrecordA.size(); i++) {
	    		String carA = carrecordA.get(i).split(",")[0];
	    		String timeA = carrecordA.get(i).split(",")[1];
	    		for(int j=0; j<carrecordB.size(); j++) {
	    			String carB = carrecordB.get(j).split(",")[0];
	        		String timeB = carrecordB.get(j).split(",")[1];
	        		if(carA.equals(carB)) {
	        			long traveltimetmp = base.minusCount(timeB, timeA); //单位是秒(A-B)
	        			if(traveltimetmp <= 30*60 && traveltimetmp > 0) //将超过半小时的旅行时间筛除
	        				timetravellist.add(carA+","+timeA+","+(int)traveltimetmp); //车牌号+过车时间+旅行时间
	        		}
	    		}
	    	}
	    	
	    	//只保留符合条件的，最近的recordItems条记录
	    	int hourFlag = Integer.parseInt(endTime.substring(8, 10)); //取出小时字段
	    	int recordItems = 0;
	    	if(hourFlag>=7 && hourFlag<=21) {
	    		recordItems = staticparameter.getIntValue("dayItems",40);  //白天40条
	    	} else {
	    		recordItems = staticparameter.getIntValue("nightItems",20);//夜间20条
	    	}
	    	
	    	if(timetravellist.size()>recordItems) {
	    		int counter = 0;
	    		for(int i=timetravellist.size()-1; i>0; i--)
	    			if(counter++<recordItems){
	    				timetravellist_available.add(timetravellist.get(i));
	    			}
	    	}else{
	    		for(int i=0; i<timetravellist.size(); i++)
	    			timetravellist_available.add(timetravellist.get(i));
	    	}
	    	
	    	//去掉速度最低的10%记录
	    	Comparator<String> comparator = new Comparator<String>() { //按照旅行时间从大到小排序
	 		   public int compare(String s1, String s2) {
	 		     return (int)(Long.parseLong(s2.split(",")[2])-Long.parseLong(s1.split(",")[2]));
	 		   }
		 	};
		 	Collections.sort(timetravellist_available,comparator);
		 	
		 	ArrayList<String> removelist = new ArrayList<String>();
		 	int removenum = (int)(timetravellist_available.size()*0.1);
		 	for(int i=0; i<removenum; i++) {
		 		removelist.add(timetravellist_available.get(i));
		 	}
		 	if(removelist.size()>0) {
		 		timetravellist_available.removeAll(removelist);
		 	}
    	}
    	
    	long sum_traveltime = 0;
    	for(int i=0; i<timetravellist_available.size(); i++)
    		sum_traveltime += Long.parseLong(timetravellist_available.get(i).split(",")[2]);
    	if(timetravellist_available.size()>0) {
    		traveltime = (int)(sum_traveltime/(long)timetravellist_available.size());   //旅行时间
    	} else {
    		inputORnot = 0;
    		traveltime = 0;
    	}
    	logger.info("旅行时间 = "+traveltime+"秒");
    	if(linkLength>0 && traveltime>0) {
    		avgtraveltpeed = (double)(linkLength/traveltime)*3.6; //行驶车速，单位km/h
    	} else {
    		inputORnot = 0;
    		avgtraveltpeed = 0.0;
    	}
    	logger.info("瞬时行驶速度 = "+avgtraveltpeed+"km/h");
    	if(traveltime>0){
    		delayedtime = traveltime-normalTravelTime>0?traveltime-normalTravelTime:0; //延误时间，单位是秒
    	} else {
    		inputORnot = 0;
    		delayedtime = 0;
    	}
    	logger.info("延误时间 = "+delayedtime);     
    	String updateTime = base.currentTime();		  	
    	
		String msg = "linkId="+linkId+";"+
					 "linkName="+linkName+";"+
					 "linkLength="+linkLength+";"+
					 "normalTravelTime="+normalTravelTime+";"+
					 "actualTravelTime="+traveltime+";"+
					 "delayedTime="+delayedtime+";"+
					 "avgTravelSpeed="+avgtraveltpeed+";"+
					 "updateTime="+updateTime+";";
		
		//旅行时间信息入库
		if(inputORnot == 1) { //inputORnot == 1才入库
			try {
				 HTable traffic_table = new HTable(conf, "by_traffic");
				 Put put = new Put(Bytes.toBytes(linkId+","+updateTime));
				 put.add(Bytes.toBytes("cf"), Bytes.toBytes("info"),
						 Bytes.toBytes(linkId+","+linkName+","+linkLength+","+normalTravelTime+","+traveltime+","+delayedtime+","+avgtraveltpeed+","+updateTime));
				 traffic_table.put(put);
			 } catch (IOException e) {  
				 e.printStackTrace();  
			 }
		}
		
		if(cnt < Integer.parseInt(linknum)-1) {
			longmsg += msg+"@#";
			cnt++;
		} else if(cnt == Integer.parseInt(linknum)-1) {
			longmsg += msg+"#@"+linknum;
			rabbittopic("tt", longmsg);
			cnt = 0;
			longmsg = "";
		}
		
		String current2 = base.currentTime();
		
		long inter = Long.parseLong(current2)-Long.parseLong(current1);
		System.out.println("本次计算旅行时间耗时"+inter+"毫秒");
		
    	carrecordA.clear();
    	carrecordB.clear();
    	timetravellist.clear();
    	timetravellist_available.clear();
		return true;
	}
}

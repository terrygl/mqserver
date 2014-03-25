package com.hualu.cloud.serverinterface.offline;

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;
import org.apache.log4j.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.hualu.cloud.basebase.staticparameter;


public class getTravelTime extends TimerTask {
	static String startTime; //开始时间
	static String currentTime; //现在时刻
	static String endTime; //结束时间
	static String currentORgivenTime; //现在时刻或给定的结束时刻
	static int linkconfignum = 0; //线路信息的数量
	static long frequenceTime = staticparameter.getIntValue("frequenceTime",10); //返回数据时间间隔，单位s
	
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
    
    public static Logger logger = Logger.getLogger(getTravelTime.class);
	
	//构造函数
	public getTravelTime(String time) {
		currentORgivenTime = time;
	}
	
	public getTravelTime() {
		
	}


	public boolean addTravel() throws Exception {
		//遍历by_linkconfig表，统计线路数量
		HTable ctable = new HTable(conf, "by_linkconfig");  	
    	Scan scan = new Scan();
    	scan.setMaxVersions(1);	
    	ResultScanner rs = ctable.getScanner(scan);
    	for(Result rr:rs) {          	
    		linkconfignum++;
    	}
    	rs.close();	
    	System.out.println("Hbase的by_linkconfig表中的row数="+linkconfignum);
    	
		Timer timer = new Timer();
		getTravelTime te = new getTravelTime();
	    timer.schedule(te, 0, frequenceTime*1000); //frequenceTime秒调用一次run()
	    return true;
	}
	 
	public void run() {		
		currentTime = base.currentTime();
		if((currentORgivenTime.compareTo(currentTime)) >= 0 || currentORgivenTime.equals("0") ) {
			endTime = currentTime;  //结束时间
		} else {
			endTime = currentORgivenTime;
		}
		
		//7:00-21:00为白天
		int hourFlag = Integer.parseInt(endTime.substring(8, 10)); //取小时字段
		int timeSpan = 0;
		if(hourFlag>=7 && hourFlag<=21){
			timeSpan = staticparameter.getIntValue("dayTimeSpan",5); //白天的时间段取5分钟
		} else {
			timeSpan = staticparameter.getIntValue("nightTimeSpan",10); //夜间的时间段取10分钟
		}
		startTime = base.setString(endTime, timeSpan); //结束时间的前timeSpan分钟作为开始时间
		
    	HTable config_table = null;;
		try { //从by_linkconfig表中取出线路信息
			config_table = new HTable(conf, "by_linkconfig"); 
			Scan s = new Scan(); 
			s.setMaxVersions(1);
			s.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("info"));
			ResultScanner ss = config_table.getScanner(s);  
			for(Result r:ss){
				String linkrecord[] = new String(r.getValue(Bytes.toBytes("cf"),Bytes.toBytes("info"))).split(",");

				String linkId = linkrecord[0];
				String linkName = linkrecord[1];
				String tgs[] = new String[2];
				tgs[0] = linkrecord[2];
				tgs[1] = linkrecord[3];
				String travelOrientation[] = new String[2];
				travelOrientation[0] = linkrecord[4];
				travelOrientation[1] = linkrecord[5];
				String maxSpeed = linkrecord[6];
				String linkLengthStr = linkrecord[7];
				String bztgsj = linkrecord[8];
				String linknum = linkconfignum+"";
				String linkinfo = linkId+","+linkName+","+linkLengthStr+","+maxSpeed+","+linknum;				
				TravelTime4 tt = new TravelTime4(startTime);
				try {
					System.out.println("开始时间="+startTime+" | 结束时间="+endTime);
					tt.calTravelTime(tgs, travelOrientation, endTime, linkinfo, bztgsj, null);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} finally {
			if (config_table != null) {  
	        	try {
					config_table.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}  
	        }
		}
	}
}



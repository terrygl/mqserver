package com.hualu.cloud.serverinterface.watchDogServer;
import java.io.IOException;
import java.util.TimerTask;
import java.net.*;
import org.apache.log4j.Logger;
import com.hualu.cloud.db.memoryDB;
public class WatchDogServer extends TimerTask{
	/*
	 * 1，定时写redis的MQServers键，更新server列表
	 * 2，更新每个server节点的时间
	 * 
	 */
	private memoryDB mdb=new memoryDB();
	
	public static Logger logger = Logger.getLogger(WatchDogServer.class);
	/*
	 * (non-Javadoc)
	 * @see java.util.TimerTask#run()
	 * 
	 * invoke watch()
	 * 
	 */
	public void run(){
		try {
//			ClientMQ.operateClientMQListTimeout();
			watch();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			logger.info("read redis is fault on watch");
			e.printStackTrace();
		}
	}
	/*
	 * invoke writeRedis()
	 */
	
	public void watch()throws IOException{		
			if(writeRedis()==false){
				throw new IOException("read redis is fault");
			}
	}
	/*
	 * 1,get hostname string
	 * 2,connect  redis , get the names value of hosts  with key "MQServers"
	 * 3,check name value and modify value of "MQServers"
	 * 4,update timestamp of host with key hostname 
	 */
	public boolean writeRedis()  {
//		mdb.del("MQServers");
		InetAddress netAddress;
		String hostname = null;
		try {
			netAddress = InetAddress.getLocalHost();
			hostname = netAddress.getHostName();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		long timestamp =System.currentTimeMillis();
		String mqsIpStr = mdb.get("MQServers");
		if(mqsIpStr==null){
			String MQServerStr = hostname+";";
			//update MQervers
			mdb.add("MQServers",MQServerStr);
		}
		else if(mqsIpStr=="ERRORERROR"){
			logger.error("update MQServer status is fault ");
			return false;
		}
		else{
			String[] mqsIpList = mqsIpStr.split(";");
			int mIpNum = mqsIpList.length;
			int i = 0;
			while(i<mIpNum){
				if(mqsIpList[i].compareTo(hostname)==0){
					break;
				}
				i++;
			}
			if(i>=mIpNum){
				String MQServerStr = mqsIpStr+hostname+";";
				//update MQervers
				mdb.add("MQServers",MQServerStr);
			}
			//update timestamp
			String MQServerName = "MQS:"+hostname;//key
			String MQServerTime = String.valueOf(timestamp);//value
			mdb.add(MQServerName, MQServerTime);
		}
		logger.info("update MQServer status is succeeded ");
		return true;
	}
	
}

package com.hualu.cloud.serverinterface;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Timer;

import com.hualu.cloud.basebase.loglog;
import com.hualu.cloud.basebase.staticparameter;
import com.hualu.cloud.serverinterface.offline.getTravelTime;
import com.hualu.cloud.serverinterface.watchDogServer.ClientMQ;
import com.hualu.cloud.serverinterface.watchDogServer.WatchDogServerThread;

public class mymanager {
	//static String process=loglog.setProcessname("MQServer");
	public static String msgsplit=staticparameter.getValue("msgsplit", "@@");
	public static String parametersplit=staticparameter.getValue("parametersplit", "##");
	public static int expireInterval=staticparameter.getIntValue("expireInterval", 86400);
	public static int maxMessageLen=staticparameter.getIntValue("maxMessageLen", 100000);
	static String process=loglog.setProcessname("MQServer");
	
	public static void main(String args[]) throws IOException{
		//loglog.processname="MQServer";
		System.out.println("mymanager main()"+loglog.processname);
		System.out.println("mymanager main() process"+process);
		
		//msgsplit=staticparameter.getValue("msgsplit", "@@");
		//parametersplit=staticparameter.getValue("parametersplit", "##");
		//expireInterval=staticparameter.getIntValue("expireInterval", 86400);
		//maxMessageLen=staticparameter.getIntValue("maxMessageLen", 100000);
		
		//loglog.setProcessname("MQServer");
		//System.out.println(loglog.processname);
		
		//初始化命令消息队列*****
		queueListServer.listinit();
		
	    //设定定时，方便进行内存数据库master和slave的管理切换
//		
		if(staticparameter.getIntValue("MemoryDBcluster", 0)==1){
			Timer timer=new Timer();
			timer.schedule(new mdbMonite(),6000,staticparameter.getIntValue("MemoryDBMoniteInterval",5000));//默认间隔60秒钟,6秒后后执行
			System.out.println("设置内存数据库集群管理定时任务，每隔6000毫秒执行一次检测操作");
		}		
//*******	
		getTravelTime gt = new getTravelTime("0");
		try {
			gt.addTravel();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		/*
		memoryDB mdb=new memoryDB();
		mdb.add("Master", "172.16.100.62");
		mdb.finalize();
		*/
		
	    //设定定时，方便进行内存数据库master和slave的管理切换
//	    Timer timer=new Timer();
//	    timer.schedule(new mdbMonite(),6000,20000);//确认间隔20秒钟,6秒后后执行
//	    System.out.println("设置内存数据库集群管理定时任务，每隔6000毫秒执行一次检测操作");
		

//******test watchDogServer
		System.out.println("Start watch dog");
		Thread thread = new WatchDogServerThread();
		System.out.println("end watch dog");
		thread.start();
		System.out.println("end watch dog END");
	}
	
	public static void testIPPort(String ip,int port){	
		InetSocketAddress isa = new InetSocketAddress(ip,port);//创建远端地址（IP和Port）。 
		int timeout = 1000;//设置超时时间为1000毫秒。 
		Socket s = new Socket();//创建一个未连接的套接字。 
		try {
			s.connect(isa,timeout);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}//这样1秒钟后，若连接失败会抛异常。
	}
}

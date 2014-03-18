package com.hualu.cloud.serverinterface;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.hualu.cloud.basebase.staticparameter;
import com.rabbitmq.client.Address;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class queueListServer {
	
	private static int commandnum;
	Connection connection;
	public static Channel channel; 
	public static String[] liststatic = new String[3];//队列的统计信息，记录队列名、队列已收到信息总数、当前队列长度
	public static Logger logger = Logger.getLogger(queueListServer.class);
	
	
	//队列初始化--新版本（新增队列创建和topic绑定相关定义）
	public static void listinit() throws IOException{
		//确定消息队列的名字
		Connection connection=null;
//		while(true){
//			connection = getConnection();
//			if(connection!=null){
//				logger.error("MQServer消息服务器连接正常！");
//				break;
//			}
//			else{
//				logger.error("MQServer消息服务器连接异常,休眠一秒后会继续链接，请核查！");
//				try {
//					Thread.sleep(1000);
//				} catch (InterruptedException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//			}
//		}
		for(int i=0;i<60;i++){
			connection=getConnection();	
			if(connection!=null)
				break;
			else{
				logger.error("消息服务器连接异常,休眠一秒后会继续链接，请核查！");
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		if(connection==null){
		logger.error("消息服务器重连了60次依然异常,MQServer 启动失败！请核查！");
		return;
	}
	    //通过配置可设定要启动命令消息队列的名称列表。
	    channel = connection.createChannel();
	    String Queuelist=staticparameter.getValue("Queuelist","RequestCommand4");	    
	    
	    channel.close();
	    connection.close();
	    
	    liststatic[0]=Queuelist;
		liststatic[1]="0";//已处理消息总数
		liststatic[2]="0";//当前消息队列长度,无用
        listener lr = new listener(Queuelist);   
        Thread pThread = new Thread(lr);   
        pThread.start(); 
	    
	}
	
	//队列加1，队列处理消息总数加1
	public static void listinc(){		
		liststatic[1]=String.valueOf(Integer.parseInt(liststatic[1])+1);
		System.out.println(liststatic[0]+" "+liststatic[1]+" "+liststatic[2]);
	}
	
	public void listdestory(){
		try {
			channel.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			connection.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
	
	public static int getQueueNum(){
		return commandnum;
	}
	
	public static Connection getConnection(){//可满足1-4个rabbitmq节点时的connect创建
		String hostnamelist=staticparameter.getValue("MessageHostList","host51:5672");
		String[] hostarray=hostnamelist.split(",");
		logger.debug(hostnamelist+"  "+hostarray.length);
		int i=hostarray.length;
		try{
			ConnectionFactory factory = new ConnectionFactory();
			switch (i){
			case 2:
				String[] host21=hostarray[0].split(":");
				String[] host22=hostarray[1].split(":");
				Address[] addrArr2 = new Address[]{ new Address(host21[0], Integer.parseInt(host21[1]))
		        , new Address(host22[0], Integer.parseInt(host22[1]))};
				logger.debug("两个消息节点"+host21[0]+":"+host21[1]+","+host22[0]+":"+host22[1]);
				return factory.newConnection(addrArr2);
			case 3:
				String[] host31=hostarray[0].split(":");
				String[] host32=hostarray[1].split(":");
				String[] host33=hostarray[2].split(":");
				Address[] addrArr3 = new Address[]{ new Address(host31[0], Integer.parseInt(host31[1]))
		        , new Address(host32[0], Integer.parseInt(host32[1]))
				, new Address(host33[0], Integer.parseInt(host33[1]))};
				logger.debug("三个消息节点"+host31[0]+":"+host31[1]+","+host32[0]+":"+host32[1]+","+host33[0]+":"+host33[1]);
				return  factory.newConnection(addrArr3);
			case 4:
				String[] host41=hostarray[0].split(":");
				String[] host42=hostarray[1].split(":");
				String[] host43=hostarray[2].split(":");
				String[] host44=hostarray[3].split(":");
				Address[] addrArr4 = new Address[]{ new Address(host41[0], Integer.parseInt(host41[1]))
		        , new Address(host42[0], Integer.parseInt(host42[1]))
				, new Address(host43[0], Integer.parseInt(host43[1]))
				, new Address(host44[0], Integer.parseInt(host44[1]))};
				logger.debug("四个消息节点"+host41[0]+":"+host41[1]+","+host42[0]+":"+host42[1]+","+host43[0]+":"+host43[1]+","+host44[0]+":"+host44[1]);
				return factory.newConnection(addrArr4);
			default:
				String[] host11=hostarray[0].split(":");
				factory.setHost(host11[0]);
				logger.debug("一个消息节点"+host11[0]+":"+host11[1]);
				return factory.newConnection();
			}
		}catch(IOException e) {
			// TODO Auto-generated catch block
			logger.warn("创建消息链接时异常："+e.getMessage());
			return null;
		}
	}
	
}

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
	
	public static Connection getConnection(){//可满足1-n个rabbitmq节点时的connect创建
		String hostnamelist=staticparameter.getValue("MessageHostList","host51:5672");
	    String[] hostarray=hostnamelist.split(",");
	    int i=hostarray.length;
	    
	    Address[] addrArr =new Address[i] ;
	     
	    for (int j = 0; j < i; j++){
	    	 String[] host =hostarray[j].split(":");
	    	 addrArr[j] = new Address(host[0],Integer.parseInt(host[1]));
	    }
		try{
			ConnectionFactory factory = new ConnectionFactory();
			return factory.newConnection(addrArr);
		}catch(IOException e) {
			// TODO Auto-generated catch block
			logger.warn("创建消息链接时异常："+e.getMessage());
			return null;
		}	     
	}
	
}

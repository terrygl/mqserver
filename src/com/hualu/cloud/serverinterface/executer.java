package com.hualu.cloud.serverinterface;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import com.ehl.itgs.interfaces.bean.Layout;
import com.ehl.itgs.interfaces.bean.QueryInfo;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.hualu.cloud.basebase.staticparameter;
import com.hualu.cloud.serverinterface.offline.query;
import com.hualu.cloud.serverinterface.offline.getTravelTime;
import com.hualu.cloud.serverinterface.offline.TravelTime4;
import com.hualu.cloud.serverinterface.realtime.CloudLayout;
import com.hualu.cloud.serverinterface.realtime.CloudRTA;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
//import com.hualu.cloud.serverinterface.offline.getTravelTime;

/**
 * command组成：为listener接收到消息的转发，含三部分：返回消息队列名称@@命令@@参数
 * @author yanghy
 *
 */
public class executer implements Runnable {
    String  message = null; 
    static int calltraveltimeORnot = 0;

    Gson gson=new Gson();
    public static Logger logger = Logger.getLogger(executer.class);
    
    public executer(String message) {   
        super();
        this.message = message;  
    }   

    @Override  
    public void run(){   
    	// TODO Auto-generated method stub 
		//消息队列显相关参数
    	Connection connection = queueListServer.connection;
    	if (connection==null ||!connection.isOpen()){
			connection=queueListServer.getConnection();
			if(connection==null){
				logger.error("消息服务器连接异常,休眠一秒后会继续链接，请核查！");
				return;
			}
    	}
	    
		try {
			
			Channel channel= connection.createChannel();
			//获取response队列的名称
			String ret[]=message.split(mymanager.msgsplit);
			String responseQueueName=ret[0];
			
			//获取命令
			String commandname=ret[1];
			
			//获取命令参数字符串		
			String parameterList=ret[2];
			
			//启动指定业务模块：布控
			if(commandname.indexOf("InterfaceLayout.Layout")==0){//命令为运行QueryInterface
				logger.info("executer.java run()收到InterfaceLayout.layout消息");
				
				//调用后台处理业务逻辑
				CloudLayout cl=new CloudLayout();
				boolean bret=cl.layout(ret[2]);//在每个后台处理逻辑中进行写消息
				
				//获取处理结果写到消息回复队列
				String message;
				if(bret==true)
					message="1"+mymanager.msgsplit+"1"+mymanager.msgsplit+"TRUE";//执行成功，返回一条记录，结果为true
				else
					message="1"+mymanager.msgsplit+"1"+mymanager.msgsplit+"FALSE";
						
				if(responseQueueName==null)
					logger.error("responseQueueNamem is null");
				else{
					channel.basicPublish("", responseQueueName, null, message.getBytes());
					logger.info("executer.java run() response message"+responseQueueName+"**** send message"+message);
				}
			}
			
			//启动指定业务模块：撤控
			if(commandname.indexOf("InterfaceLayout.cancelLayout")==0){//命令为运行QueryInterface
				logger.info("executer.java run()收到InterfaceLayout.cancellayou消息");
				
				//调用后台处理业务逻辑
				CloudLayout cl=new CloudLayout();
				boolean bret=cl.cancelLayout(ret[2]);//在每个后台处理逻辑中进行写消息
				
				//获取处理结果写到消息回复队列
				String message;
				if(bret==true)
					message="1"+mymanager.msgsplit+"1"+mymanager.msgsplit+"TRUE";
				else
					message="1"+mymanager.msgsplit+"1"+mymanager.msgsplit+"FALSE";
						
				channel.basicPublish("", responseQueueName, null, message.getBytes());
				logger.info("executer.java run() response message"+responseQueueName+"**** send message"+message);
			}
			
			//卡口布控
			if(commandname.indexOf("InterfaceRTA.addTgs")==0){//命令为运行InterfaceQuery.query
				logger.info("收到InterfaceRTA.addTgs命令");
				
				//调用后台处理业务逻辑
				CloudRTA cl=new CloudRTA();
				boolean bret=cl.addTgs(ret[2]);//在每个后台处理逻辑中进行写消息
				
				//获取处理结果写到消息回复队列
				String message;
				if(bret==true)
					message="1"+mymanager.msgsplit+"1"+mymanager.msgsplit+"TRUE";
				else
					message="1"+mymanager.msgsplit+"1"+mymanager.msgsplit+"FALSE";
						
				channel.basicPublish("", responseQueueName, null, message.getBytes());
				logger.info("executer.java run()response message"+responseQueueName+"**** send message"+message);
			}	
			
			//特殊车辆布控
			if(commandname.indexOf("InterfaceRTA.addCar")==0){//命令为运行InterfaceQuery.query
				logger.info("收到InterfaceRTA.addCar命令");
				
				//调用后台处理业务逻辑
				CloudRTA cl=new CloudRTA();
				boolean bret=cl.addCar(ret[2]);//在每个后台处理逻辑中进行写消息
				
				//获取处理结果写到消息回复队列
				String message;
				if(bret==true)
					message="1"+mymanager.msgsplit+"1"+mymanager.msgsplit+"TRUE";
				else
					message="1"+mymanager.msgsplit+"1"+mymanager.msgsplit+"FALSE";
						
				channel.basicPublish("", responseQueueName, null, message.getBytes());
				logger.info("executer.java run()response message"+responseQueueName+"**** send message"+message);
			}	
			
			//预警
			if(commandname.indexOf("InterfaceRTA.addWarn")==0){//命令为运行InterfaceQuery.query
				logger.info("收到InterfaceRTA.addWarn命令");
				
				//调用后台处理业务逻辑
				CloudRTA cl=new CloudRTA();
				boolean bret=cl.addWarn(ret[2]);//在每个后台处理逻辑中进行写消息
				
				//获取处理结果写到消息回复队列
				String message;
				if(bret==true)
					message="1"+mymanager.msgsplit+"1"+mymanager.msgsplit+"TRUE";
				else
					message="1"+mymanager.msgsplit+"1"+mymanager.msgsplit+"FALSE";
						
				channel.basicPublish("", responseQueueName, null, message.getBytes());
				logger.info("executer.java run()response message"+responseQueueName+"**** send message"+message);
			}	
			
			//取消RTA
			if(commandname.indexOf("InterfaceRTA.cancel")==0){//命令为运行InterfaceQuery.query
				logger.info("收到InterfaceRTA.cancel命令");
				
				//调用后台处理业务逻辑
				CloudRTA cl=new CloudRTA();
				boolean bret=cl.cancel(ret[2]);//在每个后台处理逻辑中进行写消息
				
				//获取处理结果写到消息回复队列
				String message;
				if(bret==true)
					message="1"+mymanager.msgsplit+"1"+mymanager.msgsplit+"TRUE";
				else
					message="1"+mymanager.msgsplit+"1"+mymanager.msgsplit+"FALSE";
						
				channel.basicPublish("", responseQueueName, null, message.getBytes());
				logger.info("response message:"+responseQueueName+"**** send message"+message);
			}	
			
			//卡口车辆查询
			if(commandname.indexOf("InterfaceQuery.query")==0){//命令为运行InterfaceQuery.query
				System.out.println("executer：收到InterfaceQuery.query命令");
				
				//调用后台处理业务逻辑
				query cq=new query();
				cq.getFromMemoryDB(responseQueueName,commandname,parameterList);//队列名、命令、参数
			}
			
			//启动指定业务模块：旅行时间
			if(commandname.indexOf("CloudTravelTime.addTravel")==0){
				logger.info("executer.java run()收到CloudTravelTime.addTravel消息");
				//调用后台处理业务逻辑
				logger.info("Server端接收到的信息："+ret[2]);
			}
			
			//关闭消息通道和连接
			channel.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.info("executer Exception:"+e.getMessage());
			e.printStackTrace();
		}		
    }      

}


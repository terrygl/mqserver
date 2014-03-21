package com.hualu.cloud.serverinterface;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.hualu.cloud.basebase.staticparameter;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;

public class listener implements Runnable{
	public static int threadnum=0;
	public static int threadmax=100;
	public String requestQueueName;
	public static Logger logger = Logger.getLogger(listener.class);
    public listener(String queuename) {   
        super();
        this.requestQueueName=queuename;
        logger.info("服务器端启动命令消息侦听线程");
    }   
    
    @Override  
    public void run() {   
        // TODO Auto-generated method stub  

		
    	while(true){		    
	     	try {
	     		//create queue list
	     		queueInit(requestQueueName);
	     		System.out.println("已执行消息队列相关的初始化");
	    		//消息队列显相关参数
	     		Connection connection = queueListServer.connection;
		    	if (connection==null ||!connection.isOpen()){
					connection=queueListServer.getConnection();
					if(connection==null){
						logger.error("消息服务器连接异常,休眠一秒后会继续链接，请核查！");
						return;
					}
		    	}
	    	    Channel channel= connection.createChannel();
	    	    //requestQueueName 队列名有些问题
				channel.queueDeclare(requestQueueName, false, false, false, null);
				
			    QueueingConsumer consumer = new QueueingConsumer(channel);   
			    channel.basicConsume(requestQueueName, true, consumer);
			 	QueueingConsumer.Delivery delivery;
			    	
				//侦听request命令消息队列
			    while(true){
			    	//防止rabbitmq server中断
//			    	if (connection==null ||!connection.isOpen()){
//							connection=queueListServer.getConnection();
//					}
					delivery = consumer.nextDelivery();
				    String message = new String(delivery.getBody(),"UTF-8");   
				    logger.info(" listener Received '" + message + "'");
				    	
				    //用线程处理收到的命令消息，将命令消息交给具体执行线程去处理
				    executer ex = new executer(message);   
				    Thread pThread = new Thread(ex);   
				    pThread.start();   
				    
			    }
			}catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
    	}

    }
    
	//队列初始化
	public void queueInit(String requestQueueName) throws IOException{
		//消息主机
		Connection connection = queueListServer.connection; 		
    	if (connection==null ||!connection.isOpen()){
    		queueListServer.connection=queueListServer.getConnection();
    		connection = queueListServer.connection;
    	}
    	else{
    		return;
    	}
    	//create channel and declare queue
    	Channel channel = connection.createChannel();
		if(requestQueueName==null){
			if((requestQueueName=staticparameter.getValue("Queuelist","RequestCommand4"))==null){
				logger.warn("配置文件缺少消息队列Queuelist项");
				return;
			}
		}
	    
	    //通过配置可设定要启动命令消息队列的名称列表。
	    String str[]=requestQueueName.split(",");
		for(int i=0;i<str.length;i++){   
			String queueName=str[i];
		    if(queueName==null){
		    	logger.warn("配置文件缺少消息队列Queuelist项");
		    }else{
			    logger.info("commandmessage队列:"+queueName);
			    channel.queueDeclare(queueName,false,false,false,null);//通过声明保证该队列存在
		    }	    
		}
	    channel.close();	        
	}

}

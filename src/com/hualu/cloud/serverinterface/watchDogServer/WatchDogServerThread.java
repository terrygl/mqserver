package com.hualu.cloud.serverinterface.watchDogServer;
import java.io.IOException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Timer;
import java.util.concurrent.Callable;
import org.apache.log4j.Logger;
import com.hualu.cloud.basebase.staticparameter;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.QueueingConsumer;

public class WatchDogServerThread extends Thread{

	private final static String QUEUE_NAME_REQUEST="WATCHDOGREQUEST2";
	public static Logger logger = Logger.getLogger(WatchDogServerThread.class);
	public WatchDogServerThread(){
		initWatchDog();
	}
	
	public void initWatchDog(){
		if(ClientMQ.initBind==false){
			ClientMQ.clearRabbitMQBind();
			logger.info("clearRabbitMQBind");
		}
		else{
			logger.info("I do not need unbind queue");
		}
	}
	public void run(){
		try{
		startRun();
		}
		catch(Exception e){
			e.getMessage();
		}
	}
	public void startRun()throws java.io.IOException,
	java.lang.InterruptedException {
//--------------------定时写redis---------------------
		Timer timer = new Timer();
		timer.schedule(new WatchDogServer(),0,Long.parseLong(staticparameter.getValue("serverPollInterval","10000")));
////****USE TIMERTASK ******
		timer.schedule(new WatchDogClientMQList(),0, Long.parseLong(staticparameter.getValue("clientUseRabbitmqPollInterval","10000")));
//		logger.info("WatchDogServerThread after WatchDogClientMQList() timer:"+getNowDate());
//---------------------------------------------------
//**************rabbitmq connection,only one****************
		Connection connection = null;  
		Channel channel =null;
		while(true){
			try{
				if(ClientMQ.connection.isOpen()==false||ClientMQ.connection==null){
					if(ClientMQ.createConnection()==false){
						logger.error("create connection is fault");
					}
					connection = ClientMQ.connection;				
					logger.error("create connection is fault");			
				}
				else{
					connection=ClientMQ.connection;
				}
			}
			catch(Exception e){
				try{
					if(connection!=null)
						connection.close();
				}catch(Exception ee){
					logger.error("warllistener运行过程捕捉到消息服务异常,关闭消息链接是出现异常:"+e.getMessage());
				}
				logger.error("create rabbitmq connection is fault");
			
			}	

			//****** create channel ******
			try{
				
				channel = connection.createChannel();
			}
			catch(Exception e){
				logger.error("create rabbitmq channel is fault ");
			}
			//****** declare queue ******
			try{
				channel.queueDeclare(QUEUE_NAME_REQUEST, false, false, false, null);
			}
			catch(Exception e){
				logger.error("rabbitmq channel queueDeclare is fault ");
			}
			
		//  QueueingConsumer consumer = new QueueingConsumer(channel);
		//	read message from channel
		//	channel.basicConsume(QUEUE_NAME_RESPONSE, true, consumer);
			while(true){
		    	//receive message from request queue use circulation ;
				GetResponse response=null;
			try{
				response=channel.basicGet(QUEUE_NAME_REQUEST,true);//???
			}
			catch(Exception e){
				logger.error("rabbitmq response is fault ");
			}

				//check mqstate		
			if(response==null){
				try{
		//				long endtime = System.currentTimeMillis();
					Thread.sleep(500);
//					logger.info("WatchDogServerThread do not receive message via RabbitMQ , wait for 0.5 second");
				}
				catch(Exception e){
					//return false;
					e.getMessage();
				}
			}
			else{
				logger.info("WatchDogServerThread receive a message at:"+ClientMQ.getNowDate());
				//when receive message ,initial starttime
				String strMsg=new String(response.getBody());
				logger.info(strMsg);
				//----------staticparameter.msgsplit
				String[] str=strMsg.split("@@");
				//queueTemp+"@@"+session+"@@"+hostname+"@@"+'?';
				//analysis messages,str[2] represents hostname
				String hostnameClientMQ=str[2];
		//**********ADD tmpCliMQ to ClientList***************
				ClientMQ.operateClientMQList(hostnameClientMQ,1);
		//**********CHECK every client in clientList and unbind overtime one
				//write message("OK") to temporary queue
				String ackMessage  = "OK";
				String queueName = str[0];
				//send message to response queue
				try{
					channel.basicPublish("",queueName, null,ackMessage.getBytes());
				}
				catch(IOException e){
					logger.error("write rabbit queue fault");
					break;
				}
				logger.info("WatchDogServerThread figure out  a message at:"+ClientMQ.getNowDate());
		    }
			//use channel end
//			channel.close();
		}
		}
	}

}
//------traverse arraylist



package com.hualu.cloud.serverinterface.watchDogServer;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import org.apache.log4j.Logger;
import com.hualu.cloud.basebase.staticparameter;
import com.hualu.cloud.db.memoryDB;
import com.rabbitmq.client.Address;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.GetResponse;

public class ClientMQ {
	public static Connection connection=null;
	public static Logger logger = Logger.getLogger(ClientMQ.class);
	public static ArrayList<ClientMQ> listClientMQ = new ArrayList<ClientMQ>();
	public static final long TIME_OUT =Long.parseLong(staticparameter.getValue("clientUseRabbitmqTimeOut","20000"));//60秒
	private String hostname;
	public String hostType;//待定
	long bindTimeEx;//time that client bind mq
	public static  memoryDB mdb = new memoryDB();
	public static  boolean initBind = false;
//	private memoryDB mdb = null;
	
	public static boolean clearRabbitMQBind(){
		ClientMQ clientMQ=new ClientMQ();
		logger.info("start and first use redis");
		String hostnameList = clientMQ.getClientFromdb("Tomcat");
		logger.info("end use redis");
		logger.info(hostnameList);
		if(hostnameList==null){
			logger.warn("read redis is fault ,data is null");
			return false;
		}
		String htNameArr[] = hostnameList.split(";");
		for(int i=0;i<htNameArr.length;i++){
			logger.info(htNameArr[i]);
			clientMQ.setHostname(htNameArr[i]);
			if(connection==null){
				connection=createConnection();
			}
			clientMQ.executeUnBind();
		}
		hostnameList = clientMQ.getClientFromdb("Alarm");
		if(hostnameList==null){
			logger.warn("read redis is fault ,data is null");
			
			return false;
		}
//		logger.info(hostnameList);
		htNameArr=null;
		htNameArr = hostnameList.split(";");
		for(int i=0;i<htNameArr.length;i++){
			logger.info(htNameArr[i]);
			clientMQ.setHostname(htNameArr[i]);
			if(connection==null){
				connection=createConnection();
			}
			clientMQ.executeUnBind();
		}
		initBind = true;
		return true;
	}
	
	/*
	 * 
	 */
//	public ClientMQ(String hostname){
//		this.hostname = hostname;
//	}
	public boolean findClientInmdb(String str){
		String clientList =mdb.get("clientAlarmList");
//		String clientList =mdb.get("clientTomcatList");
		String clientArr[] =clientList.split(";");
		for(int i=0;i<clientArr.length;i++){
			if(clientArr[i].compareTo(hostname)==0){
				return true;
			}
		}
		return false;
	}
	/*
	 * NO USE
	 */
	public String getClientFromdb(String str){
		if(str=="Alarm"){
			return mdb.get("clientAlarmList");
		}
		else if(str == "Tomcat"){
			return mdb.get("clientTomcatList");
		}
		else{
			return mdb.get("clientTomcatList");
		}
//		return null;
	}
	
	public void setHostname(String hostname){
		this.hostname =hostname;
	}
	
	public String getHostname(){
		return hostname;
	}
	
	private String getExchange(){
		return mdb.get(hostname+":exchange");
	}
	private String getQueue(){
//		String str = mdb.get(hostname+":Queue");
		return mdb.get(hostname+":Queue");
	}
	private String getTopics(){
		return mdb.get(hostname+":Topics");
	}
	/* ######
	 * 
	 */
	public boolean executeUnBind(){
		//get exchange
		String exchange = getExchange();
		if(exchange == "ERRORERROR"||exchange == null){
			return false;
		}
		logger.error("getExchange after return is fault,exchange="+exchange);
		//get queue list
		String queueList = getQueue();
		if(queueList == "ERRORERROR"||queueList ==null){
			return false;
		}
		String queueArr[] = queueList.split(";");
		//get topics list
		String topicsList = getTopics();
		if(topicsList == "ERRORERROR"||topicsList==null){
			return false;
		}
		String topicsArr[] = topicsList.split(";");
		int len = queueArr.length;
		for(int i=0;i<len;i++){
			try{
				if(connection==null){
					connection=createConnection();
				}
				Channel channel = createChannel();
				channel.exchangeDeclare(exchange, "topic");
			    String[] topics=topicsArr[i].split(",");
			    int topicnum=topics.length;
			    for(int j=0;j<topicnum;j++)
			    	channel.queueUnbind(queueArr[i],exchange,topics[j]);
			    channel.close();
			}
			catch(Exception e){
				logger.error("unbind queue is fault");
				e.getMessage();
				return false;
			}
		}
		logger.info("GOOD!!! unbind queue is succeed");
		return true;
	}
	/* ######
	 * function : bind a queue to exchange with routing key
	 * 		note that: read exchange name ,queue name and routing keys(topics) from redis
	 * return : true (bind succee)
	 * 			false (bind fault)
	 */
	public boolean executeBind(){
//		if(bindState){
//			logger.info("it has been binded");
//			return true;
//		}
		//get exchange
		String exchange = getExchange();
		if(exchange == "ERRORERROR"||exchange==null){
			return false;
		}
		//get queue list
		String queueList = getQueue();
		if(queueList == "ERRORERROR" ||exchange ==null){
			return false;
		}
		String queueArr[] = queueList.split(";");
		//get topics list
		String topicsList = getTopics();
		if(topicsList == "ERRORERROR"||exchange ==null){
			return false;
		}
		String topicsArr[] = topicsList.split(";");
		int len = queueArr.length;
		
		for(int i=0;i<len;i++){
			try{
				Channel channel = createChannel();
				channel.exchangeDeclare(exchange, "topic");
			    String[] topics=topicsArr[i].split(",");
			    int topicnum=topics.length;
			    for(int j=0;j<topicnum;j++)
			    channel.queueBind(queueArr[i],exchange,topics[j]);
			    channel.close();
			}
			catch(Exception e){
				e.getMessage();
				return false;
			}
		}
		return true;
	}
	public static Connection createConnection(){
//		try{
//			if(connection==null){
//				//create rabbitmq conncetion factory
//				ConnectionFactory factory = new ConnectionFactory();
//				factory.setHost(staticparameter.getValue("MessageHostList","host51"));
//				//通过工厂建立新链接			
//				connection= factory.newConnection();
//			}
//		}
//		catch(Exception e){
//			return null;
//		}
//		return connection;
		String hostnamelist=staticparameter.getValue("MessageHostList","host51:5672");
		String[] hostarray=hostnamelist.split(",");
		logger.debug(hostnamelist+"  "+hostarray.length);
		int i=hostarray.length;
		try{
//			if(connection==null){
				ConnectionFactory factory = new ConnectionFactory();
				switch (i){
					case 2:
						String[] host21=hostarray[0].split(":");
						String[] host22=hostarray[1].split(":");
						Address[] addrArr2 = new Address[]{ new Address(host21[0], Integer.parseInt(host21[1]))
				        , new Address(host22[0], Integer.parseInt(host22[1]))};
						logger.debug("两个消息节点"+host21[0]+":"+host21[1]+","+host22[0]+":"+host22[1]);
						connection = factory.newConnection(addrArr2);
						break;
					case 3:
						String[] host31=hostarray[0].split(":");
						String[] host32=hostarray[1].split(":");
						String[] host33=hostarray[2].split(":");
						Address[] addrArr3 = new Address[]{ new Address(host31[0], Integer.parseInt(host31[1]))
				        , new Address(host32[0], Integer.parseInt(host32[1]))
						, new Address(host33[0], Integer.parseInt(host33[1]))};
						logger.debug("三个消息节点"+host31[0]+":"+host31[1]+","+host32[0]+":"+host32[1]+","+host33[0]+":"+host33[1]);
						connection = factory.newConnection(addrArr3);
						break;
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
						connection = factory.newConnection(addrArr4);
						break;
						default:
						String[] host11=hostarray[0].split(":");
						factory.setHost(host11[0]);
		//				logger.debug("一个消息节点"+host11[0]+":"+host11[1]);
						logger.info("一个消息节点"+host11[0]+":"+host11[1]);
						connection = factory.newConnection();
//					}
			}
		}catch(IOException e) {
			// TODO Auto-generated catch block
			logger.warn("创建消息链接时异常："+e.getMessage());
			return null;
		}
		return connection;
	}
	
	public static Channel createChannel(){

		Channel channel =null;
		try{		
		//create channel
			channel = connection.createChannel();
		}
		catch (Exception e){
			e.getMessage();
		}
		return channel;
	}
	/* ######
	 * 
	 */
	
	public boolean checkClientMQTimeEx(long timeout){
		long timeNow = System.currentTimeMillis();//ms
		if(timeNow - this.bindTimeEx>timeout){
			return false;// time out
		}
		return true;
	}
	/* ######
	 * unbind timeout client
	 * return 
	 * 		succeed:true
	 * 		fail:false
	 */
	public static boolean operateClientMQListTimeout(){
		//del ClientMQ
		List<ClientMQ> list = listClientMQ;
		Iterator<ClientMQ> it = list.iterator();
		while(it.hasNext()){
			ClientMQ client = it.next();
			if(!client.checkClientMQTimeEx(TIME_OUT)){				
				it.remove();
				if(!client.executeUnBind()){
					return false;
				}
			}
		}
		return true;
	}
	/* ###### important (opType=2)
	 * unbind all or update all
	 * 		opType:the operation of client(1,update;2,delete)
	 */
	public static boolean operateClientMQList(int opType){
		if(opType==1){//update ClientMQ
			List<ClientMQ> list = listClientMQ;
			Iterator<ClientMQ> it = list.iterator();
			while(it.hasNext()){
				it.next().updateClientMQTimeEx();
			}
//			client.executeBind();
		}
		else if (opType==2){//del ClientMQ
			List<ClientMQ> list = listClientMQ;
			Iterator<ClientMQ> it = list.iterator();
			while(it.hasNext()){
				ClientMQ client = it.next();
				System.out.println("operateClientMQList"+client.hostname);
				if(client.executeUnBind()==false){
					logger.error("unbind queue is fault");
				}			
				it.remove();
			}
//			list.clear();//or use it.remove(); above two lines
		}
		else{
			return false;
		}
		return true;
	}
	/* ###### important (opType=1)
	 * parameter:
	 * 		clientHostname: the hostname of client 
	 * 		opType:the operation of client(1,update;2,delete)
	 */
	public static boolean operateClientMQList(String clientHostname,int opType){
		if(opType==1){//update ClientMQ
			ClientMQ client = new ClientMQ();
			client.setHostname(clientHostname);
			updateClientMQList(client);
			if(!client.executeBind()){
				return false;
			}
		}
		else if (opType==2){//del ClientMQ
			ClientMQ client = new ClientMQ();
			client.setHostname(clientHostname);
			delClientMQList(client);
			if(!client.executeUnBind()){
				return false;
			}
		}
		else{
			return false;
		}
		return true;
	}
	/* NO USE
	 * 
	 */
	static void delClientMQList(ClientMQ clientObj){
		List<ClientMQ> list = listClientMQ;
		Iterator<ClientMQ> it = list.iterator();
//		boolean clientObjExisted = false;
		logger.info(clientObj.hostname);
		while(it.hasNext()){
			ClientMQ client = it.next();
			logger.info(client.hostname);
			//if clientObj is existed in list of clientMQ
			if(client.hostname.compareTo(clientObj.hostname)==0){
//				clientObjExisted=true;//clientobj is existed in list
				list.remove(client);//########use this method is fault				
				break;
			}
		}
	}
	
	/* ######
	 * check new client and notify ,add one
	 * parameter:
	 * 		clientObj:new active ClientMQ object 
	 */
	public static void updateClientMQList(ClientMQ clientObj){
		List<ClientMQ> list = listClientMQ;
		Iterator<ClientMQ> it = list.iterator();
		boolean clientObjExisted = false;
		while(it.hasNext()){
			ClientMQ client = it.next();
//			logger.info(client.hostname);
			//if clientObj is existed in list of clientMQ
			if(client.getHostname().compareTo(clientObj.getHostname())==0){
				clientObjExisted=true;
				//update clientMQ time
				client.updateClientMQTimeEx();
				client.bindTimeEx=System.currentTimeMillis();
				break;
			}
		}	
		//clientObj is not existed in list of listClientMQ
		//ADD clientObj to listClientMQ
		if(!clientObjExisted){
			clientObj.updateClientMQTimeEx();
			list.add(clientObj);
		}
	}
	/*
	 * 
	 */
	public static boolean checkClientMQ(ClientMQ clientObj){
		List<ClientMQ> list = listClientMQ;
		Iterator<ClientMQ> it = list.iterator();
		while(it.hasNext()){
			ClientMQ client = it.next();
			if(client.getHostname().compareTo(clientObj.getHostname())==0){
				return true;
			}
		}
		return false;
	}
/*
 * print listClientMQ
 */
	public static String printClientMQList(){
		 System.out.println(listClientMQ.size());
		Iterator<ClientMQ> it = listClientMQ.iterator();
		while (it.hasNext())
		{
		    System.out.println(it.next().getHostname());
		}	
		return "TRUE";
	}
	
	public void updateClientMQTimeEx(){
		bindTimeEx = System.currentTimeMillis();
	}
	 /**
	  * 获取现在时间
	  * 
	  * @return 返回时间类型 yyyy-MM-dd HH:mm:ss
	  */
	 public static String getNowDate() {
	  Date currentTime = new Date();
	  SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	  String dateString = formatter.format(currentTime);
	  return dateString;
	 }
	 /*
	  * 
	  */
	 public static boolean printlistClientMQ(){
		 logger.info(listClientMQ.toString());
		 return true;
	 }
}

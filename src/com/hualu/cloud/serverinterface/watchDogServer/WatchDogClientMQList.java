package com.hualu.cloud.serverinterface.watchDogServer;

import java.util.TimerTask;
import org.apache.log4j.Logger;
public class WatchDogClientMQList extends TimerTask{
	static Logger logger = Logger.getLogger(WatchDogClientMQList.class);
	public void run(){
		logger.info("watchDogClientMQList");
		ClientMQ.operateClientMQListTimeout();
	}
}

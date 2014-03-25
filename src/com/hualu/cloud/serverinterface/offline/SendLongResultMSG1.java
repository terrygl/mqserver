package com.hualu.cloud.serverinterface.offline;

import java.io.IOException;
import java.util.ArrayList;

import com.google.gson.Gson;
import com.hualu.cloud.basebase.staticparameter;
import com.hualu.cloud.db.memoryDB;
import com.hualu.cloud.serverinterface.mymanager;
import com.hualu.cloud.serverinterface.queueListServer;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class SendLongResultMSG1  implements Runnable {
    String key,msgQueue;  
    int begNum,num;
    static memoryDB memorydb=new memoryDB();

    Gson gson=new Gson();
    
    public SendLongResultMSG1(int begNum,int num,String key,String msgQueue) {   
        super();
        this.key = key; 
        this.msgQueue=msgQueue;
        this.begNum=begNum;
        this.num=num;
    }   

    @Override  
    public void run(){   
    	// TODO Auto-generated method stub 
		//消息队列相关参数
		Connection connection;
		int i0=begNum;
		int i00=begNum+num-1;
		int ret=1;//如果ret为0表示无返回结果
	    int m = 100*num;
        
		try {
			connection=queueListServer.getConnection();
		    Channel channel= connection.createChannel();
		    
	        //从redis中取出指定数据，并返回给rabbitmq
	        ArrayList<String> messageArray=new ArrayList<String>();
	        //String message0="resultnum="+s+staticparameter.msgsplit+"recordnum="+(i00-i0+1);//表示执行结果总共S条记录，本次查询返回m条记录，分P个消息返回
	        String message0=String.valueOf(m)+mymanager.msgsplit+(i00-i0+1);//表示执行结果总共S条记录，本次查询返回m条记录，分P个消息返回
	        System.out.println("message0="+message0);
//	        int i=i0;
	        long i=i0;
	       
	        String message="";
	        String value="";
	        int ii=i0-1;// 记录处理到了第几记录，如果记录编号大于i00，则跳出循环
	        while(true){
	        	if(ii==i00){//如果记录数准备完毕，形成最后一条消息
	        		messageArray.add(message);
	         		break;
	        	}

		        for(long j=i;j<=i00;j++)
		        {
		        	ii++;
		        	value=memorydb.get(key+base.getLongNumber(j));
		        	//组织消息
		        	if((message.length()+message.length())>mymanager.maxMessageLen){//如果消息大于一个长度，则开始组织一个新的消息
		        		messageArray.add(message);
		           		message=mymanager.msgsplit+value;
		        		i=++j;		        		
		        		break;
		        	}
		        	else{
		        		message=message+mymanager.msgsplit+value;
		        	}
		        }
	        }
	        
    		//将消息发送到rabbitmq
	        message0=message0+mymanager.msgsplit+messageArray.size();//消息前缀：总共N条，返回m条，总共p页
	        System.out.println("分为消息数目"+messageArray.size());
	        String sendmessage="";
	        for(int l=0;l<messageArray.size();l++){
	        	sendmessage=message0+messageArray.get(l);
	        	channel.basicPublish("", msgQueue, null, sendmessage.getBytes());
	        	System.out.println("sendmessage="+sendmessage);
	        	
	        }
			//关闭消息通道和连接
			channel.close();
			connection.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

    }   
}

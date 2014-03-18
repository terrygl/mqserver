package com.hualu.cloud.serverinterface.offline;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import com.google.gson.Gson;

import com.hualu.cloud.basebase.staticparameter;
import com.hualu.cloud.db.memoryDB;
import com.hualu.cloud.serverinterface.mymanager;
import com.hualu.cloud.serverinterface.queueListServer;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class SendLongResultMSG implements Runnable {
    String key,msgQueue;  
    int begNum,num;
    static memoryDB memorydb=new memoryDB();

    Gson gson=new Gson();
    
    public SendLongResultMSG(int begNum,int num,String key,String msgQueue) {   
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
		int i0=0;
		int i00=0;
		int ret=1;//如果ret为0表示无返回结果
	    int m = 0;
        
		//System.out.println("SendResultMSG.java run() ;begNum="+begNum+";num="+num+";msgQueue="+msgQueue+";key="+key);
		//从内存数据库中获取处理结果并返回
        String s = memorydb.get(key);
        m=Integer.parseInt(s);
        System.out.println("m="+m);
        if(s==null)
        {
        	System.out.println("SendResultMSG.java run():没有记录！");
        	ret=0;
        }
        else{
        	//System.out.println("SendResultMSG.java run():记录总数！"+s);
        	m = base.getCount(Integer.parseInt(s),begNum,num);
        	System.out.println("记录总数="+m);
        	//计算起始记录序号
	        i0=begNum;
	        if(m==0 || begNum<0 || num<0) 
	        	ret=0;//如果根据begnum和num得到的返回记录数为0，或者输入异常，将m置为0，便于后面处理为没有合适返回数据
	        
	        //计算结束的记录序号
	        if((begNum+num-1)>Integer.parseInt(s)) 
	        	i00=Integer.parseInt(s);
	        else
	        	i00=begNum+num-1;
        }
        
		try {
			connection=queueListServer.getConnection();
		    Channel channel= connection.createChannel();
		    
	        if(ret==0){//无返回记录
	            System.out.println("SendResultMSG.java run():没有更多的数据！");
	            //通过rabbitmq发送消息告知没有相应记录
	            String message="0"+mymanager.msgsplit+"0"+mymanager.msgsplit+"0";//表示执行结果总共0条记录，本次查询返回0条记录
	            channel.basicPublish("", msgQueue, null, message.getBytes());
				//关闭消息通道和连接
				channel.close();
				connection.close();
	            return ;
	        }
	        System.out.println("i0="+i0+"......"+"i00="+i00);
	        //从redis中取出指定数据，并返回给rabbitmq
	        ArrayList<String> messageArray=new ArrayList<String>();
	        //String message0="resultnum="+s+staticparameter.msgsplit+"recordnum="+(i00-i0+1);//表示执行结果总共S条记录，本次查询返回m条记录，分P个消息返回
	        String message0=s+mymanager.msgsplit+(i00-i0+1);//表示执行结果总共S条记录，本次查询返回m条记录，分P个消息返回
	        System.out.println("message0="+message0);
//	        int i=i0;
	        long i=i0;
	       
	        String message="";
	        String value="";
	        int ii=i0-1;// 记录处理到了第几记录，如果记录编号大于i00，则跳出循环
	        while(true){
	        	if(ii==i00){//如果记录数准备完毕，形成最后一条消息
//	         		if(message.length()>0)
//	        			messageArray.add(message+staticparameter.msgsplit+value);
//	        		else
//	         			messageArray.add(staticparameter.msgsplit+value);	
//	         		System.out.println("记录数已操作完毕 message="+value);
	        		messageArray.add(message);
	         		break;
	        	}

		        for(long j=i;j<=i00;j++)
		        {
		        	ii++;
//		        	value=memorydb.get(key+base.getNumber(j));
		        	value=memorydb.get(key+base.getLongNumber(j));
//		        	System.out.println("ii="+ii+"j="+j+"i="+i+"原始消息记录:"+key+base.getNumber(j)+":"+value);
		        	//组织消息
		        	if((message.length()+message.length())>mymanager.maxMessageLen){//如果消息大于一个长度，则开始组织一个新的消息
		        		messageArray.add(message);
		           		message=mymanager.msgsplit+value;
		        		i=++j;		        		
		        		break;
		        	}
		        	else{
		        		message=message+mymanager.msgsplit+value;
//		        		System.out.println(j+";else message="+message);
		        	}
		        }
	        }
	        
    		//将消息发送到rabbitmq
	        //message0=message0+staticparameter.msgsplit+"msgnum="+messageArray.size();//消息前缀：总共N条，返回m条，总共p页
	        message0=message0+mymanager.msgsplit+messageArray.size();//消息前缀：总共N条，返回m条，总共p页
	        //System.out.println("message0="+message0);
	        System.out.println("分为消息数目"+messageArray.size());
	        String sendmessage="";
	        for(int l=0;l<messageArray.size();l++){
	        	sendmessage=message0+messageArray.get(l);
	        	//System.out.println("messageArray="+messageArray.get(l));
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


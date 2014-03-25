package com.hualu.cloud.serverinterface.realtime;

import java.util.ArrayList;

import org.apache.log4j.Logger;

import com.ehl.itgs.interfaces.bean.QueryInfo;
import com.ehl.itgs.interfaces.bean.RTABean;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.hualu.cloud.basebase.staticparameter;
import com.hualu.cloud.db.memoryDB;
import com.hualu.cloud.serverinterface.mymanager;

public class CloudRTA {
	memoryDB memorydb=new memoryDB();
	public static Logger logger = Logger.getLogger(CloudRTA.class);
	Gson gson=new Gson();
	/**
	 * Description: 卡口通行车辆实时监控
	 * @param bean 监控参数
	 * @param queryInfo 请求信息
	 * 卡口监控  
	 * @return true：成功,false:失败
	 * @throws Exception
	 */
	//public boolean addTgs(RTABean bean,QueryInfo queryInfo) throws Exception{
	public boolean addTgs(String parameter) throws Exception{
		
		//将消息传来的参数字符创，恢复成参数对象
		String[] para=parameter.split(mymanager.parametersplit);
		RTABean bean=gson.fromJson(para[0], new TypeToken<RTABean>(){}.getType());
		QueryInfo queryInfo=new QueryInfo();
		queryInfo=gson.fromJson(para[1], new TypeToken<QueryInfo>(){}.getType());
		
		String key1,value1,key2,value2,tags,lanelist,orientationlist;
		String[] lanevalue;
		String[] orientationvalue;
	
	  
		   tags = bean.getTgs();
		   if(tags.length()<1) return false;
		   //orientation =  bean.getTravelOrientation().toString();
		   //lane =  bean.getLandIe().toString();
		    
			//获取监控卡口车道数组，并将之转化为字符串lane1,lane2,lane3...;
		     lanevalue=bean.getLandIe();
		     lanelist="";
				int j=0;
				if(lanevalue==null){
					lanelist="*";
				}else{
					for(j=0;j<lanevalue.length;j++){
						lanelist=lanelist+lanevalue[j]+",";
					}
				}
				//lanelist=lanelist+lanevalue[j];
				
				//获取监控卡口方向数组，并将之转化为字符串orientation1,orientation2,orientation3...;
				orientationvalue=bean.getTravelOrientation();
			     orientationlist="";
					int i=0;
					if(orientationvalue==null){
						orientationlist="*";
					}else{
						for(i=0;i<orientationvalue.length;i++){
							orientationlist=orientationlist+orientationvalue[i]+",";
						}
					}
					//orientationlist=orientationlist+orientationvalue[i];
					 key1 = "kk:"+tags;	
			
			/*
			//获取nodename节点名，一个主机可运行多个节点
			String nodename=staticparameter.getValue("nodename","node1");
			if(nodename==null){
					logger.info("addTgs():节点名为空，务必请配置节点名，为告警消息接收使用");
					nodename="null";
			}
			*/
			String valuelist=memorydb.get(key1);
			if(valuelist==null){
			  	value1 ="cd:"+lanelist+";fx:"+orientationlist+";sessionid:"+queryInfo.getSessionId().toString();
			  	key2 = "kk:"+queryInfo.getSessionId().toString();
			  	value2 = key1;
			  	if(memorydb.add(key1, value1)==null){
			  		logger.warn("往内存数据库添加记录返回值为空，请核查原因");
			  		return false;
			  	}
			  	if(memorydb.add(key2, value2)==null){
			  		logger.warn("往内存数据库添加记录返回值为空，请核查原因");
			  		return false;
			  	}
			}else{
				String[] list1=valuelist.split(";");
			  	value1 =valuelist+"@@"+"cd:"+lanelist+";fx:"+orientationlist+";sessionid:"+queryInfo.getSessionId().toString();
			  	key2 = "kk:"+queryInfo.getSessionId().toString();
			  	value2 = key1;
			  	if(memorydb.add(key1, value1)==null){
			  		logger.warn("往内存数据库添加记录返回值为空，请核查原因");
			  		return false;
			  	}
			  	if(memorydb.add(key2, value2)==null){
			  		logger.warn("往内存数据库添加记录返回值为空，请核查原因");
			  		return false;
			  	}
			}
		logger.debug("addTgs:key1="+key1+"sessionID:"+memorydb.get(key1));
		//System.out.println("carnumber:"+memorydb.get(key2));
		return true;
	}

	/**
	 *  特定车辆监控
	 * @param plateType 号牌类型
	 * @param plateNumber 号牌号码
	 * @param queryInfo 请求信息
	 * ts特殊车辆监控
	 * @return true：成功,false:失败
	 */
	//public boolean addCar(String plateType,String plateNumber,QueryInfo queryInfo) throws Exception{
	public boolean addCar(String parameter) throws Exception{
		
		//将消息传来的参数字符创，恢复成参数对象
		String[] para=parameter.split(mymanager.parametersplit);
		String plateType=gson.fromJson(para[0], new TypeToken<String>(){}.getType());
		if(plateType==null||plateType.length()<1) return false;
		String plateNumber=gson.fromJson(para[1], new TypeToken<String>(){}.getType());
		if(plateNumber==null||plateNumber.length()<1) return false;
		QueryInfo queryInfo=new QueryInfo();
		queryInfo=gson.fromJson(para[2], new TypeToken<QueryInfo>(){}.getType());
		
		String key1,value1,key2,value2,platetype,number;
	
		platetype = plateType;
		number = plateNumber;

		key2 = "ts:"+platetype+plateNumber;
	  	value1 = key2;
		String valuelist=memorydb.get(key2);
		if(valuelist==null){
			key1="ts:"+queryInfo.getSessionId().toString();
		  	value2 = "sessionid:"+queryInfo.getSessionId().toString();
		  	if(memorydb.add(key1, value1)==null){
		  		logger.warn("往内存数据库添加记录返回值为空，请核查原因");
		  		return false;
		  	}
		  	if(memorydb.add(key2, value2)==null){
		  		logger.warn("往内存数据库添加记录返回值为空，请核查原因");
		  		return false;
		  	}	  
		}else{
			key1="ts:"+queryInfo.getSessionId().toString();
		  	value2=valuelist+","+queryInfo.getSessionId().toString();
		  	if(memorydb.add(key1, value1)==null){
		  		logger.warn("往内存数据库添加记录返回值为空，请核查原因");
		  		return false;
		  	}
		  	if(memorydb.add(key2, value2)==null){
		  		logger.warn("往内存数据库添加记录返回值为空，请核查原因");
		  		return false;
		  	}  	
		}
		logger.debug("addCar:SessionID="+queryInfo.getSessionId());
		logger.info("addCar:key1="+key1+memorydb.get(key1));
		return true;		
	}
	
	/**
	 * Description: 预警
	 * @param warnType 预警类型
	 * @param bean 预警参数
	 * @param queryInfo 
	 * 预警类型yj
	 * @return true：成功,false:失败
	 * @throws Exception
	 */
	//public boolean addWarn(String[] warnType,RTABean[] bean,QueryInfo queryInfo) throws Exception{
	public boolean addWarn(String parameter) throws Exception{
		
		//将消息传来的参数字符创，恢复成参数对象
		String[] para=parameter.split(mymanager.parametersplit);
		String[] warnType=gson.fromJson(para[0], new TypeToken<String[]>(){}.getType());
		if(warnType.length<1) return false;
		RTABean[] bean=gson.fromJson(para[1], new TypeToken<RTABean[]>(){}.getType());
		QueryInfo queryInfo=new QueryInfo();
		queryInfo=gson.fromJson(para[2], new TypeToken<QueryInfo>(){}.getType());
		
		String[] lanevalue;
		String[] orientationvalue;
		String key1,value1,key2,value2,warnlist,lanelist,orientationlist,taglist;
		RTABean rtabean ;
		
		int warnlength  = warnType.length;
	    int rtalength;
	    warnlist = "";
	    for (int l= 0;l<warnlength; l++){
	    	warnlist = warnlist+ warnType[l]+",";	    	
	    }
	    rtalength = bean.length;
	    taglist = "";
	    for(int m=0;m<rtalength;m++){
	    	rtabean=bean[m];
	    	
	    	String tag = rtabean.getTgs();
	    	if(tag==null||tag.length()<1) return false;
	    	
	    	taglist=taglist+tag+";";
			//获取监控卡口车道数组，并将之转化为字符串lane1,lane2,lane3...;
		   	lanevalue=rtabean.getTravelOrientation();
		     lanelist="";
			if(lanevalue==null){
				lanelist="*";
			}else{
				for(int j=0;j<lanevalue.length;j++){
					lanelist=lanelist+lanevalue[j]+",";
				}
			}
				
				//获取监控卡口方向数组，并将之转化为字符串orientation1,orientation2,orientation3...;
				orientationvalue=rtabean.getTravelOrientation();
			     orientationlist="";
				if(orientationvalue==null){
					orientationlist="*";
				}else{
					for(int i=0;i<orientationvalue.length;i++){
						orientationlist=orientationlist+orientationvalue[i]+",";
					}
				}
					
		   // key1 ="yj:"+warnType[m]+";kk:"+taglist+";cd:"+lanelist+";fx:"+orientationlist;		
			key1 ="yj:"+tag;
			String valuelist=memorydb.get(key1);
			if(valuelist==null)
				value1 = "yjtype:"+ warnlist+";cd:"+lanelist+";fx:"+orientationlist+";sessionid:"+queryInfo.getSessionId().toString();
			else
				value1= valuelist+"@@"+"yjtype:"+ warnlist+";cd:"+lanelist+";fx:"+orientationlist+";sessionid:"+queryInfo.getSessionId().toString();
	       
		  	if(memorydb.add(key1, value1)==null){
		  		logger.warn("往内存数据库添加记录返回值为空，请核查原因");
		  		return false;
		  	}
		
			logger.debug(key1+","+memorydb.get(key1));
	    }	
	    key2 = "yj:"+queryInfo.getSessionId().toString();
        value2 = taglist;
      	if(memorydb.add(key2, value2)==null){
	  		logger.warn("往内存数据库添加记录返回值为空，请核查原因");
	  		return false;
	  	}
		logger.debug("addWarn:key1="+key2+";sessionID:"+memorydb.get(key2));
		return true;
	}
	
	/**
	 *  取消
	 * @param queryInfo 请求信息
	 * @return true：成功,false:失败
	 */
	//public boolean cancel(QueryInfo queryInfo) throws Exception{
	public boolean cancel(String parameter) throws Exception{
		//将消息传来的参数字符创，恢复成参数对象
		String[] para=parameter.split(mymanager.parametersplit);
		QueryInfo queryInfo=new QueryInfo();
		queryInfo=gson.fromJson(para[0], new TypeToken<QueryInfo>(){}.getType());
		
		String key1,key2,key3,value1,value2,value3,sessionid;
		sessionid=queryInfo.getSessionId().toString();
		key1 = "kk:"+sessionid;
		key2 = "ts:"+sessionid;
		key3 = "yj:"+sessionid;
		
		//删除布控卡口的数据
		if(key1==null)
			return false;
		if(key2==null)
			return false;
		if(key3==null)
			return false;
		value1 = memorydb.get(key1);
		if(value1!=null){
			logger.debug("key1:"+memorydb.get(key1));	
			//根据布控编号查到车牌号的键值对进行删除
			if(memorydb.del(value1)<0){//根据布控编号删除键值对
				logger.warn("从内存数据库del数据返回值<0，请核查原因");
				return false;
			}
			//根据布控编号查到车牌号的键值对进行删除
			if(memorydb.del(key1)<0){//根据布控编号删除键值对
				logger.warn("从内存数据库del数据返回值<0，请核查原因");
				return false;
			}
			logger.debug("kk:"+memorydb.get(value1));
			logger.debug("sessionid:"+memorydb.get(key1));
		}
		else {
			//删除特殊车辆布控的数据(cd:***;fx:***;sessionid:****@cd:***;fx:***,sessionid:***;)
			value2 = memorydb.get(key2);
		    if(value2!=null){
			    logger.debug("key2:"+memorydb.get(key2));
			    //根据特殊车辆编号查到对应的键值对进行删除
			    //判断该特殊车辆号码对应了几个session
			    String list=memorydb.get(value2);
			    if(list==null);
			    else{
		    		list=list.replaceAll(("@"+sessionid+"@"), "@");
			    	list=list.replaceAll((sessionid+"@"), "");
			    	list=list.replaceAll(("@"+sessionid), "");
			    	list=list.replaceAll(sessionid, "");
			    	if(list.length()<11)
						if(memorydb.del(value2)<0){
							logger.warn("从内存数据库del数据返回值<0，请核查原因");
							return false;
						}
			    }			    
				//根据布控编号查到车牌号的键值对进行删除
				if(memorydb.del(key2)<0){//根据布控编号删除键值对
					logger.warn("从内存数据库del数据返回值<0，请核查原因");
					return false;
				}
				logger.debug("ts:"+memorydb.get(value2));
				logger.debug("sessionid:"+memorydb.get(key2));
		    }
		    else{
				//删除预警的数据
				logger.debug("key3:"+memorydb.get(key3));
				value3 = memorydb.get(key3);
				if(value3!=null){
					String[] warnlist = value3.split(";");
					System.out.println("warnlist lentgth:" +warnlist.length);
					for(int i=0;i<warnlist.length;i++)
					{
						//根据预警编号逐一查到对应的键值对进行删除
					   if(warnlist[i]==null)
						   ;
					   else{
						   System.out.println(i);
						   if(memorydb.del("yj:"+warnlist[i])<0){
							   logger.warn("从内存数据库del数据返回值<0，请核查原因");
							   return false;
						   }
						   logger.debug(warnlist[i]);
						   logger.debug("yjtag:"+memorydb.get("yj:"+warnlist[i]));
					   }
					}
					//memorydb.del(memorydb.get(key3));
					//根据预警编号查到对应的键值对进行删除
					if(key3==null)
						;
					else
						if(memorydb.del(key3)<0){
							logger.warn("从内存数据库del数据返回值<0，请核查原因");
							return false;
						}
					logger.debug("sessionid="+key3);
				}
			}
		}
		return true;
	}

}

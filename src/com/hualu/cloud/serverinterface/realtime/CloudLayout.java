package com.hualu.cloud.serverinterface.realtime;

import java.net.InetAddress;
import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;

import org.apache.log4j.Logger;

import com.ehl.itgs.interfaces.LayoutInterface;
import com.ehl.itgs.interfaces.bean.Layout;
import com.ehl.itgs.interfaces.bean.LayoutList;
import com.ehl.itgs.interfaces.bean.QueryInfo;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.hualu.cloud.basebase.staticparameter;
import com.hualu.cloud.db.memoryDB;
import com.hualu.cloud.serverinterface.mymanager;
import com.hualu.cloud.serverinterface.offline.base;

/**
 * 
 *  布控（公开性布控及私密性布控）、违法未处理 接口
 */
public class CloudLayout{
	Gson gson=new Gson();
	public static Logger logger = Logger.getLogger(CloudLayout.class);
	public memoryDB memorydb=new memoryDB();
	
	/**
	 * 
	 *  布控：key分为HPW前缀表示按照号牌号码和号牌类型比对告警，TSGW前缀表示按照卡口
	 *  1个布控编号对应一个车牌号。但一个车牌号可能对应多个布控编号。
	 * @param bean 布控条件集合
	 * @return true：成功,false:失败
	 * 特别说明：
	 * 2014年元月7日跟小高确认过:车牌类型必须指定，不会指定会全部的。车牌号对多两位为通配符，英文下划线表示。
	 * 如果没有通配符：key1=BKBH:bkbhid value1=GJ:platetypeplatenumber
	 * 				  key2=GJ:platetypeplatenumber value2=BKBH:bkbhid1@tgs=id1,tgs=id2...;BKBH:bkbhid2@tgs=id1,tgs=id2...(保证一个车牌可对应多个布控)
	 * 如果有通配符：key1保持不变，另再增加两类值
	 * 如果有一个通配符：key3=GJMH1	value3=1;GJ:platetypeplatenumber;BKBH:bkbhid;tgs=...@1;GJ:platetypeplatenumber@@5;GJ:platetypeplatenumber;BKBH:bkbhid;tgs=...
	 * 上面表示，第一位为通配符的有两条记录，第五位为通配符的有一条记录
	 * key4=GJMH2	value3=1,3;GJ:platetypeplatenumber;BKBH:bkbhid;tgs=...@3,6;GJ:platetypeplatenumber;BKBH:bkbhid;
	 * 上面表示，1、3为通配符的记录有一条，3、6为通配符的记录有一条。
	 * 	取消时根据BKBH和value值进行取消			
	 */
	public boolean layout(String parameter) throws Exception {
		if(parameter==null||parameter==""){
			logger.error("开始布控参数为空");
			return false;
		}else{
			logger.info("开始布控:"+parameter);
		
			//将消息传来的参数字符创，恢复成参数对象
			String[] para=parameter.split(mymanager.parametersplit);
			QueryInfo queryInfo=new QueryInfo();
			queryInfo=gson.fromJson(para[1], new TypeToken<QueryInfo>(){}.getType());
			ArrayList<Layout> bean=new ArrayList<Layout>();
			bean=gson.fromJson(para[0], new TypeToken<ArrayList<Layout>>(){}.getType());
			
			String key1,value1,key2,value2,bkendtime,tgslist;
			String[] value;
			Layout layout;
			int jzsj;
			
			int i0=bean.size();
			for(int i=0;i<i0;i++){
				layout=bean.get(i);
				key1="BKBH:"+layout.getLayoutId();
				value1="GJ:"+layout.getPlateType()+layout.getPlateNumber();
				key2=value1;
				
				//获取监控范围：卡口数组，并将之转化为字符串tgs1,tgs2,tgs3...;
				value=layout.getTgs();
				tgslist="";
				if(value==null)
					tgslist="tgs=*";
				else{
					int j=0;
					for(j=0;j<value.length;j++){
						tgslist=tgslist+"tgs="+value[j]+",";
					}
				}
				
				String value3=memorydb.get(key2);//该号码原有的监控
				if(value3==null)
					value2=key1+"@"+tgslist;//该号码之前没有被监控
				else
					value2=value3+";"+key1+"@"+tgslist;//该号码之前有部门在监控
				
				if(memorydb.add(key1, value1)==null) {
					logger.warn("往内存数据库添加记录返回值为空，请核查原因");
					return false;
				}
				int pos1=value1.indexOf("_");
				if(pos1>=0){
					int pos2=value1.indexOf("_", pos1+1);
					if(pos1>=0 && pos2>0){//两个通配符
						key2="GJM2";
						value2=memorydb.get(key2);
						if(value2==null)
							value2=String.valueOf(pos1)+","+String.valueOf(pos2)+";"+value1+";"+key1+";"+tgslist;
						else
							value2=value2+"@"+String.valueOf(pos1)+","+String.valueOf(pos2)+";"+value1+";"+key1+";"+tgslist;
					}else{//一个通配符
						key2="GJM1";
						value2=memorydb.get(key2);
						if(value2==null)
							value2=String.valueOf(pos1)+";"+value1+";"+key1+";"+tgslist;
						else
							value2=value2+"@"+String.valueOf(pos1)+";"+value1+";"+key1+";"+tgslist;
					}					
				}
				if(memorydb.add(key2, value2)==null) {
					logger.warn("往内存数据库添加记录返回值为空，请核查原因");
					return false;		
				}
				
				logger.info("key1:"+key1+";"+memorydb.get(key1));
				logger.info("key2:"+key2+";"+memorydb.get(key2));
			}
			return true;
		}
	}


	//该方法只处理无通配符的情况，目前已不适用该方法
	//public boolean layout(ArrayList<Layout> bean,QueryInfo queryInfo) throws Exception {
	public boolean layoutno(String parameter) throws Exception {
		logger.info("开始布控");
		
		//将消息传来的参数字符创，恢复成参数对象
		String[] para=parameter.split(mymanager.parametersplit);
		QueryInfo queryInfo=new QueryInfo();
		queryInfo=gson.fromJson(para[1], new TypeToken<QueryInfo>(){}.getType());
		ArrayList<Layout> bean=new ArrayList<Layout>();
		bean=gson.fromJson(para[0], new TypeToken<ArrayList<Layout>>(){}.getType());
		
		String key1,value1,key2,value2,bkendtime,tgslist;
		String[] value;
		Layout layout;
		int jzsj;
		
		int i0=bean.size();
		for(int i=0;i<i0;i++){
			layout=bean.get(i);
			key1="BKBH:"+layout.getLayoutId();
			value1="GJ:"+layout.getPlateType()+layout.getPlateNumber();
			key2=value1;
			
			//获取监控范围：卡口数组，并将之转化为字符串tgs1,tgs2,tgs3...;
			value=layout.getTgs();
			tgslist="";
			if(value==null)
				tgslist="tgs=*";
			else{
				int j=0;
				for(j=0;j<value.length;j++){
					tgslist=tgslist+"tgs="+value[j]+",";
				}
			}
			
			String value3=memorydb.get(key2);//该号码原有的监控
			if(value3==null)
				value2=key1+"@"+tgslist;//该号码之前没有被监控
			else
				value2=value3+";"+key1+"@"+tgslist;//该号码之前有部门在监控
			
			memorydb.add(key1, value1);
			memorydb.add(key2, value2);
			
			
			logger.debug("key1:"+key1+";"+memorydb.get(key1));
			logger.debug("key2:"+key2+";"+memorydb.get(key2));
		}
		return true;
	}


	
	/**
	 * 
	 *  撤控
	 * @param bean 布控条件集合
	 * @return true：成功,false:失败
	 */
	//可处理有通配符的布控报警的取消
	public boolean cancelLayout(String parameter) throws Exception{
		String key1,key2;
		Layout layout;
		
		//将消息传来的参数字符创，恢复成参数对象
		String[] para=parameter.split(mymanager.parametersplit);
		QueryInfo queryInfo=new QueryInfo();
		queryInfo=gson.fromJson(para[1], new TypeToken<QueryInfo>(){}.getType());
		ArrayList<Layout> bean=new ArrayList<Layout>();
		bean=gson.fromJson(para[0], new TypeToken<ArrayList<Layout>>(){}.getType());
		
		int i0=bean.size();
		logger.info("cancelLayout 撤控记录条数："+i0);
		for(int i=0;i<i0;i++){
			layout=bean.get(i);
			/*
			第一个key是BKBH:布控编号,value是车牌类型+车牌种类，主要是便于撤控操作。
			第二个key为 GJ:号牌种类+号牌号码，value是TGS1,tgs2...tgsn;车辆信息+案件信息+布控信息
			*/
			key1="BKBH:"+layout.getLayoutId();
			key2="GJ:"+layout.getPlateType()+layout.getPlateNumber();
			
			String value1=memorydb.get(key1);//其实是GJ：车牌类型车牌号码”
			if(value1==null){//根据布控编号查询为空，表示该布控不存在
				logger.info(key1+" 该布控编号不存在!");
			}
			else{//布控编号查询不为空，则将要对得到的结果进行剔除或者删除处理
				//判断value1中是否有通配符
				int pos1=value1.indexOf("_");
				if(pos1<0){//无通配符
					logger.info(key1+" 无通配符撤控! "+key2);
					String value2=memorydb.get(value1);
					String value3="";
					if(value2==null) ;//如果结果为空，表示GJ布控记录不存在
					else{//如果结果不为空，则看是满足剔除条件，还是删除条件
						String[] list1=value2.split(";");//普通布控，多个记录间是用分号分割的。
						logger.info("无通配符撤控记录个数："+list1.length);
						if(list1.length==1)//只有一个记录，则直接删除
							if(memorydb.del(value1)<0){
								logger.warn("往内存数据库删除记录返回值小于零，请核查原因");
								return false;
							}
						else{//有GJ布控的记录中有多条信息，则需要剔除
							for(int j=0;j<list1.length;j++){
								if(list1[j].indexOf(key1)<0){
									if(value3.length()<1)
										value3=list1[j];
									else
										value3=value3+";"+list1[j];
								}
							}
							if(memorydb.add(value1,value3)==null){//相当于覆盖
								logger.warn("往内存数据库添加记录返回值为空，请核查原因");
								return false;
							}
						}
					}
				}else{//有通配符
					int pos2=value1.indexOf("_", pos1+1);
					if(pos2<0){//有一个通配符
						logger.info(key1+" 一个通配符撤控! "+key2);
						String value3=memorydb.get("GJM1");
						String valuelist="";
						if(value3!=null){
							String[] ss=value3.split("@");
							for(int ii=0;ii<ss.length;ii++){
								System.out.println(ss[ii]);
								System.out.println(value1+";"+key1);
								if(ss[ii].indexOf(value1+";"+key1)<0){
									if(valuelist.length()<1)
										valuelist=ss[ii];
									else
										valuelist=valuelist+"@"+ss[ii];
								}
							}
							if(valuelist.length()<1){
								if(memorydb.del("GJM1")<0){
									logger.warn("往内存数据库del记录返回值<0，请核查原因");
									return false;
								}
							}else{
								if(memorydb.add("GJM1", valuelist)==null){
									logger.warn("往内存数据库添加记录返回值为空，请核查原因");
									return false;
								}
							}
						}
					}else{//有两个通配符
						logger.info(key1+" 两个通配符撤控! "+key2);
						String value3=memorydb.get("GJM2");
						String valuelist="";
						if(value3!=null){
							String[] ss=value3.split("@");
							for(int ii=0;ii<ss.length;ii++){
								logger.info(ss[ii]);
								logger.info(value1+";"+key1);
								if(ss[ii].indexOf(value1+";"+key1)<0){
									if(valuelist.length()<1)
										valuelist=ss[ii];
									else
										valuelist=valuelist+"@"+ss[ii];
								}
							}
							if(valuelist.length()<1){
								if(memorydb.del("GJM2")<0){
									logger.warn("往内存数据库del记录返回值<0，请核查原因");
									return false;
								}
							}else{
								if(memorydb.add("GJM2", valuelist)==null){
									logger.warn("往内存数据库添加记录返回值为空，请核查原因");
									return false;
								}
							}
						}
					}
				}					
				if(memorydb.del(key1)<0){//根据布控编号删除键值对
					logger.warn("往内存数据库del记录返回值<0，请核查原因");
					return false;
				}
			}//else			
		}//for
		return true;
	}
	//只处理无通配符的撤销,目前已不适用该方法
	//public boolean cancelLayout(ArrayList<Layout> bean,QueryInfo queryInfo) throws Exception{
	public boolean cancelLayoutno(String parameter) throws Exception{
		String key1,key2;
		Layout layout;
		
		//将消息传来的参数字符创，恢复成参数对象
		String[] para=parameter.split(mymanager.parametersplit);
		QueryInfo queryInfo=new QueryInfo();
		queryInfo=gson.fromJson(para[1], new TypeToken<QueryInfo>(){}.getType());
		ArrayList<Layout> bean=new ArrayList<Layout>();
		bean=gson.fromJson(para[0], new TypeToken<ArrayList<Layout>>(){}.getType());
		
		int i0=bean.size();
		for(int i=0;i<i0;i++){
			layout=bean.get(i);
			/*
			第一个key是BKBH:布控编号,value是车牌类型+车牌种类，主要是便于撤控操作。
			第二个key为 GJ:号牌种类+号牌号码，value是TGS1,tgs2...tgsn;车辆信息+案件信息+布控信息
			*/
			key1="BKBH:"+layout.getLayoutId();
			key2="GJ:"+layout.getPlateType()+layout.getPlateNumber();
			
			memorydb.del(memorydb.get(key1));//根据布控编号查到车牌号的键值对进行删除
			String value1=memorydb.get(key1);//其实是“GJ：车牌类型车牌号码”
			if(value1==null){//根据布控编号查询为空，表示该布控不存在
				logger.info(key1+" 该布控编号不存在!");
			}
			else{//根据布控编号查询不为空，则将要对得到的结果进行剔除或者删除处理
				String value2=memorydb.get(value1);
				String value3="";
				if(value2==null) ;//如果结果为空，表示GJ布控记录不存在
				else{//如果结果不为空，则看是满足剔除条件，还是删除条件
					String[] list1=value2.split(";");
					if(list1.length==1)//只有一个记录，则直接删除
						memorydb.del(value2);
					else{//有GJ布控的记录中有多条信息，则需要剔除
						for(int j=0;j<list1.length;j++){
							if(list1[j].indexOf(key1)>=0);
							else
								if(value3=="")
									value2=list1[j];
								else
									value3=value3+";"+list1[j];
						}
						memorydb.add(value1,value3);//相当于覆盖
					}
				}
				memorydb.del(key1);//根据布控编号删除键值对
			}//else			
		}//for
		return true;
	}
	
	/**
	 * 
	 *  布控：key分为HPW前缀表示按照号牌号码和号牌类型比对告警，TSGW前缀表示按照卡口
	 * @param bean 布控条件集合
	 * @return true：成功,false:失败
	 */
	//public boolean layout(ArrayList<Layout> bean,QueryInfo queryInfo) throws Exception {
	public boolean layout0(String parameter) throws Exception {
		logger.info("开始布控");
		
		//将消息传来的参数字符创，恢复成参数对象
		String[] para=parameter.split(mymanager.parametersplit);
		QueryInfo queryInfo=new QueryInfo();
		queryInfo=gson.fromJson(para[1], new TypeToken<QueryInfo>(){}.getType());
		ArrayList<Layout> bean=new ArrayList<Layout>();
		bean=gson.fromJson(para[0], new TypeToken<ArrayList<Layout>>(){}.getType());
		
		String key1,value1,key2,value2,bkendtime,tgslist;
		String[] value;
		Layout layout;
		int jzsj;
		
		int i0=bean.size();
		for(int i=0;i<i0;i++){
			layout=bean.get(i);
			logger.debug("BeginTime="+layout.getBegTime()+" EndTime="+layout.getEndTime());
			System.out.println("BeginTime="+layout.getBegTime()+" EndTime="+layout.getEndTime());
			System.out.println("BKBH:"+layout.getLayoutId()+";   GJ:"+layout.getPlateType()+layout.getPlateNumber());
			key1="BKBH:"+layout.getLayoutId();
			value1="GJ:"+layout.getPlateType()+layout.getPlateNumber();
			key2=value1;
			
			//获取监控范围：卡口数组，并将之转化为字符串tgs1,tgs2,tgs3...;
			value=layout.getTgs();
			tgslist="";
			if(value==null)
				tgslist="tgs=*";
			else{
				int j=0;
				for(j=0;j<value.length-1;j++){
					tgslist=tgslist+"tgs="+value[j]+",";
				}
				tgslist=tgslist+"tgs="+value[j];
			}
			
			//获取布控时限
			jzsj=0;
			//bkstarttime=layout.getBegTime();//yyyy-mm-dd hh24:mi:ss
			bkendtime=layout.getEndTime();//yyyy-mm-dd hh24:mi:ss
			if(bkendtime==null){
				logger.info("layout endtime is null,so layout interval set 1hour");
				jzsj=3600000;
			}else{
				if(bkendtime.length()==19){//如果布控时限正常
					long start=System.currentTimeMillis();
					long end=getTimelong(bkendtime);
					long interval=end-start;
					if(interval<0)
						interval=3600000;//如果endtime小与当前时间，则有效时间设为1小时
					//把毫秒变成秒
					jzsj=Integer.parseInt(Long.toString(interval/1000));//获取改时间与当前系统时间的差值（秒）
				}else{
					logger.info("layout endtime is not valid,so layout interval set 1hour");
					jzsj=3600000;
				}
			}
			logger.debug("bkendtime="+bkendtime+",bukonginternal(second)="+jzsj);

			//获取nodename节点名，一个主机可运行多个节点
			String nodename=staticparameter.getValue("nodename","node1");
			if(nodename==null){
				logger.debug("layout():节点名为空，务必请配置节点名，为告警消息接收使用");
				nodename="null";
			}
			//value2="TGS:"+tgslist+";"+key1+";SessionID:"+queryInfo.getSessionId()+";nodename:"+nodename;
			value2="TGS:"+tgslist+";"+key1;//TGS:**;BKBH:**
			if((value1.length()>6)&&(value1.indexOf("*")<0)&&(value1.indexOf("%")<0)){//济南的401条布控，只有3条是5个字符，其他都是7个字符的完整车牌
				//如果车牌类型和车牌明确，在redis数据库中插入两条记录
				//第一个key是BKBH:布控编号,value是 GJ:号牌种类+号牌号码，主要是便于按照布控编号进行撤控。
				//第二个key为 GJ:号牌种类+号牌号码，value是TGS1=tgs2...tgsn;BKBH=**;SessionID=***;

				logger.debug(key1+":"+value1+";"+key2+":"+value2);


				//存入内存数据库，并根据布控时限设置好数据失效时间
				memorydb.add(key1, value1);
				memorydb.add(key2, value2);
				if(jzsj>0){//如果布控时限正常
					memorydb.expire(key1, jzsj);
					memorydb.expire(key2, jzsj);

				}else{//如果布控时限不对，则不设置过期时间
					logger.info("布控时限不对，则不设置过期时间");
				}

				logger.debug("key1:"+key1+";"+memorydb.get(key1));
				logger.debug("key2:"+key2+";"+memorydb.get(key2));
			}
			else{
				/*
				如果车牌类型和车牌不明确，在需要在redis数据库中插入多条记录
				第一个key是BKBH:布控编号,value是告警模糊民用 GJMHMY:号牌种类+号牌号码，主要是便于按照布控编号进行撤控。
				第二个到第N个key为 GJMHMY:号牌种类+号牌号码，value是TGS1,tgs2...tgsn;车辆信息+案件信息+布控信息
				*/
				logger.info("车牌不明确，不予处理");
				//把模糊车牌变成固定车牌号码进行:
				// 布控编号和和模糊车牌列表信息
				//布控信息存入内存数据库
			}
		}
		return true;
	}

	
	/**
	 * 
	 *  撤控
	 * @param bean 布控条件集合
	 * @return true：成功,false:失败
	 */
	//public boolean cancelLayout(ArrayList<Layout> bean,QueryInfo queryInfo) throws Exception{
	public boolean cancelLayout0(String parameter) throws Exception{
		String key1,key2;
		Layout layout;
		
		//将消息传来的参数字符创，恢复成参数对象
		String[] para=parameter.split(mymanager.parametersplit);
		QueryInfo queryInfo=new QueryInfo();
		queryInfo=gson.fromJson(para[1], new TypeToken<QueryInfo>(){}.getType());
		ArrayList<Layout> bean=new ArrayList<Layout>();
		bean=gson.fromJson(para[0], new TypeToken<ArrayList<Layout>>(){}.getType());
		
		int i0=bean.size();
		for(int i=0;i<i0;i++){
			layout=bean.get(i);
			/*
			第一个key是BKBH:布控编号,value是车牌类型+车牌种类，主要是便于撤控操作。
			第二个key为 GJ:号牌种类+号牌号码，value是TGS1,tgs2...tgsn;车辆信息+案件信息+布控信息
			*/
			key1="BKBH:"+layout.getLayoutId();
			key2="GJ:"+layout.getPlateType()+layout.getPlateNumber();
			
			memorydb.del(memorydb.get(key1));//根据布控编号查到车牌号的键值对进行删除
			memorydb.del(key1);//根据布控编号删除键值对
		}
		return true;
	}
	
	/**
	 * 
	 *  违法未处理
	 * @return true：成功,false:失败
	 */
	public boolean illegalUntreated() throws Exception{
		return false;
	}
	
	/**
	 * 
	 *  撤销违法未处理
	 * @return true：成功,false:失败
	 */
	public boolean cancelIllegalUntreated() throws Exception{
		return false;
	}
	
	/**
	 * 
	 *  清除违法未处理
	 * @return true：成功,false:失败
	 */
	public boolean clearIllegalUntreated() throws Exception{
		return false;
	}
	
	public String currentTime(){
        Date date=new Date(System.currentTimeMillis()); 
        SimpleDateFormat format=new SimpleDateFormat("yyyyMMddHHmmss"); 
        return format.format(date); 
    }
	
	public long getTimelong(String time){//enddate的格式为yyyy-mm-dd hh24:mi:ss
		long s=0;
		
		 try {
			//根据字符串time得到毫秒数。
			s = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse(time).getTime();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		 
		 return s;
	}
}

package com.hualu.cloud.serverinterface.offline;

import java.util.ArrayList;
import java.util.Iterator;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.ehl.itgs.interfaces.bean.analysis.Multipoint;
import com.ehl.itgs.interfaces.bean.BackResult;
import com.ehl.itgs.interfaces.bean.QueryCar;
import com.ehl.itgs.interfaces.bean.QueryInfo;
import com.ehl.itgs.interfaces.bean.ResultCar;
import com.hualu.cloud.basebase.staticparameter;
import com.hualu.cloud.db.memoryDB;

public class multipointquery {
	memoryDB memorydb=new memoryDB();
	static String photoURL="http://10.2.111.106/photo";
	private static Configuration config = null;
	private static HTablePool tp = null;
	static int expireInterval;
	static ResultScanner scanner;
	static HTableInterface table;
	static Scan scan;
	static {
		// 加载集群配置
		config = HBaseConfiguration.create();
		// 创建表池
		tp = new HTablePool(config, 10);
		// System.out.println(config.get("hbase.zookeeper.quorum"));
		// 指定表
		table = getTable("flow");
	}

    /**
     * 构造函数：初始化一些信息包括图片访问地址等
     */
    public multipointquery(){
    	photoURL=staticparameter.getValue("photoURL","http://172.16.100.51:8888/n/0/0/");
    	expireInterval=staticparameter.getIntValue("expireInterval",86400);
    	base.printMsg("photoURL=",photoURL);
    	base.printMsg("expireInterval=",Integer.toString(expireInterval));
    }
	
	/**
	 * 
	 * @param tableName
	 *            表名
	 * @return 返回htableinterface对表进行操作
	 */
	public static HTableInterface getTable(String tableName) {
		if (StringUtils.isEmpty(tableName))
			return null;
		return tp.getTable(tableName.getBytes());
	}

    public int getFromHBase(ArrayList<Multipoint> multipoint,QueryInfo queryInfo)throws Exception{
        Configuration conf = HBaseConfiguration.create();
        HTable table = new HTable(conf, "flow");
        
        Scan scan = new Scan();
        
        String key =getKey(multipoint);
        ArrayList<String> carList1=new ArrayList<String>();//本次查询的车牌号
        ArrayList<String> carList2=new ArrayList<String>();//上次与本次的交集车牌号
         
           
        //第一个tgs的查询结果集作为第一次交集结果
    	//依次用第n次交集结果比较第n+1个tgs的查询结果集取交集。
    	//最终结果就是结果车牌号。
        for(int i=0;i<multipoint.size();i++){
        	Multipoint m1=multipoint.get(i);
        	ArrayList a = m1.getTgs();
        	String begTime=base.changeTimeStyle(m1.getBegTime());
        	String endTime=base.changeTimeStyle(m1.getEndTime());
            for(int j=0;j<a.size();j++){
             	if((i==0)&&(j==0)){//取第一个tgs的查询结果
                	String tgs=(String)(a.get(j));
                    String begin = tgs + "," + begTime;
                    String end = tgs + "," + endTime;
                   
                    scan.setStartRow(Bytes.toBytes(begin));
                    scan.setStopRow(Bytes.toBytes(end));
                    ResultScanner r = table.getScanner(scan);
                    while(true){
                    	Result rr=r.next();
                    	if(rr==null) break;
                    	String rowkey=new String(rr.getRow());
                        String result = rowkey; 
                        //第一个tgs的查询结果集作为第一次交集结果
                        carList2.add(result);
                    }
                }else{//去本次与上一个交集结果进行比较
                	carList1.clear();
                	String tgs=(String)(a.get(j));
                    String begin = tgs + "," + begTime;
                    String end = tgs + "," + endTime;
                   
                    scan.setStartRow(Bytes.toBytes(begin));
                    scan.setStopRow(Bytes.toBytes(end));
                    ResultScanner r = table.getScanner(scan);
                    while(true){//获取该时间段内通过该卡口的车牌号
                    	Result rr=r.next();
                    	if(rr==null) break;
                    	String rowkey=new String(rr.getRow());
                        String result = rowkey; 
                        //第一个tgs的查询结果集作为第一次交集结果
                        carList1.add(result);
                    }
                    //将车牌号与上次交集结果进行比较
                    carList2=base.intersection1(carList2,carList1);
                }
            }
        }
               
        //如果carList2不为空，则从数据库中根据car从HBase中取出满足要求的过车记录,并将结果写入内存数据库
        ArrayList<String> resultList=new ArrayList<String>();//本次查询的结果集
        int count=0;
        if(carList2.size()>0){
        	for(count=0;count<carList2.size();count++){
        		Get getscan = new Get(carList2.get(count).getBytes());// 根据rowkey查询   
	        	Result rr = table.get(getscan);  
	        	String column=carList2.get(count)
	        	+ "," + new String(rr.getValue(Bytes.toBytes("flowfamily"),Bytes.toBytes("CARSTATE")))
                + "," + new String(rr.getValue(Bytes.toBytes("flowfamily"),Bytes.toBytes("SPEED")))
                + "," + new String(rr.getValue(Bytes.toBytes("flowfamily"),Bytes.toBytes("PLATECOLOR")))
                + "," + new String(rr.getValue(Bytes.toBytes("flowfamily"),Bytes.toBytes("LOCATIONID")))
                + "," + new String(rr.getValue(Bytes.toBytes("flowfamily"),Bytes.toBytes("DRIVEWAY")))
                + "," + new String(rr.getValue(Bytes.toBytes("flowfamily"),Bytes.toBytes("CAPTUREDIR")))
                + "," + new String(rr.getValue(Bytes.toBytes("flowfamily"),Bytes.toBytes("CARCOLOR")))
                + "," + new String(rr.getValue(Bytes.toBytes("flowfamily"),Bytes.toBytes("CARBRAND")));;
	        	resultList.add(column);
	        	//System.out.println(column);
        	}
        	//结果按时间进行排序
        	ArrayList<String> newList=(ArrayList<String>) base.sort1(resultList);
        	
        	//存入内存数据库作为缓存
            Iterator iterator0 = newList.iterator();
            String keynum;
            int m=1;//序号从1开始
            while(iterator0.hasNext())
            {
                String element = (String) iterator0.next();
                keynum= key + base.getNumber(m);
                System.out.println("keynum="+keynum+" value="+element);
                memorydb.add(keynum,element);
                memorydb.expire(keynum,expireInterval);
                System.out.println(memorydb.get(keynum));
                m++;            
            }
           
            memorydb.add(key,count + "");
            memorydb.expire(key, expireInterval);
            System.out.println("The total number is:" + count);
            m = -1;
            
        }else{//如果carList2为空，则内存数据库结果总数为0
            memorydb.add(key,"0");
            memorydb.expire(key, expireInterval);
            count=0;
        }
        	
        //并将满足条件的记录数返回
        return count;
    }
	
    public BackResult<ResultCar> getFromMemoryDB(ArrayList<Multipoint> multipoint, int begNum, int num,
			QueryInfo queryInfo) throws Exception
            {  
                String key =getKey(multipoint);
                String s = memorydb.get(key);
                System.out.println("key="+key+"  "+"value="+s);
                
                ResultCar rc = new ResultCar();
                BackResult br = new BackResult();
                ArrayList<ResultCar> al = new ArrayList<ResultCar>();
                
                int m=0;

                if (s==null){
                    getFromHBase(multipoint,queryInfo);
                    s = memorydb.get(key);
                    m = base.getCount(Integer.parseInt(s),begNum,num);
                }else{
                    m = base.getCount(Integer.parseInt(s),begNum,num);
                    if(m==0){
	                	System.out.println("没有更多的数据！");
	                	return null;
                    }
                }

                    for(int i = begNum ;i < begNum + m;i++)
                    {
                        base.printMsg("from redis: ", base.getNumber(i) + " " + memorydb.get(key + base.getNumber(i)));
                        String split[] = memorydb.get(key + base.getNumber(i)).split(",");

                        rc.setTgs(split[0]);
                        rc.setTimeStamp(split[1]);
                        rc.setPlateType(split[2]);
                        rc.setPlateNumber(split[3]);
                        
                        rc.setCarState(split[4]);
                        rc.setSpeed(split[5]);
                        rc.setPlateColor(split[6]);
                        rc.setLoncatonId(split[7]);
                        rc.setTravelOrientation(Integer.parseInt(split[8]));
                        rc.setCaptureDir(split[9]);
                        rc.setCarColor(split[10]);
                        rc.setCarBrand(split[11]);

                        String[] url={""};
                        url[0]=photoURL+split[0]+","+split[1]+","+split[2]+","+split[3];
                        rc.setImgUrls(url);
                        al.add(rc);
                    }
                    br.setBeans(al);
                    br.setTotalNum(Integer.parseInt(s));
                    System.out.println("本次返回条数="+m);
                    String s2 = memorydb.get(key);
                    base.printMsg(key+":", s2);
                    return br;

            }

	
    /**
     * 根据输入参数组织Key：车牌号码+车牌类型字符创+开始时间+结束时间
     * @param multipoint
     * @return tgslist+begtime+endtime组织成的key
     */
    public static String getKey(ArrayList<Multipoint> multipoint)
    {
        String key="";
        for(int i=0;i<multipoint.size();i++){
        	Multipoint m1=multipoint.get(i);
	    	ArrayList a = m1.getTgs();
	    	
	        for(int j=0;j<a.size();j++){
	        	key+=(String)(a.get(j));
	        }
	        String  begtime= m1.getBegTime();
	    	begtime=base.changeTimeStyle(begtime);
	    	key+=begtime;
	
	        String  endtime= m1.getEndTime();
	    	endtime=base.changeTimeStyle(endtime);
	    	key+=endtime;
        }
        
        return key;
     }
}

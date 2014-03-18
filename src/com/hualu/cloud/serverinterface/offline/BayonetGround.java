package com.hualu.cloud.serverinterface.offline;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.ehl.itgs.interfaces.bean.BackResult;
import com.ehl.itgs.interfaces.bean.QueryInfo;
import com.ehl.itgs.interfaces.bean.ResultCar;
import com.ehl.itgs.interfaces.bean.analysis.LocusCarResult;
import com.ehl.itgs.interfaces.bean.analysis.PassTgsInfo;
import com.ehl.itgs.interfaces.bean.analysis.TheGround;
import com.ehl.itgs.interfaces.bean.analysis.TheGround.GroundTGS;
import com.hualu.cloud.basebase.staticparameter;
import com.hualu.cloud.db.memoryDB;

public class BayonetGround {
    
    static int count = 0;
    static String key ="";
    static int m = 1;
    static String photoURL="http://10.2.111.106/photo";
    static int expireInterval=86400;
    static memoryDB memorydb=new memoryDB();
    static ArrayList<String> resultList = new ArrayList<String>();
    static ArrayList<String> resultList1 = new ArrayList<String>();
    static ArrayList<String> resultList2 = new ArrayList<String>();
    static List<String> sortList = new ArrayList<String>();
    static List<String> newList = new ArrayList<String>();
    static ArrayList<String> filterList1 = new ArrayList<String>();
    static ArrayList<String> filterList2 = new ArrayList<String>();
    
    public BayonetGround(){
        photoURL=staticparameter.getValue("photoURL","http://172.16.100.51:8888/n/0/0/");
        expireInterval=staticparameter.getIntValue("expireInterval",86400);
        base.printMsg("photoURL=",photoURL);
        base.printMsg("expireInterval=",Integer.toString(expireInterval));
    }
    
    public ArrayList<String> insetFilter(ArrayList<String> al,TheGround theGround)
    {
        ArrayList<String> result = new ArrayList<String>();
        Set<String> inset = new HashSet<String>();
        ArrayList<GroundTGS> tgs = theGround.getTgsList();
        
        Iterator iterator1 = tgs.iterator();
        while(iterator1.hasNext())
        {
            GroundTGS gtgs = (GroundTGS) iterator1.next();
            inset = gtgs.getInSet();
        }
        
        Iterator iterator = al.iterator();
        while(iterator.hasNext())
        {
            String element = (String) iterator.next();
            String[] ss = element.split("\\,/");
            //System.out.println("vvvvvvvvvv" + ss[0] +" " +  ss[1] +" "+ss[2] +" "+ss[3]);
            
            Iterator iterator2 = inset.iterator();
            while(iterator2.hasNext())
            {
                String element2 = (String) iterator2.next();
                if(element2.equals(ss[2]))
               {
                   result.add(element);
               }
            }
        }
        return result;
    }
    
    public ArrayList<String> outsetFilter(ArrayList<String> al,TheGround theGround)
    {
        ArrayList<String> result = new ArrayList<String>();
        Set<String> outset = new HashSet<String>();
        ArrayList<GroundTGS> tgs = theGround.getTgsList();
        
        Iterator iterator1 = tgs.iterator();
        while(iterator1.hasNext())
        {
            GroundTGS gtgs = (GroundTGS) iterator1.next();
            outset = gtgs.getOutSet();
        }
        
        Iterator iterator = al.iterator();
        while(iterator.hasNext())
        {
            String element = (String) iterator.next();
            String[] ss = element.split("\\,/");
            Iterator iterator2 = outset.iterator();
            while(iterator2.hasNext())
            {
                String element2 = (String) iterator2.next();
                if(element2.equals(ss[2]))
               {
                    result.add(element);
               }
                
            }
        }
        return result;
    }
    
    public int getFromHBase(TheGround theGround,QueryInfo queryInfo) throws Exception
    {
        Configuration conf = HBaseConfiguration.create();
        HTable table = new HTable(conf, "flow");
        Scan scan = new Scan();
        Scan scan1 = new Scan();
        
        key = getKey(theGround,queryInfo);
        String begin1 = theGround.getBeforeTime();
        String end1 = theGround.getBegTime();
        String begin2 = theGround.getEndTime();
        String end2 = theGround.getAfterTime();
        ArrayList<GroundTGS> groundList = theGround.getTgsList();
        ArrayList<String> kList = new ArrayList<String>();
        
        Iterator iteratorG = groundList.iterator();
        while (iteratorG.hasNext())
        {
        	GroundTGS element = (GroundTGS) iteratorG.next();
        	String s = element.getTgs();
        	kList.add(s);
        }
        
        Iterator iteratorZ = kList.iterator();
        while(iteratorZ.hasNext())
        {
            String element = (String) iteratorZ.next();
            
        scan.setStartRow(Bytes.toBytes(element + "," + begin1));
        scan.setStopRow(Bytes.toBytes(element + "," + end1));
        ResultScanner r1 = table.getScanner(scan);
        for(Result rr=r1.next();rr!=null;rr=r1.next())
        {   
        	 //count++;
        	//if(rr.getValue(Bytes.toBytes("flowfamily"),Bytes.toBytes("PASSCARSTATE")) == null)
        	   // continue;
        	if(rr.getValue(Bytes.toBytes("flowfamily"),Bytes.toBytes("SPEED")) == null)
            	continue;
        	if(rr.getValue(Bytes.toBytes("flowfamily"),Bytes.toBytes("PLATECOLOR")) == null)
            	continue;
        	if(rr.getValue(Bytes.toBytes("flowfamily"),Bytes.toBytes("LOCATIONID")) == null)
            	continue;
        	if(rr.getValue(Bytes.toBytes("flowfamily"),Bytes.toBytes("DRIVEWAY")) == null)
            	continue;
        	if(rr.getValue(Bytes.toBytes("flowfamily"),Bytes.toBytes("DRIVEDIR")) == null)
            	continue;
        	if(rr.getValue(Bytes.toBytes("flowfamily"),Bytes.toBytes("CAPTUREDIR")) == null)
            	continue;
        	if(rr.getValue(Bytes.toBytes("flowfamily"),Bytes.toBytes("CARCOLOR")) == null)
            	continue;
        	if(rr.getValue(Bytes.toBytes("flowfamily"),Bytes.toBytes("CARBRAND")) == null)
            	continue;
            //System.out.println(new String(rr.getRow()));
            //System.out.println(new String(rr.getValue(Bytes.toBytes("flowfamily"),Bytes.toBytes("CARBRAND"))));
            //System.out.println(new String(rr.getValue(Bytes.toBytes("flowfamily"),Bytes.toBytes("CARSTATE"))));
            //System.out.println(new String(rr.getValue(Bytes.toBytes("flowfamily"),Bytes.toBytes("CARSTATE"))));
        	//count++;
            String result = new String(rr.getRow()) 
            //+ "," + new String(rr.getValue(Bytes.toBytes("flowfamily"),Bytes.toBytes("PASSCARSTATE")))
            + ",/" + new String(rr.getValue(Bytes.toBytes("flowfamily"),Bytes.toBytes("SPEED")))
            + "," + new String(rr.getValue(Bytes.toBytes("flowfamily"),Bytes.toBytes("PLATECOLOR")))
            + "," + new String(rr.getValue(Bytes.toBytes("flowfamily"),Bytes.toBytes("LOCATIONID")))
            + "," + new String(rr.getValue(Bytes.toBytes("flowfamily"),Bytes.toBytes("DRIVEWAY")))
            + ",/" + new String(rr.getValue(Bytes.toBytes("flowfamily"),Bytes.toBytes("DRIVEDIR")))
            + ",/" + new String(rr.getValue(Bytes.toBytes("flowfamily"),Bytes.toBytes("CAPTUREDIR")))
            + "," + new String(rr.getValue(Bytes.toBytes("flowfamily"),Bytes.toBytes("CARCOLOR")))
            + "," + new String(rr.getValue(Bytes.toBytes("flowfamily"),Bytes.toBytes("CARBRAND")));
            
            
             System.out.println("1 : " + result);
             resultList1.add(result);
        }
        
        scan1.setStartRow(Bytes.toBytes(element + "," + begin2));
        scan1.setStopRow(Bytes.toBytes(element + "," + end2));
        ResultScanner r2 = table.getScanner(scan1);
        
        for(Result rr2=r2.next();rr2!=null;rr2=r2.next())
        {
            //System.out.println(new String(rr.getValue(Bytes.toBytes("flowfamily"),Bytes.toBytes("CARBRAND"))));
            //System.out.println(new String(rr.getValue(Bytes.toBytes("flowfamily"),Bytes.toBytes("CARSTATE"))));
            //System.out.println(new String(rr.getValue(Bytes.toBytes("flowfamily"),Bytes.toBytes("CARSTATE"))))
        	//if(rr2.getValue(Bytes.toBytes("flowfamily"),Bytes.toBytes("PASSCARSTATE")) == null)
        	   // continue;
        	if(rr2.getValue(Bytes.toBytes("flowfamily"),Bytes.toBytes("SPEED")) == null)
            	continue;
        	if(rr2.getValue(Bytes.toBytes("flowfamily"),Bytes.toBytes("PLATECOLOR")) == null)
            	continue;
        	if(rr2.getValue(Bytes.toBytes("flowfamily"),Bytes.toBytes("LOCATIONID")) == null)
            	continue;
        	if(rr2.getValue(Bytes.toBytes("flowfamily"),Bytes.toBytes("DRIVEWAY")) == null)
            	continue;
        	if(rr2.getValue(Bytes.toBytes("flowfamily"),Bytes.toBytes("DRIVEDIR")) == null)
            	continue;
        	if(rr2.getValue(Bytes.toBytes("flowfamily"),Bytes.toBytes("CAPTUREDIR")) == null)
            	continue;
        	if(rr2.getValue(Bytes.toBytes("flowfamily"),Bytes.toBytes("CARCOLOR")) == null)
            	continue;
        	if(rr2.getValue(Bytes.toBytes("flowfamily"),Bytes.toBytes("CARBRAND")) == null)
            	continue;
        	
            //count++;
            String result = new String(rr2.getRow()) 
           // + "," + new String(rr2.getValue(Bytes.toBytes("flowfamily"),Bytes.toBytes("PASSCARSTATE")))
            + ",/" + new String(rr2.getValue(Bytes.toBytes("flowfamily"),Bytes.toBytes("SPEED")))
            + "," + new String(rr2.getValue(Bytes.toBytes("flowfamily"),Bytes.toBytes("PLATECOLOR")))
            + "," + new String(rr2.getValue(Bytes.toBytes("flowfamily"),Bytes.toBytes("LOCATIONID")))
            + "," + new String(rr2.getValue(Bytes.toBytes("flowfamily"),Bytes.toBytes("DRIVEWAY")))
            + ",/" + new String(rr2.getValue(Bytes.toBytes("flowfamily"),Bytes.toBytes("DRIVEDIR")))
            + ",/" + new String(rr2.getValue(Bytes.toBytes("flowfamily"),Bytes.toBytes("CAPTUREDIR")))
            + "," + new String(rr2.getValue(Bytes.toBytes("flowfamily"),Bytes.toBytes("CARCOLOR")))
            + "," + new String(rr2.getValue(Bytes.toBytes("flowfamily"),Bytes.toBytes("CARBRAND")));
            resultList2.add(result);
            System.out.println("2 : " + result);
          }
        }
        filterList1 = insetFilter(resultList1,theGround);
        filterList2 = outsetFilter(resultList2,theGround);
        
      /*  Iterator iteratorM  = filterList1.iterator();//测试2个filter
        while(iteratorM.hasNext())
        {
            String element = (String) iteratorM.next();
            System.out.println(element);
        }*/
        
        resultList = base.merge(filterList1, filterList2);
        System.out.println("merge");
        
        
        
        Iterator iterator0  = resultList.iterator();
        while(iterator0.hasNext())
        {
        	count++;
            String element = (String) iterator0.next();
            sortList.add(element);
            System.out.println("resultList : " + element);
        }
        List<String> newList = (List<String>) base.sort1(sortList);
        
        Iterator iterator  = newList.iterator();
        while(iterator.hasNext())
        {
            String element = (String) iterator.next();
            //System.out.println("newlist : " + base.getNumber(m) +" : " +element);
            element=element.replace("/", "");
            //System.out.println("newlist after replace: " + base.getNumber(m) +" : " +element);
            System.out.println("key: "+key+base.getNumber(m)+" value:"+element);
            String s1 = memorydb.add(key + base.getNumber(m),element);
            memorydb.expire(key + base.getNumber(m),expireInterval);
            m++;
        }
        String s1 = memorydb.add(key,count + "");
        memorydb.expire(key,expireInterval);
        m = 0;
        
        return count;
    }
    
    public BackResult<LocusCarResult> getFromMemoryDB(TheGround theGround,int begNum,int num,QueryInfo queryInfo) throws Exception
    {
        key = getKey(theGround,queryInfo);
        String s = memorydb.get(key);

        BackResult br = new BackResult();
        ArrayList<LocusCarResult> brList = new ArrayList<LocusCarResult>();
        ResultCar rc = new ResultCar();
        LocusCarResult lcr = new LocusCarResult();
        PassTgsInfo pti = new PassTgsInfo();
        ArrayList<PassTgsInfo> ptiList = new ArrayList<PassTgsInfo>();
        
        if (s == null)
        {
           
            getFromHBase(theGround,queryInfo);
            s = memorydb.get(key);
            m = base.getCount(Integer.parseInt(s),begNum,num);
        }
        else
        {
            m = base.getCount(Integer.parseInt(s),begNum,num);
            if(m==0)
            {
                System.out.println(key+":"+s+":"+"没有更多的数据！");
                return null;
            }
        }
        
         		
                for(int i = begNum;i < begNum + m;i++)
                {
                	System.out.println("from memory:"+memorydb.get(key + base.getNumber(i)));
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
                    //System.out.println(url[0]);
                    rc.setImgUrls(url);
                    
                    pti.setTgsId(split[0]);
                    pti.setPassTime(split[1]);
                    pti.setSpeed(split[5]);
                    pti.setTravelOrientation(split[8]);
                    pti.setImgUrl(url);
                    ptiList.add(pti);
                    lcr.setCar(rc);
                    lcr.setTgsInfo(ptiList);
                    brList.add(lcr);
                }
                br.setBeans(brList);
                br.setTotalNum(Integer.parseInt(s));
                System.out.println("The total number is : " + s);
                return br;
       }
            
        
        
    
    

    
    public static String getKey(TheGround theGround,QueryInfo queryInfo)
    {
        String result;
        ArrayList<GroundTGS>  al = new ArrayList<GroundTGS>();
        al = theGround.getTgsList();
        result = theGround.getBeforeTime() + theGround.getBegTime() + theGround.getEndTime() + theGround.getAfterTime();
        return result;
    }
}

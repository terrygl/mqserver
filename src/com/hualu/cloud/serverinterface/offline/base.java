package com.hualu.cloud.serverinterface.offline;


import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;

import com.hualu.cloud.basebase.staticparameter;


public class base {
	
	static int appstate=-1;
	static int timePosStart=11;
	static int timePosEnd=25;
	//static String photoURL="http://10.2.111.106/photo";
	

	//.getValue("appstate");//1:有些内容要打印，0：有些内容不要打印
	
    /**
     * 获取格式为yyyyMMddHHmmss的当前时间
     * @return：返回格式为yyyyMMddHHmmss的当前时间
     */
	public static String currentTime(){
        Date date=new Date(System.currentTimeMillis()); 
        SimpleDateFormat format=new SimpleDateFormat("yyyyMMddHHmmss"); 
        return format.format(date); 
    }
	
	/**
	 * 生成序号，用于往内存数据库中插入详细记录时使用，要求key后+7位序号。
	 * @param num
	 * @return
	 */
    public static String getNumber(int num)
    {
        String result = "";
        switch ((num+"").length()) {
	        case 1:
	            result = "000000" + num;
	            break;
	        case 2:
	            result = "00000" + num;
	            break;
	        case 3:
	            result = "0000" + num;
	            break;
	        case 4:
	            result = "000" + num;
	            break;
	        case 5:
	            result = "00" + num;
	            break;
	        case 6:
	            result = "0" + num;
	            break;
	        case 7:
	            result = "" + num;
	            break;
	        default:
	        	result = "0000000";
        }
        return result;
    }
    
	/**
	 * 生成长整形序号，用于往内存数据库中插入详细记录时使用，要求key后+12位序号。
	 * @param num
	 * @return
	 */
    public static String getLongNumber(long num)
    {
        String result = "";
        switch ((num+"").length()) {
	        case 1:
	            result = "00000000000" + num;
	            break;
	        case 2:
	            result = "0000000000" + num;
	            break;
	        case 3:
	            result = "000000000" + num;
	            break;
	        case 4:
	            result = "00000000" + num;
	            break;
	        case 5:
	            result = "0000000" + num;
	            break;
	        case 6:
	            result = "000000" + num;
	            break;
	        case 7:
	            result = "00000" + num;
	            break;
	        case 8:
	            result = "0000" + num;
	            break;
	        case 9:
	            result = "000" + num;
	            break;
	        case 10:
	            result = "00" + num;
	            break;
	        case 11:
	            result = "0" + num;
	            break;
	        case 12:
	            result = "" + num;
	            break;
	            
	        default:
	        	result = "000000000000";
        }
        return result;
    }
    
    /**
     * 根据总结果数、begNum和num判断本次应该返回的记录条数。主要为了防止超出范围返回记录
     * @param count:count为redis里总共记录数
     * @param begNum:要求从第begNum条开始返回
     * @param num:要求返回的条目数
     * @return
     */
    public static int getCount(int count,int begNum,int num)
    {
        int m;
        if(begNum>count)
        {
        	m=0;
 
        }
        else
        {
            if(begNum+num > count)
            {
                m = count-begNum+1;
            }
            else
            {
                m = num;
            }
        }
        return m;
    }
    
    public static long getLongCount(long count,long begNum,long num)
    {
        long m;
        if(begNum>count)
        {
        	m=0;
 
        }
        else
        {
            if(begNum+num > count)
            {
                m = count-begNum+1;
            }
            else
            {
                m = num;
            }
        }
        return m;
    }
    
    /**
     * 把字符串数组变成字符串。
     * @param s：字符串数组
     * @return：生成的字符串
     */
    public static String stringArrayChange(String[] s)
    {
        StringBuffer sb = new StringBuffer();
        for(int i = 0;i < s.length;i++)
        {
            sb.append(s[i]);
        }
        String result = sb.toString();
        return result;
    }

    /**
     * flow的rowkey按时间排序:获取List每个元素的时间并加上数组下标，排序后，根据下标调整result的顺序，形成newList
     * @param resultList：待排序的List
     * @return
     */
    public static List<String> sort1(List<String> resultList)
    {
    	//获取List每个元素的时间并加上数组的下标形成一个新的List
    	List<String> timenum=new ArrayList<String>(resultList.size());
    	Iterator iterator0=resultList.iterator();
    	String element0,element1;
    	int i=0;
    	while(iterator0.hasNext()){
    		element0 = (String) iterator0.next();
    		element1 = element0.substring(timePosStart, timePosEnd)+i;
    		//System.out.println("element1="+element1);
    		//时间元素正确的情况下才进行排序，不正确的剔除,这个另外判断
    		timenum.add(element1);
    		i++;
    	}
    	//按时间顺序排序
        Collections.sort(timenum);
        
    	List<String> newList = new ArrayList<String>();//排序后的记录
        Iterator iterator = timenum.iterator();
        while (iterator.hasNext()) 
        {
            element0 = (String)iterator.next();

            i=Integer.parseInt(element0.substring(14));
            newList.add(resultList.get(i));
        }
        return newList;
    }
    
    
    /**
     * flow的rowkey按时间排序
     * @param a：从待排序的List中截取的时间串
     * @param resultList：待排序的List
     * @return
     */
    public static List<String> sort(List<String> a,List<String> resultList)
    {
    	List<String> newList = new ArrayList<String>();//排序后的记录
        Collections.sort(a);
        Iterator iterator = a.iterator();
        while (iterator.hasNext()) 
        {
            String element = (String)iterator.next();
            Iterator iterator1 = resultList.iterator();
            while (iterator1.hasNext()) 
            {
                String element1 = (String) iterator1.next();
                //if (element.equals(element1.substring(11, 25)))
                if (element.equals(element1.substring(timePosStart, timePosEnd)))
                {
                    newList.add(element1);
                }
            }
        }
        return newList;
    }
    
    /**
     * 把格式为YYYY-MM-DD HH24:MI:SS的时间转换为YYYYMMDDHHMISS
     * @param timeStr:格式为YYYY-MM-DD HH24:MI:SS的时间字符串
     * @return：格式为为YYYYMMDDHHMISS的时间字符串
     */
    public static String changeTimeStyle(String timeStr){
    	String[] datetime=timeStr.split(" ");
    	String[] date=datetime[0].split("-");
    	String[] time=datetime[1].split(":");
    	return date[0]+date[1]+date[2]+time[0]+time[1]+time[2];
    }
    
    /**
     * 打印信息：如果config文件中appstate为1则在屏幕打印，如果为0则不打印。
     * @param txt：信息名称
     * @param value：信息内容
     */
	static void printMsg(String txt,String value){
		if(appstate==0);
		else{
			if(appstate==1)
				System.out.println(txt+value);
			else
			{
				appstate=staticparameter.getIntValue("appstate",0);
				timePosStart=staticparameter.getIntValue("timePosStart",11);
				timePosEnd=staticparameter.getIntValue("timePosEnd",25);
				System.out.println("appstate="+appstate);
				if(appstate==1)
					System.out.println(txt+value);
			}
		}			
	}
	
    /**
     * 日志信息：需要记录日志信息
     * @param log：需要记录的日志信息
     */
	static void Log(String log){
		
	}
	
	/**在msg中获取tag的value，例如 TIME=2012;TGS=1,2,3;NAME=yhy,tag为"NAME",tag1为"=",tag2为";"
	 * @param msg：需要进行处理的字符串
	 * @param tag：需要获取value的tag值
	 * @param tag0：value中的name和value的分割符
	 * @param tagl：不同keyvalue之间的分隔符，例如工控机的就是换行符
	 * @return ：返回tag的value
	 */
    public static String getValue(String msg,String tag,String tag1,String tag2){

        int pos0=msg.indexOf(tag);
        if(pos0<0) {
        	printMsg(tag," doesn't exist!");
        	return null;
        }
        int pos1=msg.indexOf(tag1,pos0);
        int pos2=msg.indexOf(tag2,pos0);
        String value;
        if(pos2<0) {     
        	//System.out.println("tag is at the tail!");
        	pos2=msg.length();
			value=msg.substring(pos1+1,pos2);//value
        }else{
        	//System.out.println("tag is at the middle!");
        	if(pos1+1>pos2)
        		return null;
			value=msg.substring(pos1+1,pos2);//value
        }
		return value;		
	}
    
    /**
     * flow表的rowkey组成的list，按照车牌类型和车牌号码比较两个list是否有交集，把交集相关记录提取出来。
     * @param list3:flow表的rowkey组成的list
     * @param list4:flow表的rowkey组成的list
     * @return:list3和list4的交集相关记录，每个值也是flow的rowkey格式。
     */
    public static ArrayList<String> intersection1(ArrayList<String> list3,ArrayList<String> list4){
    	ArrayList<String> resultList=new ArrayList<String>();
    	ArrayList<String> list1=new ArrayList<String>(list3);
    	ArrayList<String> list2=new ArrayList<String>(list4);
    	int size1=list1.size();
    	int size2=list2.size();

    	int i=0;
    	while(i<size1){
	    		//从rowkey中取出车牌类型和车牌号码
	    		String a1=list1.get(i);
	    		String[] a2=a1.split(",");
	    		String a=a2[2]+","+a2[3];
	    		
	    		//从list1中取一个，比对list2
	    		int state=0;
	    		int j=0;
	    		while(j<size2){
	        		String b=list2.get(j);
	    			if(b.indexOf(a)>0){//如果b中有a，即车牌类型和车牌号相同
	           			//将b加入到resultlist
	    				
	    				resultList.add(b);
	        			//并从list2中将该元素remove
	    				list2.remove(j);
	    				size2=list2.size();
	    				state=1;
	    			}else
	    				j++;	    			
	    		}
	    		
	    		//如果该记录在list2中有交集，即state=1，则需要从list1中删除该记录，并加入到resultList中
	    		if(state==1){
		    		//把a1也加入resultList
		    		resultList.add(a1);
		    		list1.remove(i);
		    		size1=list1.size();
		    		size2=list2.size();
	    		}
	    		
	    		//比对list1中元素i之后，逐个比较看是否有相同车牌类型和车牌号的数据
	    		int l=i;
	    		while(l<size1){
	        		String c=list1.get(l);
	    			if(c.indexOf(a)>0){//如果c中有a，即车牌类型和车牌号相同
	        			//list1中将该元素remove
	    				list1.remove(l);
	    				size1=list1.size();
	    				if(state==1){
		           			//将c加入到resultlist
		    				resultList.add(c);
	    				}
	    			}else{
	    				l++;
	    			}
	    		}
	    		
	    		if(state==0)
	    			i++;
    	}
    	return resultList;
    }
    
    public static ArrayList<String> merge(ArrayList<String> al1,ArrayList<String> al2)
    {int i = 0;int j =0;
        ArrayList<String> subList1 = new ArrayList<String>();
        ArrayList<String> subList2 = new ArrayList<String>();
        ArrayList<String> resultList = new ArrayList<String>();
        ArrayList<String> wholeList = new ArrayList<String>();
        
        
        Iterator iterator1 = al1.iterator();
        while (iterator1.hasNext())
        {
            String element1 = (String) iterator1.next();
            String[] ss = element1.split("\\,/");
            String sub1 = ss[0];
            subList1.add(sub1);
        }
        
        Iterator iterator2 = al2.iterator();
        while (iterator2.hasNext())
        {
            String element2 = (String) iterator2.next();
            String[] ss = element2.split("\\,/");
            String sub2 = ss[0];
            subList2.add(sub2);
        }
        
        resultList = intersection1(subList1,subList2);
        
        
        Iterator iterator3 = al1.iterator();
        while (iterator3.hasNext()) 
        {
            String element3 = (String)iterator3.next();
            String[] ss = element3.split("\\,/");
            Iterator iterator4 = resultList.iterator();
            while (iterator4.hasNext()) 
            {
                String element4 = (String) iterator4.next();
                if (element4.equals(ss[0]))
                {
                    wholeList.add(element3);
                }
            }
        }
        
        Iterator iterator5 = al2.iterator();
        while (iterator5.hasNext()) 
        {
            String element5 = (String)iterator5.next();
            String[] ss = element5.split("\\,/");
            Iterator iterator6 = resultList.iterator();
            while (iterator6.hasNext()) 
            {
                String element6 = (String) iterator6.next();
                if (element6.equals(ss[0]))
                {
                    wholeList.add(element5);
                }
            }
        }
        return wholeList;
    }
    
    public static int week(String s) throws Exception
    {
    	String[] ss = s.split(",");
		SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
		Calendar c = Calendar.getInstance();
		c.setTime(format.parse(ss[2].substring(0, 8)));
		int dayForWeek = 0;
		if (c.get(Calendar.DAY_OF_WEEK) == 1) {
			dayForWeek = 7;
		} else {
			dayForWeek = c.get(Calendar.DAY_OF_WEEK) - 1;
		}
    	return dayForWeek;
    }
    
    public static int month(String s)
    {
    	String[] ss = s.split(",");
    	return Integer.parseInt(ss[2].substring(6, 8));
    }
    
    public static Boolean carNumberFilter(String s1,String s2)//s1是给出的查询信息，s2是HBase取出的数据
    {
    	//System.out.println("filter:"+s1+" "+s2);
    	String s11 = s1.replace('%', '.');
    	String s111 = s11.replace("*", ".*");
    	Pattern p1 = Pattern.compile(s111);
    	Matcher m1 = p1.matcher(s2);
    	return m1.matches();    
    }
    
    public static String setString(String beginTime, long i)//输入时间字符串和i分钟，返回加上i分钟后的时间字符串
    {
    	int year = Integer.parseInt(beginTime.substring(0,4));
    	int month = Integer.parseInt(beginTime.substring(4,6));
    	int date = Integer.parseInt(beginTime.substring(6,8));
    	int hour = Integer.parseInt(beginTime.substring(8,10));
    	int minute = Integer.parseInt(beginTime.substring(10,12));
    	int second = Integer.parseInt(beginTime.substring(12,14));
    	Calendar c = Calendar.getInstance();
    	c.set(year, month, date, hour, minute, second);
    	long temp = c.getTimeInMillis();
    	long temp1 = i * 60 * 1000;
    	Calendar d = Calendar.getInstance();
	    d.setTimeInMillis(temp - temp1);
	    int year1 = d.get(Calendar.YEAR);
	    int month1 = d.get(Calendar.MONTH);
	    int date1 = d.get(Calendar.DATE);
	    int hour1 = d.get(Calendar.HOUR_OF_DAY);
	    int minute1 = d.get(Calendar.MINUTE);
	    int second1 = d.get(Calendar.SECOND);
	    
	    StringBuffer smonth1 = new StringBuffer(String.valueOf(month1));
	    if(month1 < 10)
	    	smonth1.insert(0, "0");
	    StringBuffer sdate1 = new StringBuffer(String.valueOf(date1));
	    if(date1 < 10)
	    	sdate1.insert(0, "0");
	    StringBuffer shour1 = new StringBuffer(String.valueOf(hour1));
	    if(hour1 < 10)
	    	shour1.insert(0, "0");
	    StringBuffer sminute1 = new StringBuffer(String.valueOf(minute1));
	    if(minute1 < 10)
	    	sminute1.insert(0, "0");
	    StringBuffer ssecond1 = new StringBuffer(String.valueOf(second1));
	    if(second1 < 10)
	    	ssecond1.insert(0, "0");
	    
	    String s = "" + year1 + smonth1 + sdate1 + shour1 + sminute1 + ssecond1;
	    return s;
    }
    
    public static long minusCount(String s1, String s2)
    {
    	int year1 = Integer.parseInt(s1.substring(0,4));
    	int month1 = Integer.parseInt(s1.substring(4,6 ));
    	int date1 = Integer.parseInt(s1.substring(6,8));
    	int hour1 = Integer.parseInt(s1.substring(8,10));
    	int minute1 = Integer.parseInt(s1.substring(10,12));
    	int second1 = Integer.parseInt(s1.substring(12,14));
    	Calendar c1 = Calendar.getInstance();
    	c1.set(year1, month1, date1, hour1, minute1, second1);
    	long temp1 = c1.getTimeInMillis();
    	
    	int year2 = Integer.parseInt(s2.substring(0,4));
    	int month2 = Integer.parseInt(s2.substring(4,6 ));
    	int date2 = Integer.parseInt(s2.substring(6,8));
    	int hour2 = Integer.parseInt(s2.substring(8,10));
    	int minute2 = Integer.parseInt(s2.substring(10,12));
    	int second2 = Integer.parseInt(s2.substring(12,14));
    	Calendar c2 = Calendar.getInstance();
    	c2.set(year2, month2, date2, hour2, minute2, second2);
    	long temp2 = c2.getTimeInMillis();
    	
	    return (long) ((temp2 - temp1) * 0.001);
    }
    
    /**
     * 对一个column的0-n个值进行过滤，在valuelist中过滤出来。
     * @param familyname
     * @param columnname
     * @param valuelist
     * @return
     */
    public FilterList getFilterListSingleColumnIn(String familyname,String columnname,String[] valuelist){
    	int len0=valuelist.length;
    	if(len0==0) return null;
    	else{
    		FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
    		for(int i=0;i<len0;i++){
    			SubstringComparator comp = new SubstringComparator(valuelist[i]);   
    			filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes(familyname),Bytes.toBytes(columnname),CompareOp.EQUAL,comp));
    		}
        	return filterList;
    	}    	
    }
    
	public long getTimelong(String datetime){
		int year,month,day,hour,minute,second;
		String[] v1=datetime.split(" ");
		String[] v2=v1[0].split("-");
		year=Integer.parseInt(v2[0]);
		month=Integer.parseInt(v2[1]);
		day=Integer.parseInt(v2[2]);
		String[] v3=v1[1].split(":");
		hour=Integer.parseInt(v3[0]);
		minute=Integer.parseInt(v3[1]);
		second=Integer.parseInt(v3[2]);
		
		
		Calendar calendar=Calendar.getInstance();
		calendar.set(year,month,day,hour,minute,second);
		long l1=calendar.getTimeInMillis();
		return  l1;
	}
	
	
	   public static String setStringPlus(String beginTime, long i)//杈撳叆鏃堕棿瀛楃涓插拰i鍒嗛挓锛岃繑鍥炲姞涓奿鍒嗛挓鍚庣殑鏃堕棿瀛楃涓�
	    {
	    	int year = Integer.parseInt(beginTime.substring(0,4));
	    	int month = Integer.parseInt(beginTime.substring(4,6 ));
	    	int date = Integer.parseInt(beginTime.substring(6,8));
	    	int hour = Integer.parseInt(beginTime.substring(8,10));
	    	int minute = Integer.parseInt(beginTime.substring(10,12));
	    	int second = Integer.parseInt(beginTime.substring(12,14));
	    	Calendar c = Calendar.getInstance();
	    	c.set(year, month, date, hour, minute, second);
	    	long temp = c.getTimeInMillis();
	    	long temp1 = i * 60 * 1000;
	    	Calendar d = Calendar.getInstance();
		    d.setTimeInMillis(temp + temp1);
		    int year1 = d.get(Calendar.YEAR);
		    int month1 = d.get(Calendar.MONTH);
		    int date1 = d.get(Calendar.DATE);
		    int hour1 = d.get(Calendar.HOUR_OF_DAY);
		    int minute1 = d.get(Calendar.MINUTE);
		    int second1 = d.get(Calendar.SECOND);
		    
		    StringBuffer smonth1 = new StringBuffer(String.valueOf(month1));
		    if(month1 < 10)
		    	smonth1.insert(0, "0");
		    StringBuffer sdate1 = new StringBuffer(String.valueOf(date1));
		    if(date1 < 10)
		    	sdate1.insert(0, "0");
		    StringBuffer shour1 = new StringBuffer(String.valueOf(hour1));
		    if(hour1 < 10)
		    	shour1.insert(0, "0");
		    StringBuffer sminute1 = new StringBuffer(String.valueOf(minute1));
		    if(minute1 < 10)
		    	sminute1.insert(0, "0");
		    StringBuffer ssecond1 = new StringBuffer(String.valueOf(second1));
		    if(second1 < 10)
		    	ssecond1.insert(0, "0");
		    
		    String s = "" + year1 + smonth1 + sdate1 + shour1 + sminute1 + ssecond1;
		    return s;
	    }
	   
//	   public static void main(String args[]) throws IOException{
//		   int i=1999;
//		   System.out.println("mmmmmmmmmmmmmmm="+base.getNumber(i));
//	   }


}

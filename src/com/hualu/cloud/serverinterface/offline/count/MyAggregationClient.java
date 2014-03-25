package com.hualu.cloud.serverinterface.offline.count;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.coprocessor.ColumnInterpreter;
import org.apache.hadoop.hbase.util.Bytes;

public class MyAggregationClient {
	private static final byte[] TABLE_NAME = Bytes.toBytes("by_flow");
	private static final byte[] CF = Bytes.toBytes("cf");//familyname
	private static final byte[] columnqualifier=Bytes.toBytes("info");//columnname
	public static void count() throws Throwable {
		Configuration conf = HBaseConfiguration.create();
		/*
		Configuration customConf = new Configuration();
		customConf.setStrings("hbase.zookeeper.quorum",
		"host51,host51,host53");
		//提高RPC通信时长
		customConf.setLong("hbase.rpc.timeout", 600000);
		//设置Scan缓存
		customConf.setLong("hbase.client.scanner.caching", 1000);
		Configuration configuration = HBaseConfiguration.create(customConf);
		*/
		AggregationClient aggregationClient = new AggregationClient(conf);
		Scan scan = new Scan();
		//指定扫描列族，唯一值
		scan.addColumn(CF, columnqualifier);
		long rowCount = aggregationClient.rowCount(TABLE_NAME,null, scan);
		System.out.println("row count is " + rowCount);
	}
}

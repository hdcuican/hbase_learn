package com.hbase;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

/**
 * <p>
 * Decsription:
 * </p>
 * 
 * @author shadow
 * @date 2016年8月17日
 */
public class HbasePool {

	private static final int HBASE_POOL_SIZE = 5;
	private LinkedBlockingQueue<Connection> queue = new LinkedBlockingQueue<Connection>();

	public HbasePool() {
		for (int i = 0; i < HBASE_POOL_SIZE; i++) {
			Configuration configuration = HBaseConfiguration.create();
			configuration.set("hbase.rootdir",
					"file:///usr/local/hbase/hbase-tmp");
			try {
				Connection connection = ConnectionFactory
						.createConnection(configuration);
				queue.put(connection);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public Connection getConnection() {
		try {
			return queue.take();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return null;
	}

	public void putConnection(Connection connection) {
		try {
			if(connection != null){
				queue.put(connection);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void closeTableTable(Table table) {
		if (table != null) {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

}

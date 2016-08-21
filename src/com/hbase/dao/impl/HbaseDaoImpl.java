package com.hbase.dao.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.hbase.HbasePool;
import com.hbase.dao.HbaseDao;

/**
 * <p>
 * Decsription:
 * </p>
 * 
 * @author shadow
 * @date 2016年8月17日
 */
public abstract class HbaseDaoImpl<T extends Object> implements HbaseDao<T> {

	private HbasePool hbasePool;

	 /* (non-Javadoc)
	 * 
	 * @see com.hbase.dao.HbaseDao#insert(java.lang.String, java.lang.Object)
	 */
	@Override
	public int insert(String tableName, T t) {
		Table table = null;
		Connection connection = null;
		try {
			connection = hbasePool.getConnection();
			if (connection != null) {
				table = connection.getTable(TableName.valueOf(tableName));
				Put put = new Put(Bytes.toBytes(getRowkey(t)));
				table.put(buildPut(put, t));
			}
		} catch (IOException e) {
			e.printStackTrace();
			return 0;
		} finally {
			hbasePool.putConnection(connection);
			hbasePool.closeTableTable(table);
		}
		return 1;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.hbase.dao.HbaseDao#batchInsert(java.lang.String, java.util.List)
	 */
	@Override
	public int batchInsert(String tableName, List<T> lists) {
		Table table = null;
		Connection connection = null;
		try {
			connection = hbasePool.getConnection();
			if (connection != null) {
				table = connection.getTable(TableName.valueOf(tableName));
				List<Put> puts = new ArrayList<Put>(lists.size());
				for (T t : lists) {
					Put put = new Put(Bytes.toBytes(getRowkey(t)));
					puts.add(buildPut(put, t));
				}
				table.put(puts);
				table.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
			return 0;
		}
		return lists.size();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.hbase.dao.HbaseDao#delete(java.lang.String, java.lang.String)
	 */
	@Override
	public int delete(String tableName, String rowKey) {
		return delete(tableName, rowKey, null, null);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.hbase.dao.HbaseDao#delete(java.lang.String, java.lang.String,
	 * java.lang.String)
	 */
	@Override
	public int delete(String tableName, String rowKey, String colFamily) {
		return delete(tableName, rowKey, colFamily, null);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.hbase.dao.HbaseDao#delete(java.lang.String, java.lang.String,
	 * java.lang.String, java.lang.String)
	 */
	@Override
	public int delete(String tableName, String rowKey, String colFamily,
			String quaiter) {
		Table table = null;
		Connection connection = null;
		try {
			connection = hbasePool.getConnection();
			if (connection != null) {
				table = connection.getTable(TableName.valueOf(tableName));
				Delete delete = new Delete(rowKey.getBytes());
				// 删除指定列族的所有数据
				if (StringUtils.isNotEmpty(colFamily)) {
					delete.addFamily(Bytes.toBytes(colFamily));
					if (StringUtils.isNotEmpty(quaiter)) {
						delete.addColumn(Bytes.toBytes(colFamily),
								Bytes.toBytes(quaiter));
					}
				}
				table.delete(delete);
			}
		} catch (IOException e) {
			e.printStackTrace();
			return 0;
		} finally {
			hbasePool.putConnection(connection);
			hbasePool.closeTableTable(table);
		}
		return 1;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.hbase.dao.HbaseDao#find(java.lang.String, java.lang.String)
	 */
	@Override
	public T find(String tableName, String rowKey) {
		Table table = null;
		Connection connection = null;
		try {
			connection = hbasePool.getConnection();
			if (connection != null) {
				table = connection.getTable(TableName.valueOf(tableName));
				Get get = new Get(rowKey.getBytes());
				Result result = table.get(get);
				return bulidResult(result);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.hbase.dao.HbaseDao#findForAll()
	 */
	@Override
	public List<T> findForAll(String tableName) {
		Scan scan = new Scan();
		scan.setCaching(100);
		Table table = null;
		Connection connection = null;
		try {
			connection = hbasePool.getConnection();
			List<T> lists = new LinkedList<T>();
			if (connection != null) {
				table = connection.getTable(TableName.valueOf(tableName));
				ResultScanner scanner = table.getScanner(scan);
				for (Result result : scanner) {
					lists.add(bulidResult(result));
				}
			}
			return null;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return Collections.emptyList();
	}

	/**
	 * @param hbasePool
	 *            the hbasePool to set
	 */
	public void setHbasePool(HbasePool hbasePool) {
		this.hbasePool = hbasePool;
	}

}

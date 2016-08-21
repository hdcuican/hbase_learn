package com.hbase.dao;

import java.util.List;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

/**
 * <p>Decsription: </p>
 * @author  shadow
 * @date  2016年8月17日
 */
public interface HbaseDao<T> {
	
	String getRowkey(T t);
	
	Put buildPut(Put put, T t);
	
	T bulidResult(Result result);
	
	int insert(String tableName, T t);
	
	int batchInsert(String tableName, List<T> lists);
	
	int delete(String tableName, String rowKey);
	
	int delete(String tableName, String rowKey, String colFamily);
	
	int delete(String tableName, String rowKey, String colFamily, String quaiter);
	
	T find(String tableName, String rowKey);
	
	List<T> findForAll(String tableName);
	
}

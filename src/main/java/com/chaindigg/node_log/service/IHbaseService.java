package com.chaindigg.node_log.service;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import com.chaindigg.node_log.domain.EntityRowKeyMap;
import com.chaindigg.node_log.domain.entity.LogEntity;

/**
 * @des eth hbase 公共接口
 */
public interface IHbaseService {

	Object save(LogEntity data) throws Exception;

	/**
	 * 向表中插入多条数据
	 *
	 * @return
	 */
	Object saveList(List<LogEntity> pojos) throws Exception;


	Map<String, LogEntity> batchGet(List<String> rowKeys);

	Map<String, LogEntity> batchGet(List<String> rowKeys, boolean reverseRowKey,String family);

	LogEntity get(String rowKey) throws Exception;



}

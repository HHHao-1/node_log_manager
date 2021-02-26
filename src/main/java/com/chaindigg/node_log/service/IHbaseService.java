package com.chaindigg.node_log.service;

import java.util.List;
import java.util.Map;

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


	List<LogEntity> fuzzyScan(List<String> rowKeys) throws Exception;

	Map<String, LogEntity> batchGet(List<String> rowKeys);


	LogEntity get(String rowKey) throws Exception;



}

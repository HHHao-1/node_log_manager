package com.chaindigg.node_log.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Resource;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.ObjectUtils;

import com.chaindigg.node_log.domain.EntityRowKeyMap;
import com.chaindigg.node_log.domain.entity.LogEntity;
import com.chaindigg.node_log.service.IHbaseService;
import com.chaindigg.node_log.util.hbase.HdoopUtils;

/**
 * 默认 IHBaseService 接口实现；
 *
 * @author ssliu
 */
@Component
public  class LogHbaseImpl implements IHbaseService {
	@Resource
	private HdoopUtils hdoopUtils;

	LogEntity t = new LogEntity();

	@Value("${data.family}")
	private String family;

	@Value("${data.table}")
	private String table;

	@Override
	public Object save(LogEntity data) throws Exception {
		List<LogEntity> list = new ArrayList<>();
		list.add(data);
		return this.saveList(list);
	}

	@Override
	public Object saveList(List<LogEntity> pojos) throws Exception {
		if (ObjectUtils.isEmpty(pojos)) {
			return null;
		}
		List<EntityRowKeyMap> collect = pojos.stream().map(pojo -> {
			Map<String, Object> map = new HashMap<>(2);
			map.put(family, pojo);
			return new EntityRowKeyMap(pojo.getKey(), map);
		}).collect(Collectors.toList());

		return hdoopUtils.batchSaveList(table, getFamilies(), "", collect);
	}

	@Override
	public LogEntity get(String rowKey) throws Exception {
		return (LogEntity) hdoopUtils.get(table, rowKey, LogEntity.class);
	}

	@Override
	public List<LogEntity> fuzzyScan(List<String> rowKeys) throws Exception {
			return fuzzyScan(rowKeys,false,family);
	}
	private List<LogEntity> fuzzyScan(List<String> rowKeys, boolean reverseRowKey, String family)
			throws Exception {
		return hdoopUtils.fuzzyScan(table, rowKeys, LogEntity.class, family);
	}
	@Override
	public Map<String, LogEntity> batchGet(List<String> rowKeys) {
			return batchGet(rowKeys,false,family);
	}

	private Map<String, LogEntity> batchGet(List<String> rowKeys, boolean reverseRowKey, String family) {
		return hdoopUtils.parallelGet(table, rowKeys, LogEntity.class, family);
	}

	private String[] getFamilies() {
		return new String[] {family};
	}


}

package com.chaindigg.node_log.domain;

import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * hbase entity 保存时，需要将数据保存为 rowkey 和 map
 * @author zhifan_jyshi
 * @date 2020/6/11 12:19 下午
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class EntityRowKeyMap {

	private String rowKey;
	private Map<String,Object> map;

}

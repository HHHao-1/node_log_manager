package com.chaindigg.node_log.util.hbase;

import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Resource;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.StopWatch;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.BeanWrapper;
import org.springframework.beans.BeanWrapperImpl;
import org.springframework.beans.PropertyAccessorFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;

import com.alibaba.fastjson.JSON;
import com.chaindigg.node_log.domain.EntityRowKeyMap;
import com.chaindigg.node_log.domain.entity.LogEntity;
import com.google.common.collect.Lists;

import io.netty.channel.ConnectTimeoutException;
import lombok.extern.slf4j.Slf4j;

/**
 * @Author sunhp
 * @Description hbase 封装数据库接口
 * @Date 2020/2/5
 */
@Slf4j
@Component
public class HdoopUtils {
	@Resource
	private HbaseUtils hbaseUtils;
	@Resource
	private HbaseTemplate hbaseTemplate;


	@Value("${data.putSize}")
	int putSize;

	private static Connection conn;
	//

	/**
	 * 把除info保存为实体外 剩余的全部保存为json
	 *
	 * @param tableName      表名称
	 * @param columnFamilies 列族集合
	 * @param pojos          数据对象
	 *
	 * @return
	 */
	public <T> Object batchSaveList(String tableName, String[] columnFamilies, String jsonKey,
			List<EntityRowKeyMap> pojos) throws Exception, InterruptedException {
		if (pojos == null || pojos.isEmpty()) {
			return null;
		}
		log.info("start save list size {}", pojos.size());
		// hbaseUtils.initConnection();
		// return hbaseTemplate.execute(tableName, new TableCallback<Object>() {
		// 	@Override
		// 	public Object doInTable(HTableInterface table) throws Throwable {
		StopWatch t = new StopWatch();
		t.start();
		// List<Put> puts = new ArrayList<>(2000);
		// List<List<Put>> putsList = new ArrayList<>(2000);
		List<Put> puts = pojos.parallelStream().map(p -> data2Put(p, jsonKey)).collect(Collectors.toList());
		// int putSize = puts.size();
		// for (EntityRowKeyMap entityRowKeyMap : pojos) {
		//
		// 	Map<String, Object> map = entityRowKeyMap.getMap();
		// 	Put put = new Put(Bytes.toBytes(entityRowKeyMap.getRowKey()));
		// 	for (String column : columnFamilies) {
		// 		Object obj = map.get(column);
		// 		if (!ObjectUtils.isEmpty(obj)) {
		// 			if (HbaseTableConstant.FAMILY_INFO.contains(column)) {
		// 				PropertyDescriptor[] pds = BeanUtils.getPropertyDescriptors(obj.getClass());
		// 				BeanWrapper beanWrapper = PropertyAccessorFactory.forBeanPropertyAccess(obj);
		// 				//PropertyDescriptor[] pds = beanWrapper.getPropertyDescriptors();
		// 				for (PropertyDescriptor pd : pds) {
		// 					String properName = pd.getName();
		// 					String value = "";
		// 					if (beanWrapper.getPropertyValue(properName) != null) {
		// 						Class<?> type = pd.getPropertyType();
		// 						if (type.equals(String.class) || type.equals(Integer.class) || type.equals(
		// 								Long.class)) {
		// 							value = String.valueOf(beanWrapper.getPropertyValue(properName));
		// 						} else {
		// 							value = JSON.toJSONString(beanWrapper.getPropertyValue(properName));
		// 						}
		// 					}
		//
		// 					if (!StringUtils.isBlank(value) && !"class".equals(properName)) {
		// 						put.addColumn(Bytes.toBytes(column), Bytes.toBytes(properName), Bytes.toBytes(value));
		// 					}
		// 				}
		// 			} else {
		// 				put.add(Bytes.toBytes(column), Bytes.toBytes(jsonKey), Bytes.toBytes(obj.toString()));
		// 			}
		// 		}
		// 	}
		// 	puts.add(put);
		// 	if (puts.size() >= ParserConfigConstant.batchSize) {
		// 		putsList.add(puts);
		// 		puts = new ArrayList<>(ParserConfigConstant.batchSize);
		// 	}
		// }
		// if (!puts.isEmpty()) {
		// 	putsList.add(puts);
		// 	puts = new ArrayList<>(ParserConfigConstant.batchSize);
		// }

		long sum = puts.stream().mapToLong(put -> put.heapSize()).sum();
		t.stop();
		log.info("[PUT_SIZE] {} data to {} puts used {}, {}KB", tableName, pojos.size(), t, sum / 1000);
		t.reset();
		t.start();
		List<List<Put>> putsList = Lists.partition(puts, putSize);
		// putsList.parallelStream().forEach(list -> {
		// 	try {
		// 		saveList(tableName, list);
		// 	} catch (IOException e) {
		// 		throw new RuntimeException(e);
		// 	}
		// });

		putsList.parallelStream().forEach(list -> {
			try {
				saveList(tableName, list);
			} catch (IOException e) {
				log.error("hbase save error", e);
				throw new RuntimeException(e);
			}
		});

		// List<Future> futureList = new ArrayList<>(putsList.size());
		// for (List<Put> putList : putsList) {
		// 	Future<Object> submit = ThreadPoolUtil.submit(() -> saveList(tableName, putList));
		// 	futureList.add(submit);
		// }
		// for (Future future : futureList) {
		// 	future.get();
		// }
		t.stop();
		log.info("HBase save {} to {} used {}", pojos.size(), tableName, t);
		return null;
		// }
		// });
	}

	private Put data2Put(EntityRowKeyMap entityRowKeyMap, String jsonKey) {
		Put put = new Put(Bytes.toBytes(entityRowKeyMap.getRowKey()));
		Map<String, Object> map = entityRowKeyMap.getMap();
		for (Map.Entry<String, Object> entry : map.entrySet()) {
			String column = entry.getKey();
			Object obj = entry.getValue();
			if (!ObjectUtils.isEmpty(obj)) {
					PropertyDescriptor[] pds = BeanUtils.getPropertyDescriptors(obj.getClass());
					BeanWrapper beanWrapper = PropertyAccessorFactory.forBeanPropertyAccess(obj);
					//PropertyDescriptor[] pds = beanWrapper.getPropertyDescriptors();
					for (PropertyDescriptor pd : pds) {
						String properName = pd.getName();

						if ("key".equals(properName)) {
							continue;
						}
						String value = "";
						if (beanWrapper.getPropertyValue(properName) != null) {
							Class<?> type = pd.getPropertyType();
							if (type.equals(String.class) || type.equals(Integer.class) || type.equals(Long.class)) {
								value = String.valueOf(beanWrapper.getPropertyValue(properName));
							} else {
								value = JSON.toJSONString(beanWrapper.getPropertyValue(properName));
							}
						}

						if (!StringUtils.isBlank(value) && !"class".equals(properName)) {
							put.addColumn(Bytes.toBytes(column), Bytes.toBytes(properName), Bytes.toBytes(value));
						}
					}
			}
		}
		return put;
	}

	public <T> Object saveList(String tableName, List<Put> puts) throws IOException {
		if (puts == null || puts.isEmpty()) {
			return true;
		}
		Connection conn = hbaseUtils.getConnection();
		try (Table table = conn.getTable(TableName.valueOf(tableName))) {
			// StopWatch watch = new StopWatch();
			// watch.start();
			table.put(puts);
			// log.info("{} start", conn);
			// watch.stop();
			// log.info("{} used {}", conn, watch);
		}
		// if (watch.getTime() > parserConfigConstant.habseSpentTimeLog) {
		// long sum = puts.stream().mapToLong(put -> put.heapSize()).sum();
		// log.info("[PUT_SIZE] head size {}bytes", sum);
		// }

		return true;

	}





	/**
	 * 根据 rowkey 获取数据
	 *
	 * @param tableName
	 * @param rowKey
	 * @param <T>
	 *
	 * @return
	 *
	 * @throws Exception
	 */
	public <T> T get(String tableName, String rowKey, Class<T> z) throws Exception {
		if (StringUtils.isEmpty(rowKey)) {
			return null;
		}

		if (conn == null) {
			//初始化hbase连接
			conn = hbaseUtils.getConnection();
		}
		//获取表
		log.info("tablename:{}",tableName);
		Table tables = conn.getTable(TableName.valueOf(tableName));
		Get get = new Get(Bytes.toBytes(rowKey));
		Result result = tables.get(get);
		if (result == null) {
			return null;
		}
		List<Cell> ceList = result.listCells();
		if (ceList != null && ceList.size() > 0) {
			T t = z.newInstance();
			BeanWrapper beanWrapper = new BeanWrapperImpl(t);
			beanWrapper.setAutoGrowNestedPaths(true);
			for (Cell cell : ceList) {
				// BeanWrapper beanWrapper = PropertyAccessorFactory.forBeanPropertyAccess(t);
				String cellName = new String(CellUtil.cloneQualifier(cell));
				String strValue = new String(CellUtil.cloneValue(cell));
				beanWrapper.setPropertyValue(cellName, strValue);

			}
			tables.close();
			((LogEntity)t).setKey(rowKey);
			return t;
		}

		tables.close();
		return null;
	}




	@Retryable(maxAttempts = 1000, backoff = @Backoff(value = 5000), value = {RuntimeException.class})
	public <T> Map<String, T> parallelGet(String tableName, List<String> rowkeys, Class<T> z, String family) {
		if (CollectionUtils.isEmpty(rowkeys)) {
			return new HashMap<>();
		}
		if (conn == null) {
			//初始化hbase连接
			conn = hbaseUtils.getConnection();
		}

		// StopWatch watch = new StopWatch();
		// watch.start();

		try (Table table = conn.getTable(TableName.valueOf(tableName))) {

			// if (rowkeys.size() <= ParserConfigConstant.putSize) {
			// 	// Future<Result[]> results = batchGetByT(table, rowkeys, z);
			// 	Future<Result[]> results = hTableExecutor.batchGetByT(table,rowkeys,z);
			// 	Map<String, T> map = getResult2Map(results.get(), z);
			// 	watch.stop();
			// 	log.info("HBase get {} on {} used {}", rowkeys.size(), tableName, watch);
			// 	return map;
			// }
			List<List<String>> partition = Lists.partition(rowkeys, putSize);

			// List<Future<Result[]>> futureList = partition.stream().map(s -> {
			// 	return ThreadPoolUtil.submit(() -> {
			// 		List<Get> gets = rowKey2Gets(s);
			// 		Result[] results = table.get(gets);
			// 		return results;
			// 	});
			// }).collect(Collectors.toList());
			// log.info("future size {}", futureList.size());
			// List<Result[]> resultsList = new ArrayList<>(futureList.size());
			// for (Future<Result[]> future : futureList) {
			// 	Result[] results = future.get();
			// 	resultsList.add(results);
			// }

			List<Result[]> resultsList = partition.parallelStream().map(s -> {
				try {
					return batchGetByT(tableName, table, s, family);
				} catch (Exception e) {
					log.error("get hbase error", e);
					throw new RuntimeException(e);
				}
			}).collect(Collectors.toList());

			// watch.stop();
			// log.info("HBase parallel get {} from {} used {}", rowkeys.size(), tableName, watch);
			// watch.reset();
			// watch.start();
			List<Map<String, T>> mapList = resultsList.parallelStream().map(rs -> {
				try {
					return getResult2Map(rs, z);
				} catch (Exception e) {
					log.error("result trans to map", e);
					throw new RuntimeException(e);
				}
			}).collect(Collectors.toList());
			Map<String, T> resultMap = new HashMap<>(mapList.size() * putSize);
			for (Map<String, T> map : mapList) {
				resultMap.putAll(map);
			}

			// log.info("HBase results list {} to map used {}", resultMap.size(), watch);
			return resultMap;
		} catch (IOException e) {
			log.error("hbase get error, wait retry...", e);
			throw new RuntimeException(e);
		}

	}

	public List<Get> rowKey2Gets(List<String> rowkeys, byte[] bytes) {

		List<Get> collect = rowkeys.stream().map(s -> {
			Get get = new Get(Bytes.toBytes(s));
			if (bytes != null) {
				get.addFamily(bytes);
			}
			return get;
		}).collect(Collectors.toList());
		return collect;
	}

	/**
	 * hbaseTemplate 获取批量数据
	 *
	 * @param table
	 * @param rowkeys
	 *
	 * @return
	 */
	public Result[] batchGetByT(String tableName, Table table, List<String> rowkeys, String family) throws Exception {
		byte[] bytes = null;
		if (family != null) {
			bytes = family.getBytes();
		}
		List<Get> gets = rowKey2Gets(rowkeys, bytes);
		Result[] results = table.get(gets);

		// Result[] results = (Result[]) hbaseTemplate.execute(tableName, new TableCallback<Object>() {
		// 	@Override
		// 	public Object doInTable(HTableInterface table) throws Throwable {
		// 		Result[] results = table.get(gets);
		// 		return results;
		// 	}
		// });
		return results;
	}

	/**
	 * get result to map bean
	 *
	 * @param results
	 * @param <T>
	 *
	 * @return
	 */
	public <T> Map<String, T> getResult2Map(Result[] results,
			Class<T> z) throws IllegalAccessException, InstantiationException {
		if (results == null || results.length == 0) {
			return new HashMap<>();
		}
		Map<String, T> resultMap = new HashMap<>(results.length);
		//对结果集进行处理
		for (Result result : results) {
			if (result.getRow() == null) {
				continue;
			}
			String rowKey = new String(result.getRow());
			List<Cell> ceList = result.listCells();
			if (ceList != null && ceList.size() > 0) {
				T t = z.newInstance();
				BeanWrapper beanWrapper = new BeanWrapperImpl(t);
				beanWrapper.setAutoGrowNestedPaths(true);
				for (Cell cell : ceList) {
					// BeanWrapper beanWrapper = PropertyAccessorFactory.forBeanPropertyAccess(t);
					String cellName = new String(CellUtil.cloneQualifier(cell));
					String strValue = new String(CellUtil.cloneValue(cell));

						beanWrapper.setPropertyValue(cellName, strValue);
				}
				resultMap.put(rowKey, t);
			}
		}
		return resultMap;
	}


}

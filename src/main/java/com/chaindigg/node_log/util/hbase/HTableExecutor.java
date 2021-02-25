package com.chaindigg.node_log.util.hbase;

import java.util.List;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Component;

/**
 * @author zhifan_jyshi
 * @date 2020/6/24 10:56 上午
 */
@Component
public class HTableExecutor {

	@Async
	public <T> Future<Result[]> batchGetByT(Table table, List<String> rowkeys, Class<T> z) throws Exception {
		if (rowkeys == null || rowkeys.isEmpty()) {
			return new AsyncResult<>(new Result[1]);
		}
		List<Get> gets = rowKey2Gets(rowkeys);
		Result[] results = table.get(gets);
		return new AsyncResult<>(results);
	}

	public List<Get> rowKey2Gets(List<String> rowkeys) {
		List<Get> collect = rowkeys.stream().map(s -> {
			Get get = new Get(Bytes.toBytes(s));
			// get.addFamily(bytes);
			return get;
		}).collect(Collectors.toList()); return collect;
	}
}

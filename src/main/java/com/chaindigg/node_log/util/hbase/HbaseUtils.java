package com.chaindigg.node_log.util.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import javax.annotation.Resource;

import org.apache.commons.collections.ListUtils;
import org.apache.commons.lang.time.StopWatch;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.TableConfiguration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

/**
 * @Author sunhp
 * @Description 获取hbase链接
 * @Date 2020/2/5
 */
@Slf4j
@Component
public class HbaseUtils {

	@Resource
	private HBaseConfig config;
	private Connection connection;

	@Value("${parser.hbase.connection-pool-size:0}")
	private int maxConnection;
	private static List<Connection> connectionPool = ListUtils.synchronizedList(new ArrayList(24));
	private static final Random RANDOM = new Random();

	public void initConnection() throws IOException {
		if (connectionPool.size() >= maxConnection) {
			return;
		}
		log.info("init connection...");
		for (int i = 0; i < maxConnection; i++) {
			Connection connection = ConnectionFactory.createConnection(config.configuration());
			connectionPool.add(connection);
		}
		log.info("init connection success");
	}

	private Connection getConnection(int index) {
		return connectionPool.get(index);
	}

	public Connection createConnection() {
		try {
			StopWatch watch = new StopWatch();
			watch.start();
			Connection connection = ConnectionFactory.createConnection(config.configuration());
			watch.stop();
			log.info("create new connection success, used {}", watch);
			return connection;
		} catch (IOException e) {
			log.error("create connection error", e);
		}
		return null;
	}

	public Connection getConnection() {
		try {
			if (connection != null) {
				return connection;
			}
			synchronized (this) {
				if (connection != null) {
					return connection;
				}
				connection = ConnectionFactory.createConnection(config.configuration());
				String s = connection.getConfiguration().get(TableConfiguration.MAX_KEYVALUE_SIZE_KEY);
				log.info("connection get config {}, {}", TableConfiguration.MAX_KEYVALUE_SIZE_KEY, s);
			}
			return connection;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	public Admin getAdmin() {
		try {
			Connection connection = ConnectionFactory.createConnection(config.configuration());
			return connection.getAdmin();
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	public int getMaxConnection() {
		return maxConnection;
	}
}

package com.chaindigg.node_log.config;

import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;

import lombok.Data;

/**
 * @Author sunhp
 * @Description 读取前缀为hbase的配置文件
 * @Date 2020/2/5
 */
@Data
@ConfigurationProperties(prefix = "hbase")
public class HBaseProperties {
    private Map<String, String> config;
}

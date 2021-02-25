package com.chaindigg.node_log.util.hbase;

import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import com.chaindigg.node_log.config.HBaseProperties;

/**
 * @Author sunhp
 * @Description 获取hbase配置
 * @Date 2020/2/5
 */
@Configuration
@EnableConfigurationProperties(HBaseProperties.class)
public class HBaseConfig {

    private final HBaseProperties properties;

    public HBaseConfig(HBaseProperties properties) {
        this.properties = properties;
    }

    public org.apache.hadoop.conf.Configuration configuration() {
        org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
        Map<String, String> config = properties.getConfig();
        Set<String> keySet = config.keySet();
        for (String key : keySet) {
            configuration.set(key, config.get(key));
        }
        return configuration;
    }
}

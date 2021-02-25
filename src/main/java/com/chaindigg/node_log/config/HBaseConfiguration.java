package com.chaindigg.node_log.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.hadoop.hbase.HbaseTemplate;

/**
 * @author zhifan_jyshi
 * @date 2020/4/8 2:05 下午
 */
@Configuration
public class HBaseConfiguration {
    @Value("${hbase.config.hbase.zookeeper.property.clientPort}")
    private String clientPort;
    @Value("${hbase.config.hbase.zookeeper.quorum}")
    private String quorum;
    @Value("${hbase.config.hbase.client.keyvalue.maxsize}")
    private String size;

    @Bean
    public HbaseTemplate getHbaseTemplate() {//这里需要写Configuration全名,是因为这个类上有一个注解@Configuration,名称相同了
        org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
        configuration = org.apache.hadoop.hbase.HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", this.clientPort);//Hbase中zookeeper的端口号;默认是2181
        configuration.set("hbase.zookeeper.quorum", this.quorum);//hadoop的地址,这里需要在系统的host文件配置如:hadoop1,hadoop2,host文件中未:192.168.0.1 hadoop1  192.168.0.2 hadoop2
        configuration.set("hbase.client.keyvalue.maxsize",this.size);//hbase中size中默认大小10M
        HbaseTemplate ht = new HbaseTemplate(configuration);
        return ht;
    }
}

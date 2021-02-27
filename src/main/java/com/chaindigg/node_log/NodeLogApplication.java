package com.chaindigg.node_log;

import com.chaindigg.node_log.service.IHbaseService;
import com.chaindigg.node_log.service.ILogDataService;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

@SpringBootApplication
@EnableScheduling
public class NodeLogApplication {
  @Resource
  private IHbaseService logHbase;
  @Resource
  private ILogDataService logDataService;
  
  private static NodeLogApplication nodeLogApplication;
  
  @PostConstruct
  public void init() {
    nodeLogApplication = this;
    nodeLogApplication.logHbase = this.logHbase;
    nodeLogApplication.logDataService = this.logDataService;
  }
  
  public static void main(String[] args) {
    SpringApplication.run(NodeLogApplication.class, args);
    try {
      nodeLogApplication.logDataService.saveList("2021-01-01");
    } catch (Exception e) {
      e.printStackTrace();
    }

//    List<String> list = new ArrayList<>();
//    list.add("1126878911e8f401621f7d02d6a42fef66258e20f5b9055bd742d7b2a6d9cc6e,66.30.252.1281,beijing");
//    Map<String, LogEntity> map = nodeLogApplication.logHbase.batchGet(list);
//    System.out.println(map);
  }
}

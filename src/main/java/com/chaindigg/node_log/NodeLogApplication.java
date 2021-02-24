package com.chaindigg.node_log;

import com.chaindigg.node_log.service.Imp.ParseLog;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.io.IOException;

@SpringBootApplication
public class NodeLogApplication {
  
  @Resource
  private ParseLog parseLog;
  
  private static NodeLogApplication nodeLogApplication;
  
  @PostConstruct
  public void init() {
    nodeLogApplication = this;
    nodeLogApplication.parseLog = this.parseLog;
  }
  
  public static void main(String[] args) {
    SpringApplication.run(NodeLogApplication.class, args);
    try {
      nodeLogApplication.parseLog.parse();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}

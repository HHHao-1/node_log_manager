package com.chaindigg.node_log;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class NodeLogApplication {
  public static void main(String[] args) {
    SpringApplication.run(NodeLogApplication.class, args);
  }
}

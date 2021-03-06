//package com.chaindigg.node_log.service.impl;
//
//import java.util.ArrayList;
//import java.util.List;
//
//import org.junit.jupiter.api.Test;
//import org.springframework.beans.factory.annotation.Autowired;
//
//import com.chaindigg.node_log.NodeLogApplicationTests;
//import com.chaindigg.node_log.domain.entity.LogEntity;
//
//class LogHbaseImplTest extends NodeLogApplicationTests {
//
//  @Autowired
//  LogHbaseImpl logHbase;
//
//  @Test
//  void saveList() throws Exception {
//    ArrayList<LogEntity> logEntities = new ArrayList<>();
//    LogEntity logEntity1 = new LogEntity("1","test");
//    LogEntity logEntity2 = new LogEntity("3","test2");
//    logEntities.add(logEntity1);
//    logEntities.add(logEntity2);
//
//    logHbase.saveList(logEntities);
//
//  }
//
//  @Test
//  void get() throws Exception {
//    System.out.println(logHbase.get("1"));
//  }
//
//  @Test
//  void fuzzyScan() throws Exception {
//    ArrayList<String> rowKeys = new ArrayList<>();
//    rowKeys.add("ch");
//    rowKeys.add("ab");
//    List<LogEntity> stringLogEntityMap = null;
//    stringLogEntityMap = logHbase.fuzzyScan(rowKeys);
//
//    System.out.println(stringLogEntityMap);
//  }
//    @Test
//  void batchGet() throws Exception {
//    ArrayList<String> rowKeys = new ArrayList<>();
//    rowKeys.add("ch1");
//    rowKeys.add("ab");
//      System.out.println(logHbase.batchGet(rowKeys));
//
//  }
//
//
//}

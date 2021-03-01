package com.chaindigg.node_log.service.impl;

import com.chaindigg.node_log.constant.Constans;
import com.chaindigg.node_log.domain.entity.LogEntity;
import com.chaindigg.node_log.service.IHbaseService;
import com.chaindigg.node_log.service.ILogDataService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Scanner;
import java.util.stream.Collectors;

@Slf4j
@Service
public class LogDataImpl implements ILogDataService {
  
  @Value("${var.log.path}")
  private String logPath;
  
  @Value("${var.log.loop.batch}")
  private Integer batch;
  
  @Resource
  private IHbaseService hbaseService;
  
  @Override
  public void saveList(String date) throws IOException {
//    List<LogEntity> logEntities = new ArrayList<>();
    if (date == null || date.equals("")) {
      return;
    }
    Path path = Paths.get(logPath);
//    if (date != null) {
    File[] files = path.toFile().listFiles();
    if (files != null && files.length != 0) {
      for (File file : files) {
        if (Objects.equals(file.getName(), date)) {
          save(file);
        }
      }
    }
//    }
  }
  
  //  private List<LogEntity> fileParse(File file) throws IOException {
  private void save(File file) throws IOException {
    Files.walkFileTree(
        Paths.get(file.getPath()),
        new SimpleFileVisitor<Path>() {
          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
              throws IOException {
            List<LogEntity> logEntities = new ArrayList<>();
            if (file.toString().contains(".log")) {
              String nodeName = file.getParent().getFileName().toString();
              log.info("node: {}, file : {}", nodeName, file.getFileName().toString());
//              try (Stream<String> lines = Files.lines(file)) {
              try (Scanner sc = new Scanner(file)) {
                int sum = 0;
                while (sc.hasNextLine()) {
                  sum++;
                  log.info("loop {} start...,batch: {}", sum, batch);
                  List<String> lines = new ArrayList<>();
                  for (int i = 0; i < batch; i++) {
                    if (sc.hasNextLine()) {
                      lines.add(sc.nextLine());
                    }
                  }
                  lines.stream().collect(
                      Collectors.toMap(
                          k -> {
                            String ip = k.substring(k.indexOf("ip=") + 3, k.indexOf(","));
                            String txid = k.substring(k.indexOf("hash=") + 5);
                            return txid + Constans.ROWKEY_SPLICE + ip + Constans.ROWKEY_SPLICE + nodeName;
                          },
                          v -> v.substring(v.indexOf("receivedtime=") + 13, v.indexOf("Z,") + 1),
                          (oldVal, currVal) -> oldVal))
                      .forEach((k, v) -> logEntities.add(new LogEntity(k, v)));
                  List<String> rowKey =
                      logEntities.parallelStream().map(LogEntity::getKey).collect(Collectors.toList());
                  log.info("before connect hbase to batchGet");
                  logEntities.removeAll(hbaseService.batchGet(rowKey).values());
                  log.info("before save to hbase");
                  hbaseService.saveList(logEntities);
                  log.info("loop {} end", sum);
                }
              } catch (Exception e) {
                e.printStackTrace();
              }
//              return FileVisitResult.TERMINATE;
            }
            return FileVisitResult.CONTINUE;
          }
        });
  }

//  /***
//   * 日志解析后数据存入hbase
//   * @param date 格式例如: 2021-02-26
//   * @throws Exception 抛出异常
//   */
//  @Override
//  public void saveList(String date) throws Exception {
//    if (date == null || date.equals("")) {
//      return;
//    }
//    List<LogEntity> logEntities = parse(date);
//    // List<String> rowKey = new ArrayList<>();
//    // logEntities.forEach(s -> rowKey.add(s.getKey()));
//    List<String> rowKey =
//        logEntities.parallelStream().map(LogEntity::getKey).collect(Collectors.toList());
//    log.info("before connect hbase to batchGet");
//    log.info("equals: " + new LogEntity("123", "HHH").equals(new LogEntity("123", "HHH")));
//    log.info("equals: " + new LogEntity("aaa", "bbb").equals(new LogEntity("aaa", "HHH")));
//    log.info("equals: " + new LogEntity("aaa", "bbb").equals(new LogEntity("ccc", "HHH")));
////    logEntities.removeAll(hbaseService.batchGet(rowKey).values());
////    log.info("before save to hbase");
////    hbaseService.saveList(logEntities);
////    log.info("after save to hbase");
//  }
  
  @Override
  public LogEntity getOne(String rowKey) throws Exception {
    return hbaseService.get(rowKey);
//    return null;
  }
  
  @Override
  public List<LogEntity> batchGet(List<String> rowKeys) throws Exception {
    return hbaseService.fuzzyScan(rowKeys);
//    return null;
  }
  
  @Override
  //  @Scheduled(cron = "0 0 5 * * ?")
  @Scheduled(cron = "${var.cron.date}")
  public void schedule() {
    log.info("log save to hbase,star...");
    try {
      LocalDateTime date = LocalDateTime.now().minusDays(1);
      saveList(date.toString().substring(0, date.toString().indexOf("T")));
      log.info("log task end");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}

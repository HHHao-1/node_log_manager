package com.chaindigg.node_log.service.impl;

import com.chaindigg.node_log.constant.Constans;
import com.chaindigg.node_log.domain.entity.LogEntity;
import com.chaindigg.node_log.service.IHbaseService;
import com.chaindigg.node_log.service.ILogDataService;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
public class LogDataImpl implements ILogDataService {
  
  @Value("${var.log.path}")
  private String logPath;
  
  @Resource
  private IHbaseService hbaseService;
  
  @Override
  public List<LogEntity> parse(String date) throws IOException {
    List<LogEntity> logEntities = new ArrayList<>();
    Path path = Paths.get(logPath);
    if (date != null) {
      File[] files = path.toFile().listFiles();
      if (files != null && files.length != 0) {
        for (File file : files) {
          if (Objects.equals(file.getName(), date)) {
            logEntities = fileParse(file);
          }
        }
      }
    }
    return logEntities;
  }
  
  private List<LogEntity> fileParse(File file) throws IOException {
    List<LogEntity> logEntities = new ArrayList<>();
    Files.walkFileTree(
        Paths.get(file.getPath()),
        new SimpleFileVisitor<Path>() {
          // 先去遍历删除文件
          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
              throws IOException {
            if (file.toString().contains(".log")) {
              String nodeName = file.getParent().getFileName().toString();
              try (Stream<String> lines = Files.lines(file)) {
                lines.collect(
                    Collectors.toMap(
                        k -> {
                          String ip = k.substring(k.indexOf("ip=") + 3, k.indexOf(","));
                          String txid = k.substring(k.indexOf("hash=") + 5);
                          return txid + Constans.ROWKEY_SPLICE + ip + Constans.ROWKEY_SPLICE + nodeName;
                        },
                        v -> v.substring(v.indexOf("receivedtime=") + 13, v.indexOf("Z,") + 1),
                        (oldVal, currVal) -> oldVal))
                    .forEach((k, v) -> logEntities.add(new LogEntity(k, v)));
              }
            }
            return FileVisitResult.CONTINUE;
          }
        });
    return logEntities;
  }
  
  /***
   * 日志解析后数据存入hbase
   * @param date 格式例如: 2021-02-26
   * @throws Exception 抛出异常
   */
  @Override
  public void saveList(String date) throws Exception {
    if (date == null || date.equals("")) {
      return;
    }
    List<LogEntity> logEntities = parse(date);
    List<String> rowKey = new ArrayList<>();
    logEntities.forEach(s -> rowKey.add(s.getKey()));
    logEntities.removeAll(hbaseService.batchGet(rowKey).values());
    hbaseService.saveList(logEntities);
  }
  
  @Override
  public LogEntity getOne(String rowKey) throws Exception {
    return hbaseService.get(rowKey);
  }
  
  @Override
  public List<LogEntity> batchGet(List<String> rowKeys) throws Exception {
    return hbaseService.fuzzyScan(rowKeys);
  }
  
  @Override
  //  @Scheduled(cron = "0 0 5 * * ?")
  @Scheduled(cron = "${var.cron.date}")
  public void schedule() {
    try {
      LocalDateTime date = LocalDateTime.now().minusDays(1);
      saveList(date.toString().substring(0, date.toString().indexOf("T")));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}

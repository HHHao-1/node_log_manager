package com.chaindigg.node_log.service.impl;

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
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
public class LogDataImpl implements ILogDataService {
  
  @Value("${var.cron.run}")
  private Boolean run;
  
  @Value("${var.log.path}")
  private String logPath;
  
  @Value("${var.loop.batch}")
  private Integer batch;
  
  @Value("${var.rowkey.splice}")
  private String rowKeySplice;
  
  // region data
  @Value("${var.log.string.txid.start}")
  private String txidStart;
  
  @Value("${var.log.string.ip.start}")
  private String ipStart;
  @Value("${var.log.string.ip.end}")
  private String ipEnd;
  
  @Value("${var.log.string.time.start}")
  private String timeStart;
  @Value("${var.log.string.time.end}")
  private String timeEnd;
  
  @Value("${var.log.string.ip.addEndIndex}")
  private Integer ipAddEndIndex;
  @Value("${var.log.string.time.addEndIndex}")
  private Integer timeAddEndIndex;
  // endregion
  
  @Resource
  private IHbaseService hbaseService;
  
  private List<String> lines = new ArrayList<>();
  private List<LogEntity> logEntities = new ArrayList<>();
  
  @Override
  public void saveList(String date) throws IOException {
    if (date == null || date.equals("")) {
      return;
    }
    Path path = Paths.get(logPath);
    File[] files = path.toFile().listFiles();
    if (files != null && files.length != 0) {
      for (File file : files) {
        if (Objects.equals(file.getName(), date)) {
          save(file);
        }
      }
    }
  }
  
  private void save(File file) throws IOException {
    Files.walkFileTree(
        Paths.get(file.getPath()),
        new SimpleFileVisitor<Path>() {
          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
            if (file.toString().contains(".log")) {
              String nodeName = file.getParent().getFileName().toString();
              log.info("node: {}, file : {}", nodeName, file.getFileName().toString());
              try (Scanner sc = new Scanner(file)) {
                int sum = 0;
                int flag = 0;
                while (sc.hasNextLine()) {
                  flag++;
                  lines.add(sc.nextLine());
                  if (flag >= batch || !sc.hasNextLine()) {
                    sum++;
                    buildDataToHbase(nodeName);
                    matchSave(sum);
                    flag = 0;
                    lines = new ArrayList<>();
                    logEntities = new ArrayList<>();
                  }
                }
              } catch (Exception e) {
                e.printStackTrace();
              }
            }
            return FileVisitResult.CONTINUE;
          }
        });
  }
  
  private void matchSave(int sum) throws Exception {
    log.info("loop {} start...,batch: {}", sum, batch);
    List<String> rowKey =
        logEntities.parallelStream().map(LogEntity::getKey).collect(Collectors.toList());
    log.info("before connect hbase to batchGet,total = {}", rowKey.size());
    Map<String, LogEntity> stringLogEntityMap = hbaseService.batchGet(rowKey);
    log.info("after connect hbase to batchGet");
    Set<String> keys = stringLogEntityMap.keySet();
    log.info("before removeAll");
    List<LogEntity> logEntityList =
        logEntities.parallelStream().filter(s -> !keys.contains(s.getKey())).collect(Collectors.toList());
    log.info("before save to hbase");
    hbaseService.saveList(logEntityList);
    log.info("loop {} end", sum);
  }
  
  private void buildDataToHbase(String nodeName) {
    lines.stream().collect(
        Collectors.toMap(
            k -> {
              try {
                String ip = k.substring(k.indexOf(ipStart) + ipStart.length(),
                    k.indexOf(ipEnd) + ipAddEndIndex);
                String txid = k.substring(k.indexOf(txidStart) + txidStart.length());
                return txid + rowKeySplice + ip + rowKeySplice + nodeName;
              } catch (Exception e) {
                e.printStackTrace();
                log.info("hbase---rowKey字符串构造异常");
                return "rowKeyError";
              }
            },
            v -> {
              try {
                return v.substring(v.indexOf(timeStart) + timeStart.length(),
                    v.indexOf(timeEnd) + timeAddEndIndex);
              } catch (Exception e) {
                e.printStackTrace();
                log.info("hbase--value字符串构造异常");
                return "valueError";
              }
            },
            (oldVal, currVal) -> oldVal))
        .forEach((k, v) -> {
          if (!Objects.equals(k, "rowKeyError") || !Objects.equals(v, "valueError")) {
            logEntities.add(new LogEntity(k, v));
          }
        });
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
    if (!run) return;
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

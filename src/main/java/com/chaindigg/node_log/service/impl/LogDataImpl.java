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

  @Value("${var.log.path}")
  private String logPath;

  @Value("${var.log.loop.batch}")
  private Integer batch;

  @Value("${var.log.string.rowKeySplice}")
  private String rowKeySplice;

  // region data
  @Value("${var.log.string.txid.start}")
  private String txidStart;
  @Value("${var.log.string.txid.end}")
  private String txidEnd;

  @Value("${var.log.string.ip.start}")
  private String ipStart;
  @Value("${var.log.string.ip.end}")
  private String ipEnd;

  @Value("${var.log.string.time.start}")
  private String timeStart;
  @Value("${var.log.string.time.end}")
  private String timeEnd;

  @Value("${var.log.string.txid.addEndIndex}")
  private Integer txidAddEndIndex;
  @Value("${var.log.string.ip.addEndIndex}")
  private Integer ipAddEndIndex;
  @Value("${var.log.string.time.addEndIndex}")
  private Integer timeAddEndIndex;
  // endregion

  @Resource
  private IHbaseService hbaseService;

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
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
              throws IOException {
            List<LogEntity> logEntities = new ArrayList<>();
            if (file.toString().contains(".log")) {
              String nodeName = file.getParent().getFileName().toString();
              log.info("node: {}, file : {}", nodeName, file.getFileName().toString());
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
                            String ip = k.substring(k.indexOf(ipStart) + ipStart.length(),
                                k.indexOf(ipEnd) + ipAddEndIndex);
                            String txid = k.substring(k.indexOf(txidStart) + txidStart.length());
                            return txid + rowKeySplice + ip + rowKeySplice + nodeName;
                          },
                          v -> v.substring(v.indexOf(timeStart) + timeStart.length(),
                              v.indexOf(timeEnd) + timeAddEndIndex),
                          (oldVal, currVal) -> oldVal))
                      .forEach((k, v) -> logEntities.add(new LogEntity(k, v)));
                  List<String> rowKey =
                      logEntities.parallelStream().map(LogEntity::getKey).collect(Collectors.toList());
                  log.info("before connect hbase to batchGet,total = {}",rowKey.size());
                  Map<String, LogEntity> stringLogEntityMap = hbaseService.batchGet(rowKey);
                  log.info("after connect hbase to batchGet");
                  Set<String> keys = stringLogEntityMap.keySet();
                  log.info("before removeAll");
                  List<LogEntity> logEntityList =
                      logEntities.parallelStream().filter(s -> !keys.contains(s.getKey())).collect(Collectors.toList());
//                  logEntities.removeAll(values);
                  log.info("before save to hbase");
//                  hbaseService.saveList(logEntities);
                  hbaseService.saveList(logEntityList);
                  log.info("loop {} end", sum);
                }
              } catch (Exception e) {
                e.printStackTrace();
              }
            }
            return FileVisitResult.CONTINUE;
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

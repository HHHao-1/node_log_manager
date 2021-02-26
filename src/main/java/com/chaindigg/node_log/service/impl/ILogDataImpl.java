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
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Scanner;

@Service
public class ILogDataImpl implements ILogDataService {

  @Value("${var.log.path}")
  private String logPath;

  @Resource private IHbaseService hbaseService;

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
            if (!file.toString().contains(".DS_Store")) {
              String nodeName = file.getParent().getFileName().toString();
              try (Scanner sc = new Scanner(new FileReader(file.toString()))) {
                //                for (int i = 0; i < 15; i++) {
                //                  if (sc.hasNextLine()) {
                while (sc.hasNextLine()) {
                  String line = sc.nextLine();
                  String ip = line.substring(line.indexOf("ip=") + 3, line.indexOf(","));
                  String txid = line.substring(line.indexOf("hash=") + 5);
                  String timestamp =
                      line.substring(line.indexOf("receivedtime=") + 13, line.indexOf("Z,") + 1);
                  String rowKey =
                      txid
                          + Constans.HBASE_ROWKEY_SPLICE
                          + ip
                          + Constans.HBASE_ROWKEY_SPLICE
                          + nodeName;
                  LogEntity logEntity = new LogEntity(rowKey, timestamp);
                  logEntities.add(logEntity);
                  //                  }
                }
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
   * @throws Exception
   */
  @Override
  public void saveList(String date) throws Exception {
    List<LogEntity> logSaveList = new ArrayList<>();
    if (date != null) {
      List<LogEntity> logEntities = parse(date);
      for (int i = 0; i < logEntities.size(); i++) {
        if (getOne(logEntities.get(i).getKey()) == null) {
          logSaveList.add(logEntities.get(i));
        }
      }
    }
    hbaseService.saveList(logSaveList);
  }

  @Override
  public LogEntity getOne(String rowKey) throws Exception {
    return hbaseService.get(rowKey);
  }

  @Override
  public List<LogEntity> batchGet(List<String> rowKeys) throws Exception {
    List<LogEntity> list = hbaseService.fuzzyScan(rowKeys);
    return list;
  }

  @Override
  @Scheduled(cron = "0 0 23 * * ?")
  public void schedule() {
    try {
      LocalDateTime date = LocalDateTime.now();
      saveList(date.toString().substring(0, date.toString().indexOf("T")));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}

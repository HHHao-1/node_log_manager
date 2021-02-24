package com.chaindigg.node_log.service.Imp;

import com.chaindigg.node_log.constant.Constans;
import com.chaindigg.node_log.service.Parse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

@Service
public class ParseLog implements Parse {
  @Value("${var.log.path}")
  private String logPath;
  
  @Override
  public void parse() throws IOException {
    Path path = Paths.get(logPath);
    Files.walkFileTree(
        path,
        new SimpleFileVisitor<Path>() {
          // 先去遍历删除文件
          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
              throws IOException {
            
            if (!file.toString().contains(".DS_Store")) {
              String nodeName = file.getParent().getFileName().toString();
              try (Scanner sc = new Scanner(new FileReader(file.toString()))) {
                for (int i = 0; i < 10; i++) {
                  if (sc.hasNextLine()) {
                    String line = sc.nextLine();
                    String ip = line.substring(line.indexOf("ip=") + 3, line.indexOf(","));
                    String txid = line.substring(line.indexOf("hash=") + 5);
                    String timestamp =
                        line.substring(line.indexOf("receivedtime=") + 13, line.indexOf("Z,") + 1);
                    String rowKey = txid + Constans.HBASE_ROWKEY_SPLICE + ip + Constans.HBASE_ROWKEY_SPLICE + nodeName;
                    Map<String, String> map = new HashMap<>();
                    map.put(rowKey, timestamp);
                    System.out.println(map);
                  }
                }
              }
            }
            return FileVisitResult.CONTINUE;
          }
        });
  }
}

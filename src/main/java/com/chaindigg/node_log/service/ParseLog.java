package com.chaindigg.node_log.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;

@Service
public class ParseLog {
  @Value("${var.log.path}")
  private String logPath;

  public void parse() throws IOException {
    Path path = Paths.get(logPath);
    Files.walkFileTree(
        path,
        new SimpleFileVisitor<Path>() {
          // 先去遍历删除文件
          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
              throws IOException {
            //            if (file.toString() != ".DS_Store") {}

            System.out.println(file);
            return FileVisitResult.CONTINUE;
          }
        });
    //    try(Stream<String> lines = Files.lines(path)){
    //      lines.forEach(System.out::println);
    //    }
    //    try (Scanner sc = new Scanner(new FileReader(logPath))) { // 字符流
    //      //      按行读取字符串
    //      for (int i = 0; i < 10; i++) {
    //        //      while (sc.hasNextLine()) {
    //        String line = sc.nextLine();
    //        System.out.println(line);
    //      }
    //    }
  }
}

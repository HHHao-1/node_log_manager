package com.chaindigg.node_log.controller;

import com.chaindigg.node_log.domain.entity.LogEntity;
import com.chaindigg.node_log.service.ILogDataService;
import com.chaindigg.node_log.util.ApiResponse;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.List;

@RestController
public class LogDataController {
  @Resource
  private ILogDataService logDataService;
  
  @PostMapping("/save")
  public ApiResponse save(String date) {
    try {
      logDataService.saveList(date);
      return ApiResponse.success("存入hbase成功");
    } catch (Exception e) {
      e.printStackTrace();
      return ApiResponse.fail();
    }
  }
  
  @GetMapping("/query")
  public ApiResponse query(@RequestBody List<String> rowKeys) {
    if (rowKeys == null || rowKeys.size() == 0) {
      return ApiResponse.fail();
    }
    try {
      List<LogEntity> logEntities = logDataService.batchGet(rowKeys);
      if (logEntities == null || logEntities.size() == 0) {
        return ApiResponse.fail();
      }
      return ApiResponse.success(logDataService.batchGet(rowKeys));
    } catch (Exception e) {
      e.printStackTrace();
      return ApiResponse.fail();
    }
  }
}

package com.chaindigg.node_log.controller;

import com.chaindigg.node_log.constant.ResponseStateEnum;
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
      return ApiResponse.create(ResponseStateEnum.SUCCESS, "存入hbase成功");
    } catch (Exception e) {
      e.printStackTrace();
      return ApiResponse.create(ResponseStateEnum.FAIL);
    }
  }
  
  @GetMapping("/query")
  public ApiResponse query(@RequestBody List<String> rowKeys) {
    try {
      if (rowKeys != null) {
        if (logDataService.batchGet(rowKeys).size() != 0) {
          return ApiResponse.create(ResponseStateEnum.SUCCESS, logDataService.batchGet(rowKeys));
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      return ApiResponse.create(ResponseStateEnum.FAIL);
    }
    return ApiResponse.create(ResponseStateEnum.FAIL);
  }
}

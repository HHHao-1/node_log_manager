package com.chaindigg.node_log.controller;

import com.chaindigg.node_log.constant.ResponseStateEnum;
import com.chaindigg.node_log.service.Parse;
import com.chaindigg.node_log.util.ApiResponse;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.io.IOException;

@RestController
public class LogData {
  @Resource
  private Parse parse;
  
  @PostMapping("/save")
  public ApiResponse save() {
    try {
      parse.parse();
      return ApiResponse.create(ResponseStateEnum.SUCCESS, "存入hbase成功");
    } catch (IOException e) {
      e.printStackTrace();
      return ApiResponse.create(ResponseStateEnum.FAIL);
    }
  }
  
  @PostMapping("/query")
  public ApiResponse query() {
    return null;
  }
}

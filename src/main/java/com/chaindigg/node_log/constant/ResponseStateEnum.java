package com.chaindigg.node_log.constant;

public enum ResponseStateEnum {
  // 状态码
  FAIL("0001", "请求失败"),
  SUCCESS("0000", "请求成功");
  
  ResponseStateEnum(String code, String message) {
    this.code = code;
    this.message = message;
  }
  
  public String code;
  
  public String message;
}

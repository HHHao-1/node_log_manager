package com.chaindigg.node_log.constant;

public enum ResponseStateEnum {
  // 状态码
  FAIL(1000, "请求失败"),
  SUCCESS(1001, "请求成功"),
  EXISTED(1002, "数据已存在");

  ResponseStateEnum(Integer code, String message) {
    this.code = code;
    this.message = message;
  }

  public Integer code;

  public String message;
}

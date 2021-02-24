package com.chaindigg.node_log.util;

import com.chaindigg.node_log.enums.State;
import lombok.Data;

@Data
public class ApiResponse {

  private Integer code;
  private String msg;
  private Object data;

  public static ApiResponse create(State s, Object... t) {
    ApiResponse apiResponse = new ApiResponse();
    apiResponse.code = s.code;
    apiResponse.msg = s.message;
    if (t != null && t.length != 0) {
      apiResponse.data = t;
    } else {
      apiResponse.data = null;
    }
    return apiResponse;
  }
}

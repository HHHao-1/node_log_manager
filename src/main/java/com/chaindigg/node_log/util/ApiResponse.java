package com.chaindigg.node_log.util;

import com.chaindigg.node_log.constant.ResponseStateEnum;
import lombok.Data;

@Data
public class ApiResponse {
  
  private Integer code;
  private String msg;
  private Object data;
  
  public static ApiResponse fail() {
    ApiResponse apiResponse = new ApiResponse();
    apiResponse.code = ResponseStateEnum.FAIL.code;
    apiResponse.msg = ResponseStateEnum.FAIL.message;
    apiResponse.data = null;
    return apiResponse;
  }
  
  public static ApiResponse success(Object data) {
    ApiResponse apiResponse = new ApiResponse();
    apiResponse.code = ResponseStateEnum.SUCCESS.code;
    apiResponse.msg = ResponseStateEnum.SUCCESS.message;
    apiResponse.data = data;
    return apiResponse;
  }

//  public static ApiResponse create(ResponseStateEnum s, Object... t) {
//    ApiResponse apiResponse = new ApiResponse();
//    apiResponse.code = s.code;
//    apiResponse.msg = s.message;
//    if (t != null && t.length != 0) {
//      apiResponse.data = t;
//    } else {
//      apiResponse.data = null;
//    }
//    return apiResponse;
//  }
}

package com.chaindigg.node_log.service;

import com.chaindigg.node_log.domain.entity.LogEntity;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface ILogDataService {
  List<LogEntity> parse(String date) throws IOException;
  
  void saveList(String date) throws Exception;
  
  LogEntity getOne(String rowKey) throws Exception;
  
  Map<String, LogEntity> batchGet(List<String> rowKeys);
  
  void schedule();
}

package com.chaindigg.node_log.service;

import com.chaindigg.node_log.domain.entity.LogEntity;

import java.util.List;

public interface ILogDataService {
//  List<LogEntity> parse(String date) throws IOException;
  
  void saveList(String date) throws Exception;
  
  LogEntity getOne(String rowKey) throws Exception;
  
  List<LogEntity> batchGet(List<String> rowKeys) throws Exception;
  
  void schedule();
}

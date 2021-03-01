package com.chaindigg.node_log.domain.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Objects;

/**
 * @author ssliu
 * @date 2021-02-24
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class LogEntity {
  private String key;
  private String timestamp;
  
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof LogEntity)) return false;
    LogEntity logEntity = (LogEntity) o;
    return Objects.equals(key, logEntity.key);
  }
  
  @Override
  public int hashCode() {
    return Objects.hash(key, timestamp);
  }
}

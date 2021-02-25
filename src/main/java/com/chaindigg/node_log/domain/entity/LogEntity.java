package com.chaindigg.node_log.domain.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

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
}

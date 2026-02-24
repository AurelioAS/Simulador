/**
 * 
 */
package com.simulador.config;

import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;
import lombok.Getter;


@Getter
public class MQConnectionBundle {
  private final MQQueueManager qm;
  private final MQQueue        queue;

  public MQConnectionBundle(MQQueueManager qm, MQQueue queue) {
    this.qm = qm;
    this.queue = queue;
  }
  // Getters...
}

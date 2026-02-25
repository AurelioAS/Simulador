/**
 * 
 */
package com.simulador.config;

import com.ibm.mq.MQException;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.MQConstants;
import com.simulador.config.SimulatorProperties.AutoResponse;
import com.simulador.config.SimulatorProperties.QueueConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@Slf4j
public class MQMultiConnectionConfig {

  SimulatorProperties props;

  @Bean(name = "mqConnections")
  public ConcurrentMap<String, MQConnectionBundle> mqConnections(SimulatorProperties props,
      MQNativeConfig config) {

    ConcurrentMap<String, MQConnectionBundle> connectionMap = new ConcurrentHashMap<>();
    final MQQueueManager[] qMan = new MQQueueManager[1];
    try {
      MQQueueManager qmTemp = config.createConnection("SIMU");
      qMan[0] = qmTemp;
    } catch (MQException e) {
      e.printStackTrace();
    }
    Map<String, AutoResponse> responses = props.getAutoResponses();
    List<String> res = new ArrayList<>();
    responses.forEach((key, value) -> {
      SimulatorProperties.QueueConfig sourceQ = props.getQueues().get(key);
      log.info("key res: " + sourceQ.getName());
      res.add(sourceQ.getName());
    });

    Map<String, String> consumerQueue = props.getConsumers();
    consumerQueue.forEach((key, consumeRule) -> {
      SimulatorProperties.QueueConfig sourceQ = props.getQueues().get(consumeRule);
      log.info("key con: " + sourceQ.getName());
      res.add(sourceQ.getName());
    });

    props.getQueues().entrySet().forEach((entry) -> {
      QueueConfig qConfig = entry.getValue();
      String queueName = qConfig.getName();
      try {
        // Forzamos la creación de una conexión física NUEVA por cada iteración
        MQQueueManager qm = config.createConnection(queueName);
        int options = 0;
        if (res.contains(queueName)) {
          options = MQConstants.MQOO_INPUT_AS_Q_DEF | MQConstants.MQOO_INQUIRE
              | MQConstants.MQOO_FAIL_IF_QUIESCING;
        } else {
          options = MQConstants.MQOO_OUTPUT | MQConstants.MQOO_INQUIRE
              | MQConstants.MQOO_FAIL_IF_QUIESCING;
        }
        MQQueue queue = qm.accessQueue(queueName, options);

        connectionMap.put(queueName, new MQConnectionBundle(qm, queue));
        // log.info("Conexión independiente establecida para la cola: {}", queueName);
      } catch (MQException e) {
        log.error("Error conectando a la cola {}", queueName, e);
      }
    });
    log.info("Conexión independiente establecida para las colas: {}", connectionMap.keySet());
    return connectionMap;
  }
}

/**
 * 
 */
package com.simulador.config;

import com.ibm.mq.MQException;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.MQConstants;
import com.simulador.config.SimulatorProperties.QueueConfig;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
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
    List<String> res = props.getGetQueues();
    // Map<String, AutoResponse> responses = props.getAutoResponses();
    // if (responses != null) {
    // responses.forEach((key, value) -> {
    // SimulatorProperties.QueueConfig sourceQ = props.getQueues().get(key);
    // log.info("key res: " + sourceQ.getName());
    // res.add(sourceQ.getName());
    // });
    // }
    //
    // Map<String, String> consumerQueue = props.getConsumers();
    // if (consumerQueue != null) {
    // consumerQueue.forEach((key, consumeRule) -> {
    // SimulatorProperties.QueueConfig sourceQ = props.getQueues().get(consumeRule);
    // log.info("key con: " + sourceQ.getName());
    // res.add(sourceQ.getName());
    // });
    // }

    props.getQueues().entrySet().forEach((entry) -> {
      QueueConfig qConfig = entry.getValue();
      String queueName = qConfig.getName();
      try {
        // Forzamos la creación de una conexión física NUEVA por cada iteración
        MQQueueManager qm = config.createConnection(queueName);
        int options = 0;
        if (res.size() > 0 && res.contains(queueName)) {
          log.info("Cola a conectar get/put : '{}'", queueName);
          options = MQConstants.MQOO_OUTPUT | MQConstants.MQOO_INPUT_AS_Q_DEF
              | MQConstants.MQOO_INQUIRE | MQConstants.MQOO_FAIL_IF_QUIESCING
              | MQConstants.MQPMO_SET_ALL_CONTEXT | MQConstants.MQOO_SET_ALL_CONTEXT;
        } else {
          options = MQConstants.MQOO_OUTPUT | MQConstants.MQOO_INQUIRE
              | MQConstants.MQOO_FAIL_IF_QUIESCING | MQConstants.MQPMO_SET_ALL_CONTEXT;
        }
        MQQueue queue = qm.accessQueue(queueName, options);
        connectionMap.put(queueName, new MQConnectionBundle(qm, queue));
      } catch (MQException e) {
        if (e.getReason() == 2538) {
          throw new RuntimeException("Error al conectar al servidor MQ.");
        } else {
          log.error("Error conectando a la cola {}", queueName, e);
        }
      }
    });
    List<String> listaConComillas = connectionMap.keySet().stream()
        .map(key -> "'" + key + "'")
        .collect(Collectors.toList());
    log.info("Conexión independiente establecida para las colas: {}", listaConComillas);
    return connectionMap;
  }
}

package com.simulador.components;

import com.ibm.mq.MQException;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.MQConstants;
import com.simulador.config.SimulatorProperties;
import com.simulador.config.SimulatorProperties.QueueConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class MqPoolManager {

  @Autowired
  private MQQueueManager qMgr;

  @Autowired
  private SimulatorProperties props;

  private final Map<String, List<MQQueue>> putPool    = new ConcurrentHashMap<>();
  private final List<MQQueue>              getPool    = new ArrayList<>();
  private final List<MQQueue>              getAnsPool = new ArrayList<>();
  private int                              POOL_SIZE  = 1;
  public AtomicInteger                     putIndex   = new AtomicInteger(0);

  @PostConstruct
  public void init() throws MQException {
    // Configuración para PUT (Salida)
    int putOpts = MQConstants.MQOO_OUTPUT | MQConstants.MQOO_INQUIRE;
    // Configuración para GET (Entrada Compartida)
    int getOpts = MQConstants.MQOO_INPUT_SHARED
        | MQConstants.MQOO_INQUIRE
        | MQConstants.MQOO_FAIL_IF_QUIESCING;

    POOL_SIZE = props.getNumThreads() > 0 ? props.getNumThreads() : POOL_SIZE;
    if (props.getConsumers() != null) {
      for (int i = 0; i < POOL_SIZE; i++) {
        props.getConsumers().entrySet().forEach((entry) -> {
          SimulatorProperties.QueueConfig qConfig = props.getQueues().get(entry.getValue());
          log.info("Queue to pool Consumers: " + qConfig.getName());
          try {
            getPool.add(qMgr.accessQueue(qConfig.getName(), getOpts));
          } catch (MQException e) {
            e.printStackTrace();
          }
        });
      }
    }

    if (props.getAutoResponses() != null) {
      for (int i = 0; i < POOL_SIZE; i++) {
        // putPool.add(qmgr.accessQueue("COLA.SALIDA", putOpts));
        final int x = i;
        props.getAutoResponses().entrySet().forEach((entry) -> {
          SimulatorProperties.AutoResponse qConfig =
              props.getAutoResponses().get(entry.getValue());
          String queue = props.getQueues().get(entry.getValue().getTargetQueue()).getName();
          log.info("Queue to pool AutoResponses-" + x + ": " + queue);
          try {
            getAnsPool.add(qMgr.accessQueue(queue, getOpts));
          } catch (MQException e) {
            e.printStackTrace();
          }
        });
      }
    }

    // Definimos los permisos: Entrada (leer) + Salida (escribir)
    int openOptions = MQConstants.MQOO_INPUT_AS_Q_DEF  // Abrir para leer
        | MQConstants.MQOO_OUTPUT          // Abrir para escribir
        | MQConstants.MQOO_FAIL_IF_QUIESCING; // Fallar si el gestor se está parando

    for (int i = 0; i < POOL_SIZE; i++) {
      // putPool.add(qmgr.accessQueue("COLA.SALIDA", putOpts));
      final int x = i;
      props.getQueues().entrySet().forEach((entry) -> {
        QueueConfig qConfig = entry.getValue();
        String queue = qConfig.getName();
        log.info("Queue to pool of Puts amd Get-" + x + ": " + queue);
        try {
          putPool.computeIfAbsent(queue, k -> new ArrayList<>())
              .add(qMgr.accessQueue(queue, openOptions));

        } catch (MQException e) {
          e.printStackTrace();
        }
      });
    }

  }

  // ROUND ROBIN para PUT: Reparte los mensajes entre las 5 instancias
  public MQQueue getNextPutQueue(String queue) {
    int index = putIndex.getAndIncrement();
    return putPool.get(queue).get(index % POOL_SIZE);
  }

  // ASIGNACIÓN FIJA para GET: Cada hilo mantiene su propio objeto de cola
  public MQQueue getGetQueueByIndex(int index) {
    return getPool.get(index % POOL_SIZE);
  }

  public MQQueue getGetAnsQueueByIndex(int index) {
    return getAnsPool.get(index % POOL_SIZE);
  }

}

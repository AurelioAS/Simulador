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
import org.springframework.beans.factory.annotation.Qualifier;

// @Component
@Slf4j
public class MqPoolManager {

  private MQQueueManager qMgr;

  private MQQueueManager qMgrPut;

  public MqPoolManager(@Qualifier("mqConsumer") MQQueueManager qMgr,
      @Qualifier("mqProducer") MQQueueManager qMgrPut) {
    super();
    this.qMgr = qMgr;
    this.qMgrPut = qMgrPut;
  }

  @Autowired
  private SimulatorProperties props;

  private final Map<String, List<MQQueue>> putPool    = new ConcurrentHashMap<>();
  private final List<MQQueue>              getPool    = new ArrayList<>();
  private final List<MQQueue>              getAnsPool = new ArrayList<>();
  private int                              POOL_SIZE  = 1;
  public AtomicInteger                     putIndex   = new AtomicInteger(0);
  private final List<String>               queues     = new ArrayList<>();

  @PostConstruct
  public void init() throws MQException {
    // Configuración para PUT (Salida)
    int putOpts = MQConstants.MQOO_OUTPUT | MQConstants.MQOO_INQUIRE;
    // Configuración para GET (Entrada Compartida)
    int getOpts =
        MQConstants.MQOO_INPUT_AS_Q_DEF | MQConstants.MQOO_FAIL_IF_QUIESCING;

    POOL_SIZE = props.getNumThreads() > 0 ? props.getNumThreads() : POOL_SIZE;
    if (props.getConsumers() != null) {
      for (int i = 0; i < POOL_SIZE; i++) {
        final int x = i;
        props.getConsumers().entrySet().forEach((entry) -> {
          SimulatorProperties.QueueConfig qConfig = props.getQueues().get(entry.getValue());
          try {
            String name = qMgr.getName();
            log.info("Queue to pool Consumers-" + x + ": {} - {}", qConfig.getName(), name);
            getPool.add(qMgr.accessQueue(qConfig.getName(), getOpts));
            queues.add(qConfig.getName());
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
          try {
            String name = qMgr.getName();
            QueueConfig queue = props.getQueues().get(entry.getKey());
            log.info("Queue to pool AutoResponses-" + x + ": {} - {}", queue.getName(), name);
            getAnsPool.add(qMgr.accessQueue(queue.getName(), getOpts));
            queues.add(queue.getName());
          } catch (MQException e) {
            e.printStackTrace();
          }
        });
      }
    }

    // Definimos los permisos: Salida (escribir)
    int openOptions = MQConstants.MQOO_OUTPUT          // Abrir para escribir
        | MQConstants.MQOO_FAIL_IF_QUIESCING; // Fallar si el gestor se está parando

    for (int i = 0; i < POOL_SIZE; i++) {
      final int x = i;
      props.getQueues().entrySet().forEach((entry) -> {
        QueueConfig qConfig = entry.getValue();
        String queue = qConfig.getName();
        try {
          if (!queues.contains(queue)) {
            String name = qMgrPut.getName();
            log.info("Queue to pool of Puts-" + x + ": {} - {}", queue, name);
            putPool.computeIfAbsent(queue, k -> new ArrayList<>())
                .add(qMgrPut.accessQueue(queue, openOptions));
          }
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

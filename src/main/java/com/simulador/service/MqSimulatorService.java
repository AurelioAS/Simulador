package com.simulador.service;

import com.ibm.mq.MQException;
import com.ibm.mq.MQGetMessageOptions;
import com.ibm.mq.MQMessage;
import com.ibm.mq.MQPutMessageOptions;
import com.ibm.mq.MQQueue;
import com.ibm.mq.constants.MQConstants;
import com.simulador.components.JsonService;
import com.simulador.components.MessagesMgr;
import com.simulador.config.MQConnectionBundle;
import com.simulador.config.SimulatorProperties;
import com.simulador.config.SimulatorProperties.AutoResponse;
import com.simulador.config.SimulatorProperties.QueueConfig;
import com.simulador.utils.Utils;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@DependsOn("mqConnections")
public class MqSimulatorService extends Utils {

  private final SimulatorProperties      props;

  private Map<String, MQConnectionBundle> connections;

  private ConcurrentMap<String, MQQueue> queues = new ConcurrentHashMap<>();
  private RoundRobinExecutorPool         pool   = new RoundRobinExecutorPool("pool", 25);

  private static Utils utils = new Utils();

  private JsonService jsonService;

  public MqSimulatorService(Map<String, MQConnectionBundle> connections,
      SimulatorProperties props) {
    this.connections = connections;
    this.props = props;
    try {
      Map json = jsonService.decode(props.toString(), Map.class);
      log.debug(jsonService.encode(json));
    } catch (Exception e) {
    } finally {
    }
    log.info("Iniciando autorespuestas en modo " + props.getRole());
    autoResponses();

    log.info("Iniciando consumidor en modo " + props.getRole());
    consume();
  }

  // @Override
  // public void run(String... args) throws Exception {
  // log.info("Iniciando autorespuestas en modo " + props.getRole());
  // autoResponses();
  //
  // log.info("Iniciando consumidor en modo " + props.getRole());
  // consume();
  // }

  public void processAndReply() {
    Map<String, AutoResponse> autoResponses = props.getAutoResponses();

    if (autoResponses == null || autoResponses.isEmpty()) {
      log.info("No hay colas configuradas para autoRespuesta. Ignorando autoRespuestas.");
      return;
    }
    autoResponses.forEach((sourceKey, rule) -> {
      try {
        log.info(
            "Configurada respuesta automática para " + sourceKey + " -> " + rule.getTargetQueue()
                + " | Payload: " + rule.getPayloadKey() + " | CopyCorrel: " + rule.isCopyCorrel());
        pool.execute(() -> processAndReply(sourceKey, rule));
      } catch (Exception ignored) {
        ignored.printStackTrace();
      }
    });
  }

  private void consume() {
    Map<String, String> consumerQueue = props.getConsumers();
    if (consumerQueue == null || consumerQueue.isEmpty()) {
      log.info("No hay colas configuradas para consumo. Ignorando consumidor.");
      return;
    }
    AtomicInteger threadID = new AtomicInteger(0); // Creamos una copia local e inmutable para
    // la lambda
    consumerQueue.forEach((key, consumeRule) -> {
      log.info(
          "Configurada escucha y purgado para " + consumeRule + " (definida en " + key + ")");
      // pool.execute(() -> consumeAndLog(consumeRule, poolManager.getNextIndex()));
      int poolSize = props.getNumThreads() > 0 ? props.getNumThreads() : 1;
      for (int i = 0; i < poolSize; i++) {
        new Thread(() -> {
          try {
            consumeAndLog(consumeRule, threadID.get());
          } catch (MQException e) {
            e.printStackTrace();
          }
        }, "Thread-" + threadID.getAndIncrement() + "-Consumidor").start();
      }
    });
  }

  // @Scheduled(fixedDelay = 100)
  public void autoResponses() {
    // 1. Lógica de Respuestas Automáticas (Simular Sistema Externo)
    if (props.getAutoResponses() == null || props.getAutoResponses().isEmpty()) {
      log.info("No hay colas configuradas para autoRespuesta. Ignorando autoRespuestas.");
      return;
    }
    AtomicInteger threadID = new AtomicInteger(0); // Creamos una copia local e inmutable para
    // la lambda
    props.getAutoResponses().forEach((sourceKey, rule) -> {
      try {
        int poolSize = props.getNumThreads() > 0 ? props.getNumThreads() : 1;
        for (int i = 0; i < poolSize; i++) {
          new Thread(() -> {
            processAndReply(sourceKey, rule);
          }, "Thread-" + threadID.getAndIncrement() + "-AutoResponse").start();
        }
      } catch (Exception ignored) {
        ignored.printStackTrace();
      }
    });
  }

  public void send(String queueKey, String payloadKey, byte[] correlationId, byte[] messageId,
      boolean isCopyCorrel,
      String replyTo, String replyToQMgr, boolean processed, Optional<QueueConfig> overrideConfig)
      throws Exception {
    String oldPayloadKey = payloadKey;
    String content = "";
    if (props.getRole().equals("T3270_START")) {
      content = payloadKey;
    } else {
      content = props.getPayloads().get(payloadKey);
    }
    SimulatorProperties.QueueConfig qConfig = props.getQueues().get(queueKey);

    int options = MQConstants.MQOO_OUTPUT | MQConstants.MQOO_FAIL_IF_QUIESCING;
    MQQueue queue = getQueue(qConfig.getName());
    // log.debug("Creating PUT queue to cache..." + qConfig.getName());
    // queues.computeIfAbsent(qConfig.getName(), key -> {
    // try {
    // log.info("Abriendo cola por primera vez: {}", key);
    // return qmProducer.accessQueue(key, options);
    // } catch (MQException e) {
    // throw new RuntimeException("Error al acceder a la cola MQ: " + key, e);
    // }
    // });
    // queue = queues.get(qConfig.getName());
    // MQQueue queue = poolManager.getNextPutQueue(qConfig.getName()); // Solo para avanzar el
    // // índice del pool a

    MQMessage msg = new MQMessage();
    msg.characterSet = qConfig.getCcsid();
    msg.format = MQConstants.MQFMT_STRING;
    if (replyTo != null && !replyTo.isEmpty()) {
      msg.replyToQueueName = replyTo;
    }
    if (replyToQMgr != null && !replyToQMgr.isEmpty()) {
      msg.replyToQueueManagerName = replyToQMgr;
    }
    if (correlationId != null) {
      msg.correlationId = correlationId;
    }
    if (isCopyCorrel) { // && processed) {
      msg.messageId = msg.correlationId;
    }
    AtomicBoolean copied = new AtomicBoolean(false);
    String correlation = props.getProperties().get("correlation");
    overrideConfig.ifPresent(config -> {
      log.debug("Aplicando configuración de override para " + queueKey + " | Payload-key: "
          + config.getPayloadKey() + " | CopyCorrel: " + config.isCopyCorrel());
      if (config.isCopyCorrel()) {
        msg.messageId = msg.correlationId;
        copied.set(true);
      }
    });
    if (!copied.get()) {
      if (correlation.equals("msg-correl") && !props.getRole().equals("SOH_START")) {
        msg.messageId = msg.correlationId;
      } else if (correlation.equals("msg-correl") && props.getRole().equals("SOH_START")) {
        msg.correlationId = messageId;
      } else if (correlation.equals("correl-correl")) {
        msg.messageId = msg.correlationId;
      } else if (correlation.equals("msg-msg")) {
        msg.correlationId = messageId;
      } else if (qConfig.isCopyCorrel() && !props.getRole().equals("SOH_START")) {
        msg.messageId = msg.correlationId;
      } else if (qConfig.isCopyCorrel() && props.getRole().equals("SOH_START")) {
        msg.correlationId = messageId;
      }
    }
    if (msg.messageId == null || utils.isNullOrEmpty(msg.messageId)) {
      msg.messageId = Utils.generateRandomId();
    }
    if (qConfig.getTresp() > 0) {
      log.debug(
          "Simulando tiempo de procesamiento de " + qConfig.getTresp() + " ms para " + queueKey);
      TimeUnit.MILLISECONDS.sleep(qConfig.getTresp()); // Simular tiempo de procesamiento
    }
    if (!processed) {
      if (payloadKey.startsWith("payload:")) {
        String customPayload = payloadKey.substring(8);
        payloadKey = MessagesMgr.createStrPayload(customPayload);
      } else if (props.getPayloads().get(payloadKey).startsWith("payload:")) {
        String customPayload = props.getPayloads().get(payloadKey).substring(8);
        payloadKey = MessagesMgr.createStrPayload(customPayload);
      } else {
        payloadKey = content;
      }
      msg.writeString(payloadKey);
    } else {
      msg.writeString(content);
    }
    int nTh = props.getNumThreads() > 0 ? props.getNumThreads() : 1;
    String pay = overrideConfig.map(QueueConfig::getPayloadKey).orElse(oldPayloadKey);
    log.debug(">>> [PUT] " + qConfig.getName()
        + " | CCSID: "
        + qConfig.getCcsid() + " | HEX: "
        + bytesToHex(msg.correlationId));

    msg.seek(0);
    queue.put(msg, new MQPutMessageOptions());
    if (false) {
      log.debug(
          ">>> [SENT] " + qConfig.getName() + " -> "
              + pay
              + " | CCSID: "
              + qConfig.getCcsid() + " | HEX: "
              + bytesToHex(msg.correlationId));
    }
    if (qConfig.getFire() == null || qConfig.getFire().getQueues() == null
        || qConfig.getFire().getQueues().isEmpty()) {
      return;
    }
    log.debug(
        ">>> [FIRE] Verificando mensajes adicionales configurados en fire para " + queueKey);
    checkFire(qConfig, msg.correlationId, msg.messageId, isCopyCorrel, replyTo, replyToQMgr);
  }

  /**
   * @param qConfig
   * @param correlationId
   * @param messageId
   * @param isCopyCorrel
   * @param replyTo
   * @param replyToQMgr
   */
  private void checkFire(QueueConfig qConfig, byte[] correlationId, byte[] messageId,
      boolean isCopyCorrel, String replyTo, String replyToQMgr) {
    qConfig.getFire().getQueues().forEach((targetQueue, val) -> {
      try {
        TimeUnit.MILLISECONDS.sleep(100); // Simular tiempo de espera antes de disparar
        log.debug(
            ">>> [FIRE] Disparando mensajes adicionales configurados en fire para " + targetQueue);
        pool.execute(() -> {
          try {
            send(targetQueue, val.getPayloadKey(), correlationId, messageId, val.isCopyCorrel(),
                replyTo, replyToQMgr, false, Optional.of(val));
          } catch (Exception e) {
            log.error("Error al enviar mensaje adicional configurado en fire para " + targetQueue,
                e);
          }
        });
      } catch (Exception ex) {
        ex.printStackTrace();
      }
    });
  }

  public MQQueue getQueue(String name) {
    MQConnectionBundle bundle = connections.get(name);
    return queues.compute(name, (key, existingQueue) -> {
      int options = MQConstants.MQOO_OUTPUT | MQConstants.MQOO_FAIL_IF_QUIESCING;
      // Si no existe o está cerrada, creamos una nueva
      MQQueue q = bundle.getQueue();
      if (existingQueue == null || !existingQueue.isOpen()) {
        log.info("Abriendo nueva instancia de cola: {}", key);
        return q;
        // return qmProducer.accessQueue(key, options);
      }
      return bundle.getQueue();
    });
  }

  public MQQueue getQueueGets(String name) {
    MQConnectionBundle bundle = connections.get(name);
    return queues.compute(name, (key, existingQueue) -> {
      int options = MQConstants.MQOO_INPUT_AS_Q_DEF;
      MQQueue q = bundle.getQueue();
      // Si no existe o está cerrada, creamos una nueva
      if (existingQueue == null || !existingQueue.isOpen()) {
        log.info("Abriendo nueva instancia de cola: {}", key);
        return q;
      }
      return bundle.getQueue();
    });
  }
  /**
   * @param sourceKey
   * @param rule
   */
  private void processAndReply(String sourceKey, AutoResponse rule) {
    int i = 0;
    int poolSize = props.getNumThreads() > 0 ? props.getNumThreads() : 1;
    SimulatorProperties.QueueConfig sourceQ = props.getQueues().get(sourceKey);
    log.info(
        "Hilo {} iniciando procesamiento de respuestas automáticas para {} (usando pool size {})",
        Thread.currentThread().getName(), sourceQ.getName(), poolSize);
    MQQueue queue = getQueueGets(sourceQ.getName());
    while (!Thread.currentThread().isInterrupted()) {
      try {
        int index = i % poolSize;
        i++;
        MQGetMessageOptions gmo = new MQGetMessageOptions();
        gmo.options = MQConstants.MQGMO_WAIT | MQConstants.MQGMO_FAIL_IF_QUIESCING;
        gmo.waitInterval = 10_000; // Un poco más de margen para no saturar el CPU

        MQMessage incoming = new MQMessage();

        queue.get(incoming, gmo);

        log.debug("<<< [GET] " + queue.getName().trim() + " -> " + sourceKey
            + " | CCSID: "
            + incoming.characterSet + " | HEX: "
            + bytesToHex(incoming.correlationId));

        boolean isCopyCorrel = rule.isCopyCorrel();
        if (rule.getPayloadKey().startsWith("payload:")) {
          String customPayload = rule.getPayloadKey().substring(8);
          String mgr = MessagesMgr.createStrPayload(customPayload);
          lSend(rule.getTargetQueue(), mgr, incoming.correlationId, incoming.messageId,
              isCopyCorrel,
              "", "",
              true, java.util.Optional.empty(), true);
        } else {
          lSend(rule.getTargetQueue(), rule.getPayloadKey(), incoming.correlationId,
              incoming.messageId, isCopyCorrel, "", "", true, java.util.Optional.empty(), true);
        }
      } catch (MQException e) {
        if (e.reasonCode != 2033) {
          System.err.println("Error MQ RC: " + e.reasonCode);
        }
        if (e.reasonCode == MQConstants.MQRC_CONNECTION_BROKEN ||
            e.reasonCode == MQConstants.MQRC_Q_MGR_NOT_AVAILABLE) {

          System.out.println("Conexión perdida. Intentando reconectar...");
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * @param targetQueue
   * @param payloadKey
   * @param correlationId
   * @param messageId
   * @param isCopyCorrel
   * @param string
   * @param string2
   * @param b
   * @param empty
   * @param c
   */
  private void lSend(String targetQueue, String payloadKey, byte[] correlationId, byte[] messageId,
      boolean isCopyCorrel, String string, String string2, boolean b, Optional<QueueConfig> empty,
      boolean c) {
    pool.execute(() -> {
      try {
        send(targetQueue, payloadKey, correlationId, messageId, isCopyCorrel, string, string2, b,
            empty);
      } catch (Exception e) {
        log.error("Error al enviar mensaje desde processAndReply para " + targetQueue, e);
      }
    });

  }

  /**
   * Escucha una cola, consume el mensaje y lo imprime. Esto mantiene la cola vacía permanentemente.
   * 
   * @throws MQException
   */

  private void consumeAndLog(String queueKey, int threadIndex) throws MQException {
    int poolSize = props.getNumThreads() > 0 ? props.getNumThreads() : 1;
    SimulatorProperties.QueueConfig sourceQ = props.getQueues().get(queueKey);
    log.info("Hilo {} iniciando escucha y purgado en {} (usando pool size {})", threadIndex,
        queueKey, poolSize);
    int i = 0;
    MQQueue queue = getQueueGets(sourceQ.getName());
    while (!Thread.currentThread().isInterrupted()) {
      try {
        int index = i % poolSize;
        i++;
        // int options = MQConstants.MQOO_INPUT_AS_Q_DEF;
        // if (queues != null && queues.containsKey(sourceQ.getName())) {
        // queue = queues.get(sourceQ.getName());
        // } else {
        // queues.put(sourceQ.getName(), qmConsumer2.accessQueue(sourceQ.getName(), options));
        // queue = queues.get(sourceQ.getName());
        // }
        MQGetMessageOptions gmo = new MQGetMessageOptions();
        gmo.options = MQConstants.MQGMO_WAIT | MQConstants.MQGMO_FAIL_IF_QUIESCING;
        gmo.waitInterval = 10_000; // Un poco más de margen para no saturar el CPU

        MQMessage incoming = new MQMessage();

        queue.get(incoming, gmo);
        if (incoming.getMessageLength() == 0) {
          continue; // Si el mensaje está vacío, lo ignoramos y seguimos esperando
        }
        log.debug("<<< [PRG] {}-{} - HEX: {}",
            queue.getName().trim(), index, bytesToHex(incoming.correlationId));
        TimeUnit.MILLISECONDS.sleep(50); // Simular un pequeño tiempo de procesamiento antes de
                                         // purgar
      } catch (MQException e) {
        // 2033 = MQRC_NO_MSG_AVAILABLE (Es normal si la cola está vacía)
        if (e.reasonCode != MQConstants.MQRC_NO_MSG_AVAILABLE) {

          // Si la conexión se rompe (2009, 2059)
          if (e.reasonCode == MQConstants.MQRC_CONNECTION_BROKEN ||
              e.reasonCode == MQConstants.MQRC_Q_MGR_NOT_AVAILABLE) {

            log.error("Conexión perdida en hilo {}. Reconectando...", threadIndex);
            break; // Salimos del loop para que el hilo se reinicie con conexión nueva
          }
          throw e; // Otros errores
        }
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }

}

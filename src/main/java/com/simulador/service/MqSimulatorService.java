package com.simulador.service;

import com.ibm.mq.MQException;
import com.ibm.mq.MQGetMessageOptions;
import com.ibm.mq.MQMessage;
import com.ibm.mq.MQPutMessageOptions;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.MQConstants;
import com.simulador.components.MessagesMgr;
import com.simulador.components.MqPoolManager;
import com.simulador.config.MQNativeConfig;
import com.simulador.config.SimulatorProperties;
import com.simulador.config.SimulatorProperties.AutoResponse;
import com.simulador.config.SimulatorProperties.QueueConfig;
import com.simulador.utils.Utils;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class MqSimulatorService implements Runnable {

  private MQQueueManager            qMgr;
  private final SimulatorProperties props;
  private Map<String, MQQueue>      queues      = new java.util.LinkedHashMap<>();
  private RoundRobinExecutorPool    pool        = new RoundRobinExecutorPool("pool", 25);

  private static Utils utils = new Utils();

  public MqSimulatorService(MQQueueManager qMgr, SimulatorProperties props) {
    this.qMgr = qMgr;
    this.props = props;
    try {
      log.debug(this.props.toString());
    } catch (Exception e) {
    } finally {
    }
  }

  @Autowired
  private MqPoolManager poolManager;

  @PostConstruct
  public void run() {
    log.info("Iniciando autorespuestas en modo " + props.getRole());
    autoResponses();

    log.info("Iniciando consumidor en modo " + props.getRole());
    consume();
  }

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
    consumerQueue.forEach((key, consumeRule) -> {
      log.info(
          "Configurada escucha y purgado para " + consumeRule + " (definida en " + key + ")");
      // pool.execute(() -> consumeAndLog(consumeRule, poolManager.getNextIndex()));
      int poolSize = props.getNumThreads() > 0 ? props.getNumThreads() : 1;
      for (int i = 0; i < poolSize; i++) {
        final int threadID = i; // Creamos una copia local e inmutable para la lambda
        new Thread(() -> {
          // Aquí dentro, cada lambda tiene su propia copia de threadID
          consumeAndLog(consumeRule, threadID);
        }, "Thread-" + threadID + "-Consumidor").start();
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
    props.getAutoResponses().forEach((sourceKey, rule) -> {
      try {
        int poolSize = props.getNumThreads() > 0 ? props.getNumThreads() : 1;
        for (int i = 0; i < poolSize; i++) {
          final int threadID = i; // Creamos una copia local e inmutable para la lambda
          new Thread(() -> {
            processAndReply(sourceKey, rule);
          }, "Thread-" + threadID + "-AutoResponse").start();
        }
      } catch (Exception ignored) {
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

    // int options = MQConstants.MQOO_OUTPUT | MQConstants.MQOO_FAIL_IF_QUIESCING;
    // MQQueue queue = null;
    // if (queues.containsKey(qConfig.getName())) {
    // queue = queues.get(qConfig.getName());
    // } else {
    // queues.put(qConfig.getName(), qMgr.accessQueue(qConfig.getName(), options));
    // queue = queues.get(qConfig.getName());
    // }
    MQQueue myQueues = poolManager.getNextPutQueue(qConfig.getName()); // Solo para avanzar el
                                                                       // índice del pool a

    MQQueue queue = myQueues;
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
    log.debug(">>> [PUT] " + qConfig.getName() + "-"
        + (poolManager.putIndex.get() & nTh) + " -> " + pay
        + " | CCSID: "
        + qConfig.getCcsid() + " | HEX: "
        + bytesToHex(msg.correlationId));

    msg.seek(0);
    queue.put(msg, new MQPutMessageOptions());

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
    if (qConfig.getFire() == null || qConfig.getFire().getQueues() == null
        || qConfig.getFire().getQueues().isEmpty()) {
      return;
    }
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
        Thread.currentThread().getName(), sourceKey, poolSize);
    while (true) {
      try {
        int nTh = i % poolSize;
        MQQueue queue = poolManager.getGetAnsQueueByIndex(nTh);
        i++;
        int options = MQConstants.MQOO_INPUT_AS_Q_DEF;
        // MQQueue queue = null;
        if (queues != null && queues.containsKey(sourceQ.getName())) {
          queue = queues.get(sourceQ.getName());
        } else {
          queues.put(sourceQ.getName(), qMgr.accessQueue(sourceQ.getName(), options));
          queue = queues.get(sourceQ.getName());
        }
        MQMessage incoming = new MQMessage();
        MQGetMessageOptions gmo = new MQGetMessageOptions();
        gmo.waitInterval = 10;
        gmo.options = MQConstants.MQGMO_WAIT | MQConstants.MQGMO_FAIL_IF_QUIESCING;

        queue.get(incoming, gmo);
        // log.debug("<<< [IN] " + queue.getName().trim() + "-" + nTh + " <- | Mensaje detectado en
        // "
        // + sourceKey);
        log.debug("<<< [GET] " + queue.getName().trim() + "-"
            + (poolManager.putIndex.get() & nTh) + " -> " + sourceKey
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
          reconnect();
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

  private String bytesToHex(byte[] bytes) {
    StringBuilder sb = new StringBuilder();
    for (byte b : bytes)
      sb.append(String.format("%02X ", b));
    return sb.toString();
  }

  /**
   * Escucha una cola, consume el mensaje y lo imprime. Esto mantiene la cola vacía permanentemente.
   */

  @Async("pool")
  private void consumeAndLog(String queueKey, int threadIndex) {
    int poolSize = props.getNumThreads() > 0 ? props.getNumThreads() : 1;
    log.info("Hilo {} iniciando escucha y purgado en {} (usando pool size {})", threadIndex,
        queueKey, poolSize);
    int i = 0;
    while (!Thread.currentThread().isInterrupted()) {
      try {
        // 1. En lugar de accessQueue o mirar tu propio mapa, pedimos una vía del Pool
        // Usamos el threadIndex para que cada hilo use siempre la misma vía del pool
        int index = i % poolSize;
        MQQueue queue = poolManager.getGetQueueByIndex(index);
        i++;
        MQGetMessageOptions gmo = new MQGetMessageOptions();
        gmo.options = MQConstants.MQGMO_WAIT | MQConstants.MQGMO_FAIL_IF_QUIESCING;
        gmo.waitInterval = 10; // Un poco más de margen para no saturar el CPU

        MQMessage incoming = new MQMessage();

        try {
          // 2. Intentamos el GET
          queue.get(incoming, gmo);
          if (incoming.getMessageLength() == 0) {
            incoming.clearMessage();
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
              reconnect();
              break; // Salimos del loop para que el hilo se reinicie con conexión nueva
            }
            throw e; // Otros errores
          }
        } finally {
          incoming.clearMessage();
        }
      } catch (Exception e) {
        log.error("Error en el loop de consumo: ", e);
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ie) {
          break;
        }
      }
    }
  }

  /**
   * 
   */
  private void reconnect() {
    while (!qMgr.isConnected()) {
      try {
        TimeUnit.SECONDS.sleep(5);
        System.out.println("Intentando reconectar a MQ...");
        qMgr = MQNativeConfig.mqQueueManager();
      } catch (MQException | InterruptedException e) {
        System.out.println("Reintento fallido. Volviendo a intentar en 5 segundos...");
      }
    }
    System.out.println("Reconexión exitosa a MQ.");
  }

}

/**
 * 
 */
package com.simulador.components;

import com.ibm.mq.MQMessage;
import com.ibm.mq.MQPutMessageOptions;
import com.ibm.mq.MQQueue;
import com.ibm.mq.constants.MQConstants;
import com.simulador.config.SimulatorProperties;
import com.simulador.config.SimulatorProperties.QueueConfig;
import com.simulador.service.RoundRobinExecutorPool;
import com.simulador.utils.Utils;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

// @Component
@Slf4j
@Getter
@Setter
public class MessageSender extends Utils {

  private RoundRobinExecutorPool pool = new RoundRobinExecutorPool("pool", 25);
  private static Utils utils = new Utils();

  private final SimulatorProperties props;

  // @Autowired
  // private MqSimulatorService simulatorService;

  @Autowired
  private MqPoolManager poolManager;

  public MessageSender(SimulatorProperties props) {
    this.props = props;
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
    int openOptions = MQConstants.MQOO_OUTPUT | MQConstants.MQOO_FAIL_IF_QUIESCING;
    // MQQueue queue = null;
    // if (MqSimulatorService.queues.containsKey(qConfig.getName())) {
    // queue = MqSimulatorService.queues.get(qConfig.getName());
    // } else {
    // MqSimulatorService.queues.put(qConfig.getName(),
    // qMgr.accessQueue(qConfig.getName(), openOptions));
    // queue = MqSimulatorService.queues.get(qConfig.getName());
    // }
    MQQueue queue = poolManager.getNextPutQueue(qConfig.getName()); // Solo para avanzar el
    // índice del pool a

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
    MQPutMessageOptions pmo = new MQPutMessageOptions();
    // Esto hace que el PUT sea ultra rápido y no espere confirmación total del servidor
    pmo.options = MQConstants.MQPMO_ASYNC_RESPONSE;
    queue.put(msg, pmo);
    if (qConfig.getFire() == null || qConfig.getFire().getQueues() == null
        || qConfig.getFire().getQueues().isEmpty()) {
      return;
    }
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
    log.debug("Checking if fire configured...");
    qConfig.getFire().getQueues().forEach((targetQueue, val) -> {
      try {
        TimeUnit.MILLISECONDS.sleep(25); // Simular tiempo de espera antes de disparar
        log.debug(
            ">>> [FIRE] Disparando mensajes adicionales configurados en fire para " + targetQueue);
        pool.execute(() -> {
          try {
            send(targetQueue, val.getPayloadKey(), correlationId, messageId,
                val.isCopyCorrel(),
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
}

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
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

@Service
@Slf4j
@DependsOn("mqConnections")
public class MqSimulatorService extends Utils {

  private final SimulatorProperties props;

  private Map<String, MQConnectionBundle> connections;

  private ConcurrentMap<String, MQQueue> queues = new ConcurrentHashMap<>();
  private RoundRobinExecutorPool         pool   = new RoundRobinExecutorPool("pool", 25);

  private static Utils utils = new Utils();

  private JsonService jsonService;

  public MqSimulatorService(Map<String, MQConnectionBundle> connections,
      SimulatorProperties props) {
    this.connections = connections;
    this.props = props;
  }

  @PostConstruct
  private void construct() {
    try {
      Map json = jsonService.decode(props.toString(), Map.class);
      log.debug(jsonService.encode(json));
    } catch (Exception e) {
    }

    log.info("Iniciando autorespuestas en modo " + props.getRole());
    props.getAutoResponses().forEach((sourceKey, rule) -> {
      try {
        int poolSize = props.getNumThreads() > 0 ? props.getNumThreads() : 1;
        SimulatorProperties.QueueConfig sourceQ = props.getQueues().get(sourceKey);
        for (int i = 0; i < poolSize; i++) {
          createFlux(sourceQ.getName(), processAndReply(sourceKey, rule)).subscribe();
        }
      } catch (Exception ignored) {
        ignored.printStackTrace();
      }
    });

    log.info("Iniciando consumidor en modo " + props.getRole());
    Map<String, String> consumerQueue = props.getConsumers();
    consumerQueue.forEach((key, consumeRule) -> {
      SimulatorProperties.QueueConfig sourceQ = props.getQueues().get(consumeRule);
      log.info(
          "Configurada escucha y purgado para " + sourceQ.getName() + " (definida en " + key + ")");
      int poolSize = props.getNumThreads() > 0 ? props.getNumThreads() : 1;
      for (int i = 0; i < poolSize; i++) {
        try {
          int index = i % poolSize;
          createFlux(sourceQ.getName(), consumeAndLog2(sourceQ.getName(), index)).subscribe();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });
  }

  /**
   * @param consumeRule
   * @param i
   * @return
   */
  private Consumer<MQMessage> consumeAndLog2(String consumeRule, int i) {
    return (msg) -> {
      try {
        // Leemos el contenido del mensaje
        // String data = msg.readStringOfByteLength(msg.getMessageLength());

        log.debug("<<< [PRG] {}-{} - HEX: {}",
            consumeRule, i, bytesToHex(msg.correlationId));

      } catch (Exception e) {
        log.error("Error procesando mensaje en cola {}: {}", consumeRule, e.getMessage());
      }
    };
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
    log.debug(">>> [PUT] " + qConfig.getName()
        + " | CCSID: "
        + qConfig.getCcsid() + " | HEX: "
        + bytesToHex(msg.correlationId));

    msg.seek(0);
    queue.put(msg, new MQPutMessageOptions());

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
  private Consumer<MQMessage> processAndReply(String sourceKey, AutoResponse rule) {
    return (incoming) -> {
      SimulatorProperties.QueueConfig sourceQ = props.getQueues().get(sourceKey);
      try {
        log.debug("<<< [GET] " + sourceQ.getName() + " -> " + sourceKey
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
      } catch (Exception e) {
        e.printStackTrace();
      }
    };
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

  public Flux<MQMessage> createFlux(String queueName, Consumer<MQMessage> action) {
    MQConnectionBundle bundle = connections.get(queueName);

    if (bundle == null) {
      return Flux
          .error(new IllegalArgumentException("La cola " + queueName + " no está configurada."));
    }

    return Flux.<MQMessage>create(sink -> {
      log.info("Creando Flux dedicado para la cola: {}", queueName);

      // Hilo de lectura para esta cola específica
      while (!sink.isCancelled()) {
        try {
          MQMessage msg = new MQMessage();
          MQGetMessageOptions gmo = new MQGetMessageOptions();
          gmo.options = MQConstants.MQGMO_WAIT | MQConstants.MQGMO_FAIL_IF_QUIESCING;
          gmo.waitInterval = 2000;

          bundle.getQueue().get(msg, gmo);

          // Emitimos el mensaje
          sink.next(msg);

        } catch (MQException e) {
          if (e.reasonCode != MQConstants.MQRC_NO_MSG_AVAILABLE) {
            log.error("Error MQ en {}: {}", queueName, e.reasonCode);
          }
        }
      }
    })
        .subscribeOn(Schedulers.newSingle("Thread-" + queueName)) // Hilo físico dedicado
        .doOnNext(action); // Aquí se ejecuta tu "doMethod()" cada vez que llega un mensaje
  }
}

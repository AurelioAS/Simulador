package com.simulador.service;

import com.ibm.mq.MQException;
import com.ibm.mq.MQGetMessageOptions;
import com.ibm.mq.MQMessage;
import com.ibm.mq.MQPutMessageOptions;
import com.ibm.mq.MQQueue;
import com.ibm.mq.constants.MQConstants;
import com.simulador.components.JsonService;
import com.simulador.config.MQConnectionBundle;
import com.simulador.config.SimulatorProperties;
import com.simulador.config.SimulatorProperties.AutoResponse;
import com.simulador.config.SimulatorProperties.QueueConfig;
import com.simulador.utils.MessagesMgr;
import com.simulador.utils.Utils;
import java.util.GregorianCalendar;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
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
  private final JsonService         jsonService;

  private Map<String, MQConnectionBundle> connections;

  private ConcurrentMap<String, MQQueue> queues = new ConcurrentHashMap<>();
  private RoundRobinExecutorPool         pool   = new RoundRobinExecutorPool("pool", 25);

  private final Utils utils = new Utils();

  public MqSimulatorService(Map<String, MQConnectionBundle> connections,
      SimulatorProperties props,
      JsonService jsonService) {
    this.connections = connections;
    this.props = props;
    this.jsonService = jsonService;
  }

  @PostConstruct
  private void construct() {
    log.info("Iniciando autorespuestas y consumidor en modo " + props.getRole());
    Map<String, AutoResponse> responses = props.getAutoResponses();
    if (responses != null) {
      responses.forEach((sourceKey, rule) -> {
        try {
          int poolSize = props.getNumThreads() > 0 ? props.getNumThreads() : 1;
          SimulatorProperties.QueueConfig sourceQ = props.getQueues().get(sourceKey);
          log.info("Configurada Autorespuesta y Flux para '{}' ({})", sourceQ.getName(), poolSize);
          MQConnectionBundle bundle = connections.get(sourceQ.getName());
          for (int i = 0; i < poolSize; i++) {
            createFlux(sourceQ.getName(), processAndReply(sourceKey, rule, bundle)).subscribe();
          }
        } catch (Exception ignored) {
          log.error("Error configurando autorespuesta para {}: {}", sourceKey,
              ignored.getMessage());
        }
      });
    }

    Map<String, String> consumerQueue = props.getConsumers();
    if (consumerQueue != null) {
      consumerQueue.forEach((key, consumeRule) -> {
        SimulatorProperties.QueueConfig sourceQ = props.getQueues().get(consumeRule);
        int poolSize = props.getNumThreads() > 0 ? props.getNumThreads() : 1;
        log.info("Configurada escucha y purgado y Flux para '{}' ({})", sourceQ.getName(),
            poolSize);
        MQConnectionBundle bundle = connections.get(sourceQ.getName());
        for (int i = 0; i < poolSize; i++) {
          try {
            int index = i % poolSize;
            createFlux(sourceQ.getName(),
                consumeAndLog(consumeRule, sourceQ.getName(), index, bundle))
                    .subscribe();
          } catch (Exception e) {
            log.error("Error configurando escucha para {}: {}", sourceQ.getName(), e.getMessage());
          }
        }
      });
    }
  }

  /**
   * @param consumeRule
   * @param i
   * @param bundle
   * @return
   */
  private Consumer<MQMessage> consumeAndLog(String key, String consumeRule, int i,
      MQConnectionBundle bundle) {
    return (msg) -> {
      try {
        String qmName = bundle.getQueue().getConnectionReference().getName();
        traceGet("<<< [PRG]", qmName, consumeRule, key, msg.characterSet,
            bytesToHex(msg.correlationId));
        // log.debug("<<< [PRG] {} <-- {}| CCSID: {} | HEX: {} ",
        // qmName + "|" + consumeRule, String.format("%-15s", key), msg.characterSet,
        // bytesToHex(msg.correlationId));

      } catch (Exception e) {
        log.error("Error procesando mensaje en cola {}: {}", consumeRule, e.getMessage());
      }
    };
  }


  /**
   * @param string
   * @param qmName
   * @param consumeRule
   * @param key
   * @param characterSet
   * @param bytesToHex
   */
  private void traceGet(String string, String qmName, String consumeRule, String key,
      int characterSet, String correlationId) {
    if (log.isDebugEnabled()) {
      key = key.length() > 15 ? key.substring(0, 15) + "..." : key;
      log.debug(string + " {} <-- {} | CCSID: {} | HEX: {} ",
          qmName + "|" + consumeRule, String.format("%-18s", key), characterSet,
        correlationId);
    }
  }

  /**
   * @param queueKey
   * @param payloadKey
   * @param correlationId
   * @param messageId
   * @param isCopyCorrel
   * @param replyTo
   * @param replyToQMgr
   * @param processed
   * @param overrideConfig
   * @throws Exception
   */
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
    String qmName = queue.getConnectionReference().getName();
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
    MessagesMgr messagesMgr = new MessagesMgr();
    if (!processed) {
      if (payloadKey.startsWith("payload:")) {
        String customPayload = payloadKey.substring(8);
        payloadKey = messagesMgr.createStrPayload(customPayload);
      } else if (props.getPayloads().get(payloadKey).startsWith("payload:")) {
        String customPayload = props.getPayloads().get(payloadKey).substring(8);
        payloadKey = messagesMgr.createStrPayload(customPayload);
      } else {
        payloadKey = content;
      }
      msg.writeString(payloadKey);
    } else {
      msg.writeString(content);
    }
    TimeZone utc = TimeZone.getTimeZone("Zulu");
    GregorianCalendar cal = new GregorianCalendar(utc);
    msg.putDateTime = cal;
    // msg.seek(0);
    queue.put(msg, new MQPutMessageOptions());
    String key = queueKey.length() > 15 ? queueKey.substring(0, 15) + "..." : queueKey;
    log.debug(
        ">>> [PUT] " + qmName + "|" + qConfig.getName() + " --> " + String.format("%-18s", key)
            + " | CCSID: "
            + qConfig.getCcsid() + " | HEX: "
            + bytesToHex(msg.correlationId));

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
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        log.error("Interrumpido al esperar para disparar mensaje adicional en fire para {}",
            targetQueue, ex);
      } catch (Exception ex) {
        log.error("Error inesperado en checkFire para {}", targetQueue, ex);
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
   * @param bundle
   */
  private Consumer<MQMessage> processAndReply(String sourceKey, AutoResponse rule,
      MQConnectionBundle bundle) {
    return (incoming) -> {
      SimulatorProperties.QueueConfig sourceQ = props.getQueues().get(sourceKey);
      try {
        String qmName = bundle.getQueue().getConnectionReference().getName();
        traceGet("<<< [GET]", qmName, sourceQ.getName(), sourceKey, incoming.characterSet,
            bytesToHex(incoming.correlationId));
        // log.debug("<<< [GET] " + qmName + "|" + sourceQ.getName() + " <-- "
        // + String.format("%-15s", sourceKey) + " | CCSID: "
        // + incoming.characterSet + " | HEX: " + bytesToHex(incoming.correlationId));
        MessagesMgr messagesMgr = new MessagesMgr();
        boolean isCopyCorrel = rule.isCopyCorrel();
        if (rule.getPayloadKey().startsWith("payload:")) {
          String customPayload = rule.getPayloadKey().substring(8);
          String mgr = messagesMgr.createStrPayload(customPayload);
          lSend(rule.getTargetQueue(), mgr, incoming.correlationId, incoming.messageId,
              isCopyCorrel, "", "", true, java.util.Optional.empty(), true);
        } else {
          lSend(rule.getTargetQueue(), rule.getPayloadKey(), incoming.correlationId,
              incoming.messageId, isCopyCorrel, "", "", true, java.util.Optional.empty(),
              true);
        }
      } catch (Exception e) {
        log.error("Error procesando mensaje en processAndReply para {}: {}", sourceKey,
            e.getMessage(), e);
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

      // Hilo de lectura para esta cola específica
      while (!sink.isCancelled()) {
        try {
          MQMessage msg = new MQMessage();
          MQGetMessageOptions gmo = new MQGetMessageOptions();
          gmo.options = MQConstants.MQGMO_WAIT | MQConstants.MQGMO_FAIL_IF_QUIESCING;
          gmo.waitInterval = 20_000;

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
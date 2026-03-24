package com.simulador.service;

import com.ibm.mq.MQException;
import com.ibm.mq.MQGetMessageOptions;
import com.ibm.mq.MQMessage;
import com.ibm.mq.MQPutMessageOptions;
import com.ibm.mq.MQQueue;
import com.ibm.mq.constants.MQConstants;
import com.simulador.aspect.LogFullDetails;
import com.simulador.components.JsonService;
import com.simulador.config.MQConnectionBundle;
import com.simulador.config.SimulatorProperties;
import com.simulador.config.SimulatorProperties.AutoResponse;
import com.simulador.config.SimulatorProperties.QueueConfig;
import com.simulador.utils.MessagesMgr;
import com.simulador.utils.Utils;
import java.io.IOException;
import java.util.GregorianCalendar;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

@Service
@Slf4j
@DependsOn("mqConnections")
public class MqSimulatorService extends Utils {

  private final SimulatorProperties props;
  private final JsonService         jsonService;

  private Map<String, MQConnectionBundle> connections;

  private RoundRobinExecutorPool pool = new RoundRobinExecutorPool("pool", 25);

  private final Utils                   utils           = new Utils();
  private SseEmitter                    emitter;
  private final Map<String, SseEmitter> emittersActivos = new ConcurrentHashMap<>();

  @Autowired
  public MqSimulatorService(Map<String, MQConnectionBundle> connections,
      SimulatorProperties props,
      JsonService jsonService) {
    this.connections = connections;
    this.props = props;
    this.jsonService = jsonService;
  }

  /**
   * 
   */
  public MqSimulatorService(SimulatorProperties props, JsonService jsonService) {
    this.props = props;
    this.jsonService = jsonService;
  }

  @EventListener(ApplicationReadyEvent.class)
  public void run() {
    log.info("Iniciando autorespuestas y consumidor en modo " + props.getRole());
    Map<String, AutoResponse> responses = props.getAutoResponses();
    if (responses != null) {
      responses.forEach((sourceKey, rule) -> {
        try {
          int poolSize = props.getNumThreads() > 0 ? props.getNumThreads() : 1;
          SimulatorProperties.QueueConfig sourceQ = props.getQueues().get(sourceKey);
          log.info("Configurada Autorespuesta y Flux para '{}' ({})", sourceQ.getName(), poolSize);
          MQConnectionBundle bundle = connections.get(sourceQ.getName());
          queues.put(sourceQ.getName(), bundle.getQueue()); // Asegura que la cola esté en el mapa
          for (int i = 0; i < poolSize; i++) {
            int index = i % poolSize;
            createFlux(sourceQ.getName(), (msg) -> {
              processAndReply(msg, sourceKey, rule, bundle, 1);
            }, 1).subscribe();
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
        queues.put(sourceQ.getName(), bundle.getQueue()); // Asegura que la cola esté en el mapa
        for (int i = 0; i < poolSize; i++) {
          try {
            int index = i % poolSize;
            createFlux(sourceQ.getName(),
                (msg) -> {
                  consumeAndLog(msg, consumeRule, sourceQ.getName(), index, bundle, 2);
                }, 2).subscribe();
          } catch (Exception e) {
            log.error("Error configurando escucha para {}: {}", sourceQ.getName(), e.getMessage());
            e.printStackTrace();
          }
        }
      });
    }
  }

  public void runConsumers() {
    run();
  }

  /**
   * @param msg2
   * @param consumeRule
   * @param i
   * @param bundle
   * @param j
   * @return
   */
  private void consumeAndLog(MQMessage msg, String key, String consumeRule, int i,
      MQConnectionBundle bundle, int actionType) {
    try {
      traceGet("<<< [PRG]", consumeRule, key, msg.characterSet,
          bytesToHex(msg.correlationId));
      AtomicBoolean isCompleted = new AtomicBoolean(false);
      enviarSafe(emittersActivos.get("A"),
          "Consumer: " + consumeRule + " | CCSID: " + msg.characterSet + " | HEX: "
              + bytesToHex(msg.correlationId),
          isCompleted);
    } catch (Exception e) {
      log.error("Error procesando mensaje en cola {}: {}", consumeRule, e.getMessage());
    }
  }


  /**
   * @param string
   * @param qmName
   * @param consumeRule
   * @param key
   * @param characterSet
   * @param bytesToHex
   */
  private void traceGet(String string, String consumeRule, String key,
      int characterSet, String correlationId) {
    if (log.isDebugEnabled()) {
      key = key.length() > 15 ? key.substring(0, 15) + "..." : key;
      log.debug(string + " {} <-- {} | CCSID: {} | HEX: {} ",
          consumeRule, String.format("%-18s", key), characterSet,
          correlationId);
    }
  }

  public void registrarEmitter(String key, SseEmitter emitter) {
    this.emittersActivos.put(key, emitter);
  }

  public void removerEmitter(String key) {
    this.emittersActivos.remove(key);
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
   * @param source
   * @param clone
   * @param emitter
   * @throws Exception
   */

  public void send(String queueKey, String payloadKey, byte[] correlationId, byte[] messageId,
      boolean isCopyCorrel,
      String replyTo, String replyToQMgr, boolean processed, Optional<QueueConfig> overrideConfig,
      String source, MQConnectionBundle clone, SseEmitter emitter)
      throws Exception {
    this.emitter = emitter;
    String content = "";
    if (props.getRole().equals("T3270_START")) {
      content = payloadKey;
    } else {
      content = props.getPayloads().get(payloadKey);
    }
    SimulatorProperties.QueueConfig qConfig = props.getQueues().get(queueKey);
    MQQueue queue;
    if (clone != null) {
      queue = clone.getQueue();
    } else {
      queue = getQueue(qConfig.getName());
    }

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
      if (config.isCopyCorrel()) {
        msg.messageId = msg.correlationId;
        copied.set(true);
      }
    });
    if (!copied.get()) {
      if (correlation.equals("msg-correl") && !props.getRole().equals("SOH_START")) {
        msg.messageId = msg.correlationId;
      } else if (correlation.equals("msg-correl") && props.getRole().equals("SOH_START")) {
        if (messageId != null) {
          msg.correlationId = messageId;
        }
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
      msg.messageId = Utils.generateRandomId(24);
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
        ">>> [PUT] " + qConfig.getName() + " --> " + String.format("%-18s", key)
            + " | CCSID: "
            + qConfig.getCcsid() + " | HEX: "
            + bytesToHex(msg.correlationId));
    AtomicBoolean isCompleted = new AtomicBoolean(false);
    enviarSafe(emittersActivos.get("A"),
        "Enviando a " + qConfig.getName() + " | CCSID: " + qConfig.getCcsid() + " | HEX: "
            + bytesToHex(msg.correlationId),
        isCompleted);
    if (qConfig.getFire() == null || qConfig.getFire().getQueues() == null
        || qConfig.getFire().getQueues().isEmpty()) {
      return;
    }
    log.trace(
        ">>> [FIRE] Verificando mensajes adicionales configurados en fire para " + queueKey);
    checkFire(qConfig, msg.correlationId, msg.messageId, isCopyCorrel, replyTo, replyToQMgr,
        source);
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
      boolean isCopyCorrel, String replyTo, String replyToQMgr, String source) {

    qConfig.getFire().getQueues().forEach((targetQueue, val) -> {
      try {
        TimeUnit.MILLISECONDS.sleep(5); // Simular tiempo de espera antes de disparar
        pool.execute(() -> {
          log.debug(
              ">>> [FIRE] Disparando mensajes adicionales configurados en fire para "
                  + targetQueue);
          try {
            send(targetQueue, val.getPayloadKey(), correlationId, messageId, val.isCopyCorrel(),
                replyTo, replyToQMgr, false, Optional.of(val), source, null, emitter);
          } catch (IOException e) {
            log.error("Error al enviar mensaje al front configurado en fire para " + targetQueue,
                e);
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
      if (existingQueue == null || !existingQueue.isOpen()) {
        log.info("Abriendo nueva instancia de cola: {}", key);
        return bundle.getQueue();
      }
      return existingQueue;
    });
  }

  /**
   * @param msg
   * @param sourceKey
   * @param rule
   * @param bundle
   * @param actionType
   */
  private synchronized void processAndReply(MQMessage msg, String sourceKey, AutoResponse rule,
      MQConnectionBundle bundle, int actionType) {
    SimulatorProperties.QueueConfig sourceQ = props.getQueues().get(sourceKey);

    try {
      traceGet("<<< [GET]", sourceQ.getName(), sourceKey, msg.characterSet,
          bytesToHex(msg.correlationId));
      AtomicBoolean isCompleted = new AtomicBoolean(false);
      enviarSafe(emittersActivos.get("A"),
          "Autorespuesta: " + sourceQ.getName() + " | CCSID: " + msg.characterSet + " | HEX: "
              + bytesToHex(msg.correlationId),
          isCompleted);
      MessagesMgr messagesMgr = new MessagesMgr();
      boolean isCopyCorrel = rule.isCopyCorrel();
      if (rule.getPayloadKey().startsWith("payload:")) {
        String customPayload = rule.getPayloadKey().substring(8);
        String mgr = messagesMgr.createStrPayload(customPayload);
        lSend(rule.getTargetQueue(), mgr, msg.correlationId, msg.messageId,
            isCopyCorrel, "", "", true, java.util.Optional.empty(), bundle, true);
      } else {
        lSend(rule.getTargetQueue(), rule.getPayloadKey(), msg.correlationId,
            msg.messageId, isCopyCorrel, "", "", true, java.util.Optional.empty(),
            bundle, true);
      }
    } catch (Exception e) {
      log.error("Error procesando mensaje en processAndReply para {}: {}", sourceKey,
          e.getMessage(), e);
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
  @LogFullDetails(logResult = true)
  private void lSend(String targetQueue, String payloadKey, byte[] correlationId, byte[] messageId,
      boolean isCopyCorrel, String string, String string2, boolean b, Optional<QueueConfig> empty,
      MQConnectionBundle bundle, boolean c) {
    MQConnectionBundle bund = connections.get(targetQueue);
    pool.execute(() -> {
      try {
        send(targetQueue, payloadKey, correlationId, messageId, isCopyCorrel, string, string2, b,
            empty, null, bund, emitter);
      } catch (Exception e) {
        log.error("Error al enviar mensaje desde processAndReply para " + targetQueue, e);
      }
    });

  }

  public Flux<MQMessage> createFlux(String queueName, Consumer<MQMessage> action, int actionType) {
    MQQueue queue = getQueue(queueName); // Asegura que la cola esté inicializada y
                                         // abierta
    if (queue == null) {
      return Flux
          .error(new IllegalArgumentException("La cola " + queueName + " no está configurada."));
    }
    log.info("Creando Flux para autorespuesta en cola '{}'", queueName);
    return Flux.<MQMessage>create((sink) -> {
      // Hilo de lectura para esta cola específica
      MQConnectionBundle bundle = connections.get(queueName);
      AtomicBoolean isRunning = new AtomicBoolean(true);
      while (!sink.isCancelled()) {
        try {
          MQMessage msg = new MQMessage();
          MQGetMessageOptions gmo = new MQGetMessageOptions();
          gmo.options = MQConstants.MQGMO_WAIT | MQConstants.MQGMO_FAIL_IF_QUIESCING;
          gmo.waitInterval = 30_000;
          queue.get(msg, gmo);

          sink.next(msg);
        } catch (MQException e) {
          if (e.reasonCode != MQConstants.MQRC_NO_MSG_AVAILABLE) {
            log.error("Error MQ en {}: {}", queueName, e.reasonCode);
            if (queue.isOpen()) {
              log.info("Intentando continuar escuchando en {} tras error MQ...", queueName);
            } else {
              log.warn("La cola {} parece estar cerrada. Deteniendo Flux.", queueName);
            }
            connections.clear(); // Limpia conexiones para forzar reconexión en el watchdog
            isRunning.set(false);
            break;
          }
        }
      }
      if (!isRunning.get()) {
        sink.complete();
      }
    })
        .subscribeOn(Schedulers.newSingle("Thread-" + queueName)) // Hilo físico dedicado
        .doOnNext(action); // Aquí se ejecuta tu "doMethod()" cada vez que llega un mensaje
  }

  private void enviarSafe(SseEmitter emitter, String texto, AtomicBoolean isCompleted) {
    if (isCompleted.get() || emitter == null) {
      return;
    }
    try {
      emitter.send(texto);
    } catch (IOException | IllegalStateException e) {
      log.error("Error enviando mensaje: {}", e.getMessage());
      // isCompleted.set(true);
      // Si el emisor falla, lo limpiamos de todos los sitios
      // emittersActivos.values().remove(emitter);
    }
  }
}

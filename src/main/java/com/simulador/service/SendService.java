/**
 * 
 */
package com.simulador.service;

import com.ibm.mq.MQException;
import com.ibm.mq.MQMessage;
import com.ibm.mq.MQPutMessageOptions;
import com.ibm.mq.MQQueue;
import com.ibm.mq.constants.MQConstants;
import com.simulador.aspect.LogFullDetails;
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
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter.SseEventBuilder;

/**
 * SendService - Servicio encargado de enviar mensajes a las colas MQ configuradas. Este servicio
 * maneja la lógica de construcción del mensaje, asignación de correlationId y messageId. También se
 * encarga de simular tiempos de procesamiento y disparar mensajes adicionales configurados en
 * "fire".
 */
@Service
@Slf4j
@RefreshScope
@LogFullDetails(logArguments = true, logResult = false) // Configura el aspecto para no loguear
// argumentos ni resultados (puede contener
// datos sensibles)
public class SendService extends Utils {

  @Autowired
  private SimulatorProperties props;

  @Autowired
  @Lazy
  MessagesMgr messagesMgr;

  Map<String, MQConnectionBundle> connections;

  @Autowired
  ApplicationContext appContext;

  private RoundRobinExecutorPool pool = new RoundRobinExecutorPool("pool", 25);

  private final Utils utils = new Utils();

  private Map<String, String> messages = new ConcurrentHashMap<>();

  private final Map<String, SseEmitter> emittersActivos = new ConcurrentHashMap<>();

  @Setter
  private int contador;

  public SendService(SimulatorProperties props, Map<String, MQConnectionBundle> connection) {
    super();
    this.props = props;
    this.connections = connection;
  }

  private SseEmitter emitter;

  @Value("${simulador.role:DEFAULT_START}")
  private String role;

  @Setter
  private int actual;

  @Setter
  private long startTime;

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
   * @return
   * @throws Exception
   */
  @LogFullDetails(logResult = true)
  public String send(String queueKey, String payloadKey, byte[] correlationId, byte[] messageId,
      boolean isCopyCorrel,
      String replyTo, String replyToQMgr, boolean processed, Optional<QueueConfig> overrideConfig,
      String source, MQConnectionBundle clone, SseEmitter emitter)
      throws Exception {

    this.emitter = emitter;
    if (emittersActivos.get(source) == null && emitter != null) {
      emittersActivos.put(source, emitter);
    }
    String content = "";
    if (role.equals("T3270_START")) {
      content = payloadKey;
    } else {
      content = props.getPayloads().get(payloadKey);
    }
    SimulatorProperties.QueueConfig qConfig = props.getQueues().get(queueKey);
    MQQueue queue;
    if (clone != null && false) {
      queue = clone.getQueue();
    } else {
      queue = this.getQueue(qConfig.getName());
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
      if (correlation.equals("msg-correl") && !role.equals("SOH_START")) {
        msg.messageId = msg.correlationId;
      } else if (correlation.equals("msg-correl") && role.equals("SOH_START")) {
        if (messageId != null) {
          msg.correlationId = messageId;
        }
      } else if (correlation.equals("correl-correl")) {
        msg.messageId = msg.correlationId;
      } else if (correlation.equals("msg-msg")) {
        msg.correlationId = messageId;
      } else if (qConfig.isCopyCorrel() && !role.equals("SOH_START")) {
        msg.messageId = msg.correlationId;
      } else if (qConfig.isCopyCorrel() && role.equals("SOH_START")) {
        msg.correlationId = messageId;
      }
    }
    if (msg.messageId == null || utils.isNullOrEmpty(msg.messageId)) {
      msg.messageId = Utils.generateRandomId(24);
    }
    if (qConfig.getTresp() > 0) {
      log.debug(
          "*** Simulando tiempo de procesamiento de " + qConfig.getTresp() + " ms para " + queueKey
              + " ***");
      TimeUnit.MILLISECONDS.sleep(qConfig.getTresp()); // Simular tiempo de procesamiento
    }
    String payload = messages.get(payloadKey);
    if (payload == null) {
      payload = messagesMgr.checkPayload(payloadKey, props, emittersActivos);
      messages.put(payloadKey, payload);
    }
    msg.writeString(payload);
    TimeZone utc = TimeZone.getTimeZone("Zulu");
    GregorianCalendar cal = new GregorianCalendar(utc);
    msg.putDateTime = cal;
    // msg.seek(0);
    try {
      queue.put(msg, new MQPutMessageOptions());
    } catch (MQException e) {
      log.error("Error al enviar mensaje a {}: {}. Intentando reconectar...", qConfig.getName(),
          e.getMessage(), e);
      // connections.remove(qConfig.getName()); // Limpia la conexión para forzar reconexión en el
      // // watchdog
    }
    String key = queueKey.length() > 15 ? queueKey.substring(0, 15) + "..." : queueKey;
    log.debug(
        ">>> [PUT] " + qConfig.getName() + " --> " + String.format("%-18s", key)
            + " | CCSID: "
            + qConfig.getCcsid() + " | HEX: "
            + bytesToHex(msg.correlationId));
    AtomicBoolean isCompleted = new AtomicBoolean(false);
    if (emittersActivos.get("A") != null) {
      enviarSafe(emittersActivos.get("A"),
          qConfig.getName() + " | CCSID: " + qConfig.getCcsid() + " | HEX: "
              + bytesToHex(msg.correlationId),
          "Enviando",
          isCompleted);
    }
    log.trace(
        ">>> [FIRE] Verificando mensajes adicionales configurados en fire para " + queueKey);
    checkFire(qConfig, msg.correlationId, msg.messageId, isCopyCorrel, replyTo, replyToQMgr,
        source);
    return bytesToHex(msg.correlationId);
  }

  // @Cacheable(value = "queues", key = "#name")
  public MQQueue getQueue(String name) {
    MQConnectionBundle bundle = connections.get(name);
    return queues.compute(name, (key, existingQueue) -> {
      if (existingQueue == null || !existingQueue.isOpen()) {
        return bundle.getQueue();
      }
      return existingQueue;
    });
  }

  public void enviarSafe(SseEmitter emitter, String texto, String id, AtomicBoolean isCompleted) {
    if (isCompleted.get() || emitter == null) {
      return;
    }
    try {
      // emitter.send(texto);
      SseEventBuilder event = SseEmitter.event();
      event.id(id);
      if (id.toLowerCase().contains("last") || id.toLowerCase().contains("error")) {
        log.debug("--- [SSE] '{}'", texto);
      }
      emitter.send(event.data(texto));
    } catch (IOException | IllegalStateException e) {
      log.error("Error enviando mensaje: {}", e.getMessage());
      // isCompleted.set(true);
      // Si el emisor falla, lo limpiamos de todos los sitios
      // emittersActivos.values().remove(emitter);
    }
  }

  /**
   * @param qConfig
   * @param correlationId
   * @param messageId
   * @param isCopyCorrel
   * @param replyTo
   * @param replyToQMgr
   */
  @LogFullDetails(logResult = false)
  private void checkFire(QueueConfig qConfig, byte[] correlationId, byte[] messageId,
      boolean isCopyCorrel, String replyTo, String replyToQMgr, String source) {
    if (qConfig.getFire() == null || qConfig.getFire().getQueues() == null
        || qConfig.getFire().getQueues().isEmpty()) {
      return;
    }
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

  /**
   * @param msg
   * @param sourceKey
   * @param rule
   * @param bundle
   * @param actionType
   * @return
   */
  @LogFullDetails(logResult = true)
  public synchronized String processAndReply(MQMessage msg, String sourceKey, AutoResponse rule,
      MQConnectionBundle bundle, int actionType) {
    SimulatorProperties.QueueConfig sourceQ = props.getQueues().get(sourceKey);
    SimulatorProperties.QueueConfig sourceQ2 = props.getQueues().get(rule.getTargetQueue());

    // bundle = connections.get(sourceQ.getName()); // Asegura que usamos la conexión actualizada
    try {
      traceGet("<<< [GET]", sourceQ.getName(), sourceKey, msg.characterSet,
          bytesToHex(msg.correlationId));
      AtomicBoolean isCompleted = new AtomicBoolean(false);
      if (emittersActivos.get("A") != null) {
        enviarSafe(emittersActivos.get("A"),
            sourceQ.getName() + " -> " + sourceQ2.getName() + " | CCSID: "
                + msg.characterSet + " | HEX: "
                + bytesToHex(msg.correlationId),
            "Autorespuesta",
            isCompleted);
      }
      boolean isCopyCorrel = rule.isCopyCorrel();
      lSend(rule.getTargetQueue(), rule.getPayloadKey(), msg.correlationId,
          msg.messageId, isCopyCorrel, "", "", true, java.util.Optional.empty(),
          bundle, true);
      SimulatorProperties.QueueConfig qConfig = props.getQueues().get(sourceKey);
      log.trace(
          ">>> [FIRE] Verificando mensajes adicionales configurados en fire para "
              + sourceKey);
      checkFire(qConfig, msg.correlationId, msg.messageId, isCopyCorrel, "", "",
          "A");
    } catch (Exception e) {
      log.error("Error procesando mensaje en processAndReply para {}: {}", sourceKey,
          e.getMessage(), e);
    }
    return bytesToHex(msg.correlationId);
  }

  /**
   * @param msg2
   * @param consumeRule
   * @param i
   * @param bundle
   * @param last
   * @return
   */
  @LogFullDetails(logResult = true)
  public void consumeAndLog(MQMessage msg, String key, String queue, int i,
      MQConnectionBundle bundle, int actionType, boolean last) {
    try {
      traceGet("<<< [PRG]", queue, key, msg.characterSet,
          bytesToHex(msg.correlationId));
      AtomicBoolean isCompleted = new AtomicBoolean(false);
      if (emittersActivos.get("A") != null) {
        enviarSafe(emittersActivos.get("A"),
            queue + " | CCSID: " + msg.characterSet + " | HEX: "
                + bytesToHex(msg.correlationId),
            "Consumer",
            isCompleted);
      }
      log.trace(
          ">>> [FIRE] Verificando mensajes adicionales configurados en fire para "
              + key);
      SimulatorProperties.QueueConfig qConfig = props.getQueues().get(key);
      checkFire(qConfig, msg.correlationId, msg.messageId, false, "", "",
          "A");
      actual++;
      if (last && actual >= contador) {
        enviarSafe(emittersActivos.get("A"), contador + " mensajes procesados en "
            + (System.currentTimeMillis() - startTime) + "ms.",
            "Last",
            isCompleted);
        start = false;
      }
    } catch (Exception e) {
      log.error("Error procesando mensaje en cola {}: {}", queue, e.getMessage());
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
  public void traceGet(String string, String consumeRule, String key,
      int characterSet, String correlationId) {
    if (log.isDebugEnabled()) {
      key = key.length() > 15 ? key.substring(0, 15) + "..." : key;
      log.debug(string + " {} <-- {} | CCSID: {} | HEX: {} ",
          consumeRule, String.format("%-18s", key), characterSet,
          correlationId);
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
  public void lSend(String targetQueue, String payloadKey, byte[] correlationId, byte[] messageId,
      boolean isCopyCorrel, String string, String string2, boolean b, Optional<QueueConfig> empty,
      MQConnectionBundle bundle, boolean c) {
    MQConnectionBundle bund = connections.get(targetQueue);
    if (bund != null) {
      try {
        bund.getQueue().getOpenOutputCount();
      } catch (MQException e) {
        log.error("Error verificando conexión para {}: {}. Intentando reconectar...", targetQueue,
            e.getMessage(), e);
        e.printStackTrace();
      }
    }
    pool.execute(() -> {
      try {
        send(targetQueue, payloadKey, correlationId, messageId, isCopyCorrel, string,
            string2, b, empty, "A", bund, emitter);
      } catch (Exception e) {
        log.error("Error al enviar mensaje desde processAndReply para " + targetQueue, e);
      }
    });

  }

  public void registrarEmitter(String key, SseEmitter emitter) {
    this.emittersActivos.put(key, emitter);
  }

  public void removerEmitter(String key) {
    this.emittersActivos.remove(key);
  }
}

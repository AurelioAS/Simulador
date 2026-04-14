package com.simulador.service;

import com.ibm.mq.MQException;
import com.ibm.mq.MQGetMessageOptions;
import com.ibm.mq.MQMessage;
import com.ibm.mq.MQQueue;
import com.ibm.mq.constants.MQConstants;
import com.simulador.components.JsonService;
import com.simulador.config.MQConnectionBundle;
import com.simulador.config.SimulatorProperties;
import com.simulador.config.SimulatorProperties.AutoResponse;
import com.simulador.utils.Utils;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import lombok.Getter;
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

  @Getter
  private boolean isRunning = true;

  private final Utils                   utils           = new Utils();
  private SseEmitter                    emitter;
  private final Map<String, SseEmitter> emittersActivos = new ConcurrentHashMap<>();
  private SendService                   sendService;

  @Autowired
  public MqSimulatorService(Map<String, MQConnectionBundle> connection,
      SimulatorProperties props,
      JsonService jsonService, SendService sendService) {
    this.connections = connection;
    this.props = props;
    this.jsonService = jsonService;
    this.sendService = sendService;
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
              sendService.processAndReply(msg, sourceKey, rule, bundle, 1);
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
                  sendService.consumeAndLog(msg, consumeRule, sourceQ.getName(), index, bundle, 2);
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
    queues.clear();
    run();
    isRunning = true;
  }

  public void registrarEmitter(String key, SseEmitter emitter) {
    this.emittersActivos.put(key, emitter);
  }

  public void removerEmitter(String key) {
    this.emittersActivos.remove(key);
  }

  public Flux<MQMessage> createFlux(String queueName, Consumer<MQMessage> action, int actionType) {
    MQQueue queue = sendService.getQueue(queueName); // Asegura que la cola esté inicializada y
                                         // abierta
    if (queue == null) {
      return Flux
          .error(new IllegalArgumentException("La cola " + queueName + " no está configurada."));
    }
    log.trace("Creando Flux para autorespuesta en cola '{}'", queueName);
    return Flux.<MQMessage>create((sink) -> {
      // Hilo de lectura para esta cola específica
      MQConnectionBundle bundle = connections.get(queueName);
      while (!sink.isCancelled()) {
        try {
          isRunning = true;
          MQMessage msg = new MQMessage();
          MQGetMessageOptions gmo = new MQGetMessageOptions();
          gmo.options = MQConstants.MQGMO_WAIT | MQConstants.MQGMO_FAIL_IF_QUIESCING;
          gmo.waitInterval = 30_000;
          queue.get(msg, gmo);
          sink.next(msg);
        } catch (MQException e) {
          if (e.reasonCode != MQConstants.MQRC_NO_MSG_AVAILABLE) {
            log.error("Error MQ en {}: {}", queueName, e.reasonCode);
            try {
              if (emittersActivos.get("A") != null) {
                emittersActivos.get("A").send("Error: MQ en " + queueName + ": " + e.reasonCode);
              }
            } catch (IOException e1) {
              e1.printStackTrace();
            }
            if (queue.isOpen()) {
              log.info("Intentando continuar escuchando en {} tras error MQ...", queueName);
            } else {
              log.warn("La cola {} parece estar cerrada. Deteniendo Flux.", queueName);
            }
            connections.clear(); // Limpia conexiones para forzar reconexión en el watchdog
            isRunning = false;
            break;
          }
        }
      }
      if (!isRunning) {
        sink.complete();
      }
    })
        .subscribeOn(Schedulers.newSingle("Thread-" + queueName)) // Hilo físico dedicado
        .doOnNext(action); // Aquí se ejecuta tu "doMethod()" cada vez que llega un mensaje
  }

}

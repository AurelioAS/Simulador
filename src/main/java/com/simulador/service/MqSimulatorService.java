package com.simulador.service;

import com.ibm.mq.MQException;
import com.ibm.mq.MQGetMessageOptions;
import com.ibm.mq.MQMessage;
import com.ibm.mq.MQQueue;
import com.ibm.mq.constants.MQConstants;
import com.simulador.components.JsonService;
import com.simulador.config.MQConnectionBundle;
import com.simulador.config.MQConnectionManager;
import com.simulador.config.SimulatorProperties;
import com.simulador.config.SimulatorProperties.AutoResponse;
import com.simulador.config.SimulatorProperties.Consumers;
import com.simulador.utils.Utils;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Profile;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

@Service
@Slf4j
@RefreshScope
@Profile("simulator")
@DependsOn("mqConnections")
public class MqSimulatorService extends Utils {

  private final SimulatorProperties props;

  private Map<String, MQConnectionBundle> connections;

  private RoundRobinExecutorPool pool = new RoundRobinExecutorPool("pool", 25);

  @Getter
  private boolean isRunning = true;

  @Autowired
  private SendService sendService;

  private final Map<String, SseEmitter> emittersActivos = new ConcurrentHashMap<>();

  private MQConnectionManager mqConnectionManager;

  @Autowired
  public MqSimulatorService(MQConnectionManager mqConnectionManager,
      Map<String, MQConnectionBundle> connection,
      SimulatorProperties props, JsonService jsonService) {
    this.mqConnectionManager = mqConnectionManager;
    this.connections = connection;
    this.props = props;
  }

  /**
   * 
   */
  public MqSimulatorService(SimulatorProperties props, JsonService jsonService) {
    this.props = props;
  }

  @EventListener(ApplicationReadyEvent.class)
  public void run() {
    log.info("Iniciando autorespuestas y consumidor en modo " + props.getRole());
    Map<String, AutoResponse> responses = props.getAutoResponses();
    Map<String, Integer> actives =
        sendService.activeQueues == null ? new LinkedHashMap<>() : sendService.activeQueues;
    Map<String, Integer> activesCon =
        sendService.activeQueuesCon == null ? new LinkedHashMap<>() : sendService.activeQueuesCon;
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
              sendService.processAndReply(msg, sourceKey, rule, bundle, 1, rule.isLast());
            }, 1).subscribe();
          }

          actives.put(sourceQ.getName(), 0);
        } catch (Exception ignored) {
          log.error("Error configurando autorespuesta para {}: {}", sourceKey,
              ignored.getMessage());
        }
      });
    }

    Map<String, Consumers> consumerQueue = props.getConsumers();
    if (consumerQueue != null) {
      consumerQueue.forEach((key, consumeRule) -> {
        String sourceQ = props.getQueues().get(key).getName();
        int poolSize = props.getNumThreads() > 0 ? props.getNumThreads() : 1;
        log.info("Configurada escucha y purgado y Flux para '{}' ({})", sourceQ,
            poolSize);
        MQConnectionBundle[] bundle = new MQConnectionBundle[1];
        try {
          bundle[0] = connections.get(sourceQ);
          queues.put(sourceQ, bundle[0].getQueue()); // Asegura que la cola esté en el mapa
        } catch (Exception e) {
          log.error("Error accediendo a la cola {}: {}", sourceQ, e.getMessage());
          return;
        }
        for (int i = 0; i < poolSize; i++) {
          try {
            int index = i % poolSize;
            createFlux(sourceQ,
                (msg) -> {
                  sendService.consumeAndLog(msg, key, sourceQ, index, bundle[0], 2,
                      consumeRule.isLast());
                }, 2).subscribe();
          } catch (Exception e) {
            log.error("Error configurando escucha para {}: {}", sourceQ, e.getMessage());
            e.printStackTrace();
          }
          activesCon.put(sourceQ, 0);
        }
      });
    }
  }

  public void runConsumers() {
    // queues.clear();
    run();
    isRunning = true;
  }

  public void registrarEmitter(String key, SseEmitter emitter) {
    this.emittersActivos.put(key, emitter);
  }

  public void removerEmitter(String key) {
    this.emittersActivos.remove(key);
  }

  @SuppressWarnings("unused")
  public Flux<MQMessage> createFlux(String queueName, Consumer<MQMessage> action, int actionType) {
    MQQueue[] queue1 = new MQQueue[1];
    queue1[0] = sendService.getQueue(queueName); // Asegura que la cola esté inicializada y
    // abierta
    if (queue1 == null) {
      return Flux
          .error(new IllegalArgumentException("La cola " + queueName + " no está configurada."));
    }
    log.trace("Creando Flux para autorespuesta en cola '{}'", queueName);
    return Flux.<MQMessage>create((sink) -> {
      // Hilo de lectura para esta cola específica
      while (!sink.isCancelled()) {
        try {
          MQQueue queue = queues.get(queueName);
          if (queue == null || !queue.isOpen()) {
            log.error("La cola {} no está disponible. Deteniendo Flux.", queueName);
            TimeUnit.SECONDS.sleep(5);
            continue;
          }
          queue1[0] = queue;
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
            connections.clear();
            queues.clear();
            isRunning = false;
            try {
              TimeUnit.SECONDS.sleep(35);
            } catch (InterruptedException e1) {
              log.error("Hilo de reconexión interrumpido para {}: {}", queueName, e1.getMessage());
            }
            break;
          }
        } catch (InterruptedException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
      if (!isRunning) {
        sink.complete();
      }
    })
        .subscribeOn(Schedulers.newSingle("Thread-" + queueName)) // Hilo físico dedicado
        .doOnNext(action); // Aquí se ejecuta tu "doMethod()" cada vez que llega un mensaje
  }

  /**
   * Tarea programada que revisa la salud de las conexiones cada 30 segundos
   */
  @Scheduled(fixedDelay = 15000)
  public void watchdog() {
    AtomicBoolean allHealthy = new AtomicBoolean(true);
    props.getQueues().forEach((key, config) -> {
      if (!isConnectionActive(config.getName())) {
        log.warn("Detectada conexión caída para {}. Reconectando...", key);
        MQConnectionBundle bundle = mqConnectionManager.connectQueue(config.getName());
        if (bundle == null) {
          log.error("Reconexión fallida para {}", key);
          return;
        }
        if (sendService.queues == null) {
          sendService.queues = new LinkedHashMap<>();
        }
        if(sendService.connections == null) {
          sendService.connections = new LinkedHashMap<>();
        }
        boolean connected = !(bundle == null);
        connections.put(config.getName(), bundle);
        queues.put(config.getName(), bundle.getQueue());
        sendService.connections.put(config.getName(), bundle);
        sendService.queues.put(config.getName(), bundle.getQueue());
        allHealthy.set(!connected);
      }
    });
    if (!allHealthy.get()) {
      log.info("Reconexiones realizadas. Verifique los logs para más detalles.");
      runConsumers();
    }
  }

  /**
   * Verifica si una conexión específica sigue viva
   */
  public boolean isConnectionActive(String queueName) {
    MQConnectionBundle bundle = connections.get(queueName);
    if ((bundle == null || bundle.getQm() == null || bundle.getQueue() == null)
        || !bundle.getQm().isConnected() || !bundle.getQueue().isOpen()) {
      return false;
    }
    try {
      // La mejor forma de saber si sigue vivo es preguntar algo al QMgr
      return bundle.getQm().isConnected() && bundle.getQueue().isOpen();
    } catch (Exception e) {
      return false;
    }
  }
}

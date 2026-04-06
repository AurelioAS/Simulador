package com.simulador.config;


import com.ibm.mq.MQException;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.MQConstants;
import com.simulador.components.JsonService;
import com.simulador.service.MqSimulatorService;
import jakarta.annotation.PostConstruct;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.DependsOn;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@DependsOn("cacheManager")
public class MQConnectionManager {

  private final SimulatorProperties props;
  private final MQNativeConfig      nativeConfig;
  private final JsonService         jsonService;

  @Autowired
  ApplicationContext context;
  // Este es el mapa que consumirán tus otros servicios
  private final ConcurrentMap<String, MQConnectionBundle> connectionMap = new ConcurrentHashMap<>();
  public MQQueueManager                                   qMan;

  public MQConnectionManager(SimulatorProperties props, MQNativeConfig nativeConfig,
      JsonService jsonService) {
    this.props = props;
    this.nativeConfig = nativeConfig;
    this.jsonService = jsonService;

    try {
      Map<String, Object> conn = nativeConfig.createConnection("SIMU");
      MQQueueManager qmTemp = (MQQueueManager) conn.get("mq");
      log.info(
          "Probando conexión del canal..."
              + nativeConfig.getProps().get(MQConstants.CHANNEL_PROPERTY));
      qMan = qmTemp;
      qmTemp.disconnect();
      qmTemp.close();
    } catch (MQException e) {
      e.printStackTrace();
    }
  }

  @PostConstruct
  public void init() {
    log.info("Iniciando conexión masiva a MQ...");
    connectAll();
  }

  /**
   * Intenta conectar todas las colas configuradas
   */
  public void connectAll() {
    props.getQueues().forEach((key, qConfig) -> {
      if (!isConnectionActive(key)) {
        connectQueue(qConfig.getName());
      }
    });
  }

  /**
   * Lógica de conexión para una cola individual
   * 
   * @return
   */
  public synchronized boolean connectQueue(String queueName) {
    try {
      // Limpiar conexión vieja si existe
      closeQuietly(queueName);

      log.trace("Intentando conectar a la cola: {}", queueName);
      var conn = nativeConfig.createConnection(queueName);
      MQQueueManager qm = (MQQueueManager) conn.get("mq");

      int options = getOptionsForQueue(queueName);
      MQQueue queue = qm.accessQueue(queueName, options);

      connectionMap.put(queueName, new MQConnectionBundle(qm, queue, conn.get("props")));
      log.debug("✅ Conexión establecida con éxito: {}", queueName);
      return true;

    } catch (MQException e) {
      log.error("❌ Error conectando a {}. Motivo: {} (Reason Code: {})",
          queueName, e.getMessage(), e.getReason());
      return false;
    } catch (Exception e) {
      log.error("❌ Error inesperado conectando a {}", queueName, e);
      return false;
    }
  }

  /**
   * Verifica si una conexión específica sigue viva
   */
  public boolean isConnectionActive(String queueName) {
    MQConnectionBundle bundle = connectionMap.get(queueName);
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

  /**
   * Tarea programada que revisa la salud de las conexiones cada 30 segundos
   */
  @Scheduled(fixedDelay = 30000)
  public void watchdog() {
    AtomicBoolean allHealthy = new AtomicBoolean(true);
    props.getQueues().forEach((key, config) -> {
      if (!isConnectionActive(config.getName())) {
        log.warn("Detectada conexión caída para {}. Reconectando...", key);
        boolean connected = connectQueue(config.getName());
        allHealthy.set(!connected);
      }
    });
    if (!allHealthy.get()) {
      log.info("Reconexiones realizadas. Verifique los logs para más detalles.");
      MqSimulatorService theService = context.getBean(MqSimulatorService.class);
      theService.runConsumers();
    }
  }

  @Bean(name = "mqConnections")
  public ConcurrentMap<String, MQConnectionBundle> getConnections() {
    return this.connectionMap;
  }

  private int getOptionsForQueue(String queueName) {
    List<String> res = props.getGetQueues();
    if (res != null && res.contains(queueName)) {
      return MQConstants.MQOO_OUTPUT | MQConstants.MQOO_INPUT_AS_Q_DEF
          | MQConstants.MQOO_INQUIRE | MQConstants.MQOO_FAIL_IF_QUIESCING
          | MQConstants.MQPMO_SET_ALL_CONTEXT;
    } else {
      return MQConstants.MQOO_OUTPUT | MQConstants.MQOO_INQUIRE
          | MQConstants.MQOO_FAIL_IF_QUIESCING | MQConstants.MQPMO_SET_ALL_CONTEXT;
    }
  }

  private void closeQuietly(String queueName) {
    MQConnectionBundle old = connectionMap.remove(queueName);
    if (old != null) {
      try {
        if (old.getQueue() != null)
          old.getQueue().close();
        if (old.getQm() != null) {
          old.getQm().disconnect();
          old.getQm().close();
        }
      } catch (Exception e) {
        // Ignorar fallos al cerrar una conexión ya rota
      }
    }
  }
}

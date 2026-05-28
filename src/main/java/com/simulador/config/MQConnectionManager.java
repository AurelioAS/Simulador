package com.simulador.config;


import com.ibm.mq.MQException;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.MQConstants;
import com.simulador.components.JsonService;
import jakarta.annotation.PostConstruct;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RefreshScope
// @DependsOn("cacheManager")
public class MQConnectionManager {

  private final SimulatorProperties props;
  private final MQNativeConfig      nativeConfig;
  private final JsonService         jsonService;

  @Autowired
  ApplicationContext context;
  // Este es el mapa que consumirán tus otros servicios
  private final Map<String, MQConnectionBundle> connectionMap = new LinkedHashMap<>();
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
    log.info("Iniciando conexines a MQ...");
    connectAll();
  }

  /**
   * Intenta conectar todas las colas configuradas
   */
  public void connectAll() {
    props.getQueues().forEach((key, qConfig) -> {
      // if (!isConnectionActive(key)) {
        connectQueue(qConfig.getName());
      // }
    });
  }

  /**
   * Lógica de conexión para una cola individual
   * 
   * @return
   */
  public synchronized MQConnectionBundle connectQueue(String queueName) {
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
      return new MQConnectionBundle(qm, queue, conn.get("props"));

    } catch (MQException e) {
      log.error("❌ Error conectando a {}. Motivo: {} (Reason Code: {})",
          queueName, e.getMessage(), e.getReason());
      return null;
    } catch (Exception e) {
      log.error("❌ Error inesperado conectando a {}", queueName, e);
      return null;
    }
  }

  @Bean(name = "mqConnections")
  public Map<String, MQConnectionBundle> getConnections() {
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
        // if (old.getQm() != null) {
        // old.getQm().disconnect();
        // old.getQm().close();
        // }
      } catch (Exception e) {
        // Ignorar fallos al cerrar una conexión ya rota
      }
    }
  }
}

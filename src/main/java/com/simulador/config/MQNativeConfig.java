package com.simulador.config;

import com.ibm.mq.MQException;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.MQConstants;
import java.util.Hashtable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

@Configuration
@Slf4j
public class MQNativeConfig {

  @Autowired
  MQProperties mqProperties;

  static {
    System.setProperty("com.ibm.mq.pooling.isPoolingEnabled", "false");
  }

  public MQQueueManager createConnection(String appName) throws MQException {
    Hashtable<String, Object> props = new Hashtable<>();

    // Configuración básica de red
    props.put(MQConstants.HOST_NAME_PROPERTY, mqProperties.getHost());
    props.put(MQConstants.PORT_PROPERTY, mqProperties.getPort());
    props.put(MQConstants.CHANNEL_PROPERTY, mqProperties.getChannel());
    props.put(MQConstants.TRANSPORT_PROPERTY, MQConstants.TRANSPORT_MQSERIES_CLIENT);

    // Autenticación
    props.put(MQConstants.USE_MQCSP_AUTHENTICATION_PROPERTY, false);
    props.put(MQConstants.USER_ID_PROPERTY, mqProperties.getUser());
    props.put(MQConstants.PASSWORD_PROPERTY, mqProperties.getPassword());

    // --- EL ARREGLO PARA EL BLOQUEO ---
    // 1. Identificador de aplicación único (forzamos a MQ a no agrupar)
    props.put(MQConstants.APPNAME_PROPERTY, appName);

    // 2. Opciones de conexión: Reconnect + No compartir handles
    // Usamos SHARE_BLOCK si vas a usar varios hilos para el mismo manager,
    // pero SHARE_NONE es el más agresivo para separar GET de PUT.
    props.put(MQConstants.CONNECT_OPTIONS_PROPERTY,
        MQConstants.MQCNO_RECONNECT | MQConstants.MQCNO_HANDLE_SHARE_BLOCK);

    try {
      return new MQQueueManager(mqProperties.getQueueManager(), props);
    } catch (MQException e) {
      log.error("Error conectando a MQ con AppName {}: [CC:{} RC:{}]",
          appName, e.completionCode, e.reasonCode);
      throw e;
    }
  }
}
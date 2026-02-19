package com.simulador.config;

import com.ibm.mq.MQException;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.MQConstants;
import java.util.Hashtable;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@Getter
@Setter
@Slf4j
public class MQNativeConfig {

  private static Hashtable<String, Object> props = new Hashtable<>();
  private static String                    qMgr;


  @Bean
  public MQQueueManager mqQueueManager(
      @Value("${ibm.mq.host}") String host,
      @Value("${ibm.mq.port}") int port,
      @Value("${ibm.mq.queue-manager}") String qm,
      @Value("${ibm.mq.channel}") String channel,
      @Value("${ibm.mq.user}") String user,
      @Value("${ibm.mq.password}") String pass) throws MQException {
    this.qMgr = qm;
    props.put(MQConstants.HOST_NAME_PROPERTY, host);
    props.put(MQConstants.PORT_PROPERTY, port);
    props.put(MQConstants.CHANNEL_PROPERTY, channel);
    props.put(MQConstants.USE_MQCSP_AUTHENTICATION_PROPERTY, false);
    props.put(MQConstants.USER_ID_PROPERTY, user);
    props.put(MQConstants.PASSWORD_PROPERTY, pass);
    props.put(MQConstants.TRANSPORT_PROPERTY, MQConstants.TRANSPORT_MQSERIES_CLIENT);

    props.put(MQConstants.CONNECT_OPTIONS_PROPERTY, MQConstants.MQCNO_RECONNECT);
    log.info("MQ Connection properties set: {}", props);
    return new MQQueueManager(qm, props);
  }

  public static MQQueueManager mqQueueManager() throws MQException {
    return new MQQueueManager(qMgr, props);
  }
}

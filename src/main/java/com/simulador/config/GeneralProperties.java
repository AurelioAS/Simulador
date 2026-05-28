/**
 * 
 */
package com.simulador.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties // Sin prefijo, busca en la raíz del YAML
@Data
public class GeneralProperties {

  private List<String> logs = new ArrayList<>(List.of("error", "completado", "last"));

  private Map<String, String> simuMq = new ConcurrentHashMap<>();

  private IbmConfig ibm;

  @Data
  public static class IbmConfig {
    private MqConfig mq;
  }

  // Esta clase representa todo lo que cuelga de "mq:"
  @Data
  public static class MqConfig {
    private String host;
    private String channel;
    private String queueManager; // Mapea "queue-manager" automáticamente
    private int    port;
    private String user;
    private String password;
  }
}
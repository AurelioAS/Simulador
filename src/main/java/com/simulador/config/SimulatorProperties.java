package com.simulador.config;

import java.util.Map;
import lombok.Data;
import lombok.Getter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties(prefix = "simulador")
@Getter
public class SimulatorProperties {
  private String                    role;
  private Map<String, QueueConfig>  queues;
  private Map<String, String>       payloads;
  private Map<String, AutoResponse> autoResponses;
  private Map<String, String>       consumers;
  private Map<String, String>       properties;
  private int                       numThreads;

  @Data
  public static class QueueConfig {
    private String name;
    private int    ccsid;
    private int    tresp;
    private String payloadKey;
    private boolean    copyCorrel;
    @NestedConfigurationProperty
    private FireConfig fire;
  }

  @Data
  public static class AutoResponse {
    private String targetQueue;
    private String payloadKey;
    private boolean copyCorrel;
  }

  @Data
  public static class Consume {
    private String[] targetQueue;
  }

  @Data
  public static class FireConfig {
    private Map<String, QueueConfig> queues;
  }
}

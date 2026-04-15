package com.simulador.config;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties(prefix = "simulador")
@JsonIgnoreProperties(ignoreUnknown = true) // Evita errores si el YAML tiene campos extra
@Slf4j
public class SimulatorProperties {

  private String                    role;
  private Map<String, QueueConfig>  queues;
  private Map<String, String>       payloads;
  private Map<String, AutoResponse> autoResponses = new HashMap<>();
  private Map<String, String>       consumers     = new HashMap<>();
  private Map<String, String>       properties;
  private int                       numThreads;
  private boolean                   fire;
  private long                      ttl;

  @Data
  public static class QueueConfig {
    private String     name;
    private int        ccsid;
    private int        tresp;
    private String     payloadKey;
    private boolean    copyCorrel;
    private FireConfig fire; // No suele ser necesario @NestedConfigurationProperty en Maps
  }

  @Data
  public static class AutoResponse {
    private String  targetQueue;
    private String  payloadKey;
    private boolean copyCorrel;
  }

  @Data
  public static class FireConfig {
    private Map<String, QueueConfig> queues;
  }

  public List<String> getGetQueues() {
    List<String> data = new ArrayList<>();
    autoResponses.forEach((key, config) -> {
      data.add(queues.get(key).getName());
    });
    consumers.forEach((key, value) -> {
      data.add(queues.get(value).getName());
    });
    return data;
  }
  public String toJson() {
    try {
      ObjectMapper mapper = new ObjectMapper();
      mapper.enable(SerializationFeature.INDENT_OUTPUT);
      // Configuramos para que no intente entrar en las tripas de Spring
      mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
      mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);

      // EXTRAEMOS LOS DATOS: Creamos un mapa manual o un DTO para evitar el Proxy
      // Si usamos 'this' directamente, Jackson seguirá intentando entrar en CGLIB
      Map<String, Object> data = new HashMap<>();
      data.put("role", this.role);
      data.put("queues", this.queues);
      data.put("payloads", this.payloads);
      data.put("consumers", this.consumers);
      data.put("numThreads", this.numThreads);
      data.put("autoResponses", this.autoResponses);

      return mapper.writeValueAsString(data);
    } catch (Exception e) {
      return "Error: " + e.getMessage();
    }
  }

  public Map<String, Object> toMap() {
    try {
      ObjectMapper mapper = new ObjectMapper();
      // Configuramos para que Jackson use los campos directamente
      mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
      mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);

      // Creamos el mapa raíz manualmente para "romper" el proxy de Spring
      Map<String, Object> root = new HashMap<>();
      root.put("role", this.role);
      root.put("numThreads", this.numThreads);

      // Para los mapas complejos, usamos convertValue
      // Al ser tipos estándar (Map, String), Jackson no se colgará
      if (this.queues != null)
        root.put("queues", mapper.convertValue(this.queues, Map.class));
      if (this.payloads != null)
        root.put("payloads", this.payloads);
      if (this.autoResponses != null)
        root.put("autoResponses", mapper.convertValue(this.autoResponses, Map.class));
      if (this.consumers != null)
        root.put("consumers", this.consumers);
      if (this.properties != null)
        root.put("properties", this.properties);

      return root;
    } catch (Exception e) {
      log.error("Error al convertir a Mapa: {}", e.getMessage());
      return Collections.emptyMap();
    }
}
}

/**
 * 
 */
package com.simulador.service;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.stereotype.Service;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

@Service
@Slf4j
@RefreshScope
public class ConfigManagerService {

  public ConfigManagerService() {
    log.info("ConfigManagerService inicializado");
  }

  private final String YML_PATH = "src/main/resources/application.yml";

  public List<String> getAllAvailableProfiles() throws Exception {
    Set<String> profiles = new LinkedHashSet<>(); // Evita duplicados y mantiene orden
    Yaml yaml = new Yaml();

    File file = new File(YML_PATH);
    if (!file.exists()) {
      return List.of("ERROR: Archivo no encontrado");
    }

    try (InputStream inputStream = new FileInputStream(file)) {
      Iterable<Object> docs = yaml.loadAll(inputStream);
      for (Object doc : docs) {
        if (doc instanceof Map) {
          findProfilesRecursive((Map<?, ?>) doc, profiles);
        }
      }
    }
    return new ArrayList<>(profiles);
  }

  private void findProfilesRecursive(Map<?, ?> map, Set<String> found) {
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      String key = String.valueOf(entry.getKey());
      Object value = entry.getValue();

      // Buscamos la clave mágica de Spring Boot 3
      if ("spring.config.activate.on-profile".equals(key)
          || "spring.config.activate.on-profile".equals(key)) {
        found.add(value.toString());
      }
      // Si el valor es otro mapa, seguimos bajando
      else if (value instanceof Map) {
        findProfilesRecursive((Map<?, ?>) value, found);
        }
    }
  }

  @SuppressWarnings("unchecked")
  public void updateActiveProfile(String newProfile) throws Exception {
    DumperOptions options = new DumperOptions();
    options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK); // <--- Fuerza el formato vertical
    options.setPrettyFlow(true); // Hace que sea más legible
    options.setIndent(2); // Define la sangría de 2 espacios

    Yaml yaml = new Yaml(options);
    Map<String, Object> data;

    try (InputStream input = new FileInputStream(new File(YML_PATH))) {
      data = yaml.load(input);
    }

    // Navegamos por el mapa: spring -> profiles -> active
    Map<String, Object> spring = (Map<String, Object>) data.get("spring");
    Map<String, Object> profiles = (Map<String, Object>) spring.get("profiles");
    profiles.put("active", newProfile);

    // Guardamos los cambios físicamente
    try (FileWriter writer = new FileWriter(YML_PATH)) {
      yaml.dump(data, writer);
    }

  }
}

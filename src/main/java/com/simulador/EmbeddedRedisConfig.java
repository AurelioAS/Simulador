/**
 * 
 */
package com.simulador;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import redis.embedded.RedisServer;

@Service
@Profile("dev")
@Slf4j
public class EmbeddedRedisConfig {

  private RedisServer redisServer;

  @Value("${spring.data.redis.port:6379}")
  private int redisPort;

  @PostConstruct
  public void startRedis() {
    // Si no ves esto, es que el @Profile("dev") no está activo
    log.info("Iniciando Redis Embebido en puerto: {}", redisPort);

    try {
      // ASIGNAMOS el resultado del build a nuestra variable de clase
      this.redisServer = RedisServer.newRedisServer()
          .port(redisPort)
          .setting("notify-keyspace-events Ex")
          // .setting("bind 127.0.0.1")
          .build();

      this.redisServer.start();

      log.info("###############################################");
      log.info("Redis Embebido ONLINE en puerto: {}", redisPort);
      log.info("###############################################");
    } catch (Exception e) {
      log.error("Error al arrancar Redis Embebido: {}", e.getMessage(), e);
    }
    }

  @PreDestroy
  public void stopRedis() {
    if (redisServer != null) {
      log.info("Deteniendo Redis Embebido...");
      try {
        redisServer.stop();
      } catch (Exception e) {
        log.error("Error al detener Redis: {}", e.getMessage());
      }
    }
    }
}
/**
 * 
 */
package com.simulador.cache;

import com.hazelcast.core.HazelcastInstance;
import com.simulador.config.SimulatorProperties;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.AutoConfigureOrder;
import org.springframework.boot.autoconfigure.cache.CacheAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.hazelcast.HazelcastAutoConfiguration;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Profile;
import org.springframework.core.Ordered;
import org.springframework.stereotype.Service;

@Profile("simu-cache")
@Slf4j
@Service
@AutoConfigureOrder(Ordered.HIGHEST_PRECEDENCE) // 1. Máxima prioridad de orden
@AutoConfigureBefore({ // 2. Ejecutar ANTES de que Spring intente configurar el cache real
    CacheAutoConfiguration.class,
    HazelcastAutoConfiguration.class
})
@EnableCaching // Activa el soporte de @Cacheable
public class SimuCacheConfig {

  @Bean
  @ConditionalOnMissingBean(name = "cacheManager") // Solo se crea si no existe otro bean con este
  @DependsOn("hazelcastInstance") // Asegura que hazelcastInstance se cree antes
  public CacheManager cacheManager(HazelcastInstance hazelcastInstance, SimulatorProperties props) {
    log.info("Inicializando MuninnCacheManager");
    return new SimuCacheManager(hazelcastInstance, props);
  }

  @PostConstruct
  public void check() {
    System.out.println("#############################################");
    System.out.println("   MUNINN CACHE CONFIG LOADED SUCCESSFULLY   ");
    System.out.println("#############################################");
  }
}
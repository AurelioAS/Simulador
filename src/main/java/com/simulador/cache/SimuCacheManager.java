/**
 * 
 */
package com.simulador.cache;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.simulador.config.SimulatorProperties;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;

@Slf4j
public class SimuCacheManager implements CacheManager {

  private final Map<String, Cache> caches = new LinkedHashMap<>();
  private HazelcastInstance                  hz;
  private SimulatorProperties                props;


  /**
   * @param hazelcastInstance
   */
  public SimuCacheManager(HazelcastInstance hazelcastInstance, SimulatorProperties props) {
    this.hz = hazelcastInstance;
    this.props = props;
  }

  @Override
  public Cache getCache(String name) {
    return caches.computeIfAbsent(name, cacheName -> {
      // AQUÍ creas tu IMapMock original
      log.info("Creando nuevo cache: " + cacheName);
      IMap<Object, Object> mockMap = hz.getMap(name);
      return new SimuCache(cacheName, mockMap, true, props);
    });
  }

  @Override
  public Collection<String> getCacheNames() {
    return caches.keySet();
  }
}
/**
 * 
 */
package com.simulador.cache;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;

@Slf4j
public class SimuCacheManager implements CacheManager {

  private final ConcurrentMap<String, Cache> caches = new ConcurrentHashMap<>();
  private HazelcastInstance                  hz;


  /**
   * @param hazelcastInstance
   */
  public SimuCacheManager(HazelcastInstance hazelcastInstance) {
    this.hz = hazelcastInstance;
  }

  @Override
  public Cache getCache(String name) {
    return caches.computeIfAbsent(name, cacheName -> {
      // AQUÍ creas tu IMapMock original
      log.info("Creando nuevo cache: " + cacheName);
      IMap<Object, Object> mockMap = hz.getMap(name);
      return new SimuCache(cacheName, mockMap, true);
    });
  }

  @Override
  public Collection<String> getCacheNames() {
    return caches.keySet();
  }
}
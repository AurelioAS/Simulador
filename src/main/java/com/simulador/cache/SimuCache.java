/**
 * 
 */
package com.simulador.cache;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.map.IMap;
import com.hazelcast.map.listener.EntryEvictedListener;
import com.hazelcast.map.listener.EntryExpiredListener;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.support.AbstractValueAdaptingCache;

@Slf4j
public class SimuCache extends AbstractValueAdaptingCache {

  private final String               name;
  private final IMap<Object, Object> store; // Tu IMapMock

  public SimuCache(String name, IMap<Object, Object> store, boolean allowNullValues) {
    super(allowNullValues);
    this.name = name;
    this.store = store;
    this.store.addEntryListener(new EntryEvictedListener<String, Object>() {
      @Override
      public void entryEvicted(EntryEvent<String, Object> event) {
        log.warn("Cache:Valor evicted para clave: " + event.getKey() + " en cache: " + name);
      }
    }, true);

    this.store.addEntryListener(new EntryExpiredListener<String, Object>() {
      @Override
      public void entryExpired(EntryEvent<String, Object> event) {
        log.info("Clave EXPIRADA (TTL alcanzado) - Key: {}", event.getKey());
      }
    }, true);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Object getNativeCache() {
    return store;
  }

  @Override
  protected Object lookup(Object key) {
    // Aquí se llama a tu método get() comprimido/binario
    if (log.isTraceEnabled()) {
      log.trace("Cache:Buscando valor para clave: " + key);
    }
    return store.get(key);
  }

  @Override
  public void put(Object key, Object value) {
    try {
      store.put(key, value, 120, TimeUnit.SECONDS); // Ejemplo con TTL de 10 minutos
    } catch (Exception e) {
      log.error("Cache:Error al almacenar en cache: " + e.getMessage());
    }
  }

  @Override
  public void evict(Object key) {
    if (log.isDebugEnabled()) {
      log.debug("Cache:Eliminando valor para clave (eviction): " + key);
    }
    store.remove(key);
  }

  @Override
  public void clear() {
    store.clear();
  }

  // Implementa putIfAbsent si lo necesitas
  @Override
  public ValueWrapper putIfAbsent(Object key, Object value) {
    Object existing = store.putIfAbsent(key, value);
    return toValueWrapper(existing);
  }

  @Override
  public <T> T get(Object key, Callable<T> valueLoader) {
    return null;
  }
}

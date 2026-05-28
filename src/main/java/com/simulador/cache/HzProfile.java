package com.simulador.cache;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.YamlClientConfigBuilder;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleEvent.LifecycleState;
import com.hazelcast.core.LifecycleListener;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * Hazelcast profile.
 */
@Configuration
@Slf4j
@Profile("simu-cache")
public class HzProfile {

  /**
   * Return a ClientConfig as a YamlClientConfigBuilder with 'hz.yml' as resource name.
   *
   * @return configuration to setup a Hazelcast Client
   * @throws IOException exception de entrada salida
   */
  @Bean
  public ClientConfig clientConfig()
      throws IOException {
    log.info("================================================================");
    log.info("Profile hz: Building ClientConfig bean");

    // Hazelcast is started only in hot mode
    return new YamlClientConfigBuilder("hz.yml").build();
  }


  /**
   * Creates a new hazelcast instance from 'cfg' parameter configuration.
   *
   * @param cfg hazelcast client configuration
   * @return hazelcast instance
   */
  @Bean
  public HazelcastInstance hazelcastInstance(ClientConfig cfg) {
    log.info("Profile hz: Building HazelcastInstance bean");
    HazelcastInstance clt = null;
    try {
      clt = HazelcastClient.newHazelcastClient(cfg);
    } catch (RuntimeException e) {
      log.error("HZ ERROR: {}", e.getMessage());
      bg(() -> reconnect(cfg));
      return null;
    }
    setListener(clt, () -> reconnect(cfg));
    log.info("Profile hz: Build HazelcastInstance bean");
    return clt;
  }

  /**
   * Backgroud execution.
   *
   * @param command the command
   */
  private void bg(Runnable command) {
    new Thread(command).start();
  }

  /**
   * method to reconnect to hazelcast.
   *
   * @param cfg ClientConfig
   * @return void
   */
  private void reconnect(ClientConfig cfg) {
    log.info("*** Trying reconnect to Hazelcast...");
    boolean connected = false;
    long t = 1000;
    HazelcastInstance clt = null;
    while (!connected) {
      try {
        clt = HazelcastClient.newHazelcastClient(cfg);
        connected = true;
        log.info("*** Reconnected again to hazelcast");
      } catch (RuntimeException e) {
        log.info("*** Error Reconnecting to Hazelcast: {}", e.getMessage());
      }
      try {
        Thread.sleep(t);
      } catch (InterruptedException e) {
        log.info("*** Error in thread: {}", e.getMessage());
        Thread.currentThread().interrupt();
      }
      t = (long) (t * 1.2);
      if (t > 60_000) {
        t = 60_000;
      }
    }
    setListener(clt, () -> reconnect(cfg));
  }

  /**
   * Set Listener to connection.
   *
   * @param clt the instance
   */
  private void setListener(HazelcastInstance clt, Runnable tryReconnect) {
    clt.getLifecycleService().addLifecycleListener(new LifecycleListener() {
      @Override
      public void stateChanged(LifecycleEvent event) {
        LifecycleState state = event.getState();
        if (state.equals(LifecycleState.CLIENT_DISCONNECTED)) {
          log.info("*********************** HZ DISCONNECTED ***********************");
        } else if (state.equals(LifecycleState.SHUTDOWN)) {
          log.info("*********************** HZ SHUTDOWN ***********************");
          bg(tryReconnect);
        } else if (state.equals(LifecycleState.CLIENT_CONNECTED)) {
          log.info("*********************** HZ CONNECTED ***********************");
        }
      }
    });
  }



}

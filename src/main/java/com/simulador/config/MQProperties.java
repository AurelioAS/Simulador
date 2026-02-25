/**
 * 
 */
package com.simulador.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "ibm.mq")
@Getter
@Setter
public class MQProperties {
    
    private String host;
    private int port;
    private String queueManager; // Spring mapea 'queue-manager' automáticamente a 'queueManager'
    private String channel;
    private String user;
    private String password;
    
    // Aquí es donde iría el array de colas que mencionamos antes
    private String[] queues; 
}

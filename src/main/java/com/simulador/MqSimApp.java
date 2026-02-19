package com.simulador;


import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@ComponentScan("com.simulador.*")
public class MqSimApp {
  public static void main(String[] args) {
    System.setProperty("com.ibm.mq.cfg.jmqi.libpath", "true"); // Acelera carga nativa
    System.setProperty("com.ibm.mq.cfg.ConnectOptions.ClientReconnectTimeout", "0");

    SpringApplication.run(MqSimApp.class, args);
  }
}
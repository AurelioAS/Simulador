/**
 * 
 */
package com.simulador.controller;


import lombok.Data;

@Data // Genera Getters, Setters y toString con Lombok
public class MqRequestDTO {
  private String  source;         // 'A' o 'B'
  private String  queue;          // El alias de la cola seleccionada
  private byte[]  correlationId;  // El ID en Hexadecimal
  private boolean copyCorrel;    // El checkbox de Copy
  private String  replyTo;        // Cola de respuesta
  private String  replyToQMgr;    // Manager de respuesta
  private int     iterations;        // Cantidad de mensajes a enviar
  private String  payload;        // El contenido del mensaje (JSON, XML, etc.)
  private int     threads = 1;         // Cantidad de hilos para la prueba (opcional, default 10)
}
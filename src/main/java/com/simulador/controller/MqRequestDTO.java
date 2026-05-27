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
  private boolean fireEnabled;   // Si se habilita el envío sin esperar respuesta (Fire and Forget)
  private String  fireKey;       // Si fireEnabled es true, el key para almacenar el mensaje en
                                 // Redis (ej: "fire:A")
  private String  fireTarget;    // Si fireEnabled es true, el target para enviar el mensaje (ej:
                                 // "A" o "B")
  private boolean fireCopyCorrel;   // Si se habilita el envío sin esperar respuesta (Fire and
                                    // Forget)
  private int     delay;            // Si se habilita el envío sin esperar respuesta (Fire and
                                    // Forget)
}
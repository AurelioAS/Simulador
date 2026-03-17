package com.simulador.utils;

import com.ibm.mq.MQException;
import com.ibm.mq.MQMessage;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.MQConstants;
import com.simulador.config.MQConnectionBundle;
import com.simulador.config.MQMultiConnectionConfig;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Utils {

  public static final Random random = new Random();

  protected ConcurrentMap<String, MQQueue> queues = new ConcurrentHashMap<>();

  protected MQMultiConnectionConfig multiConfigs;

  protected long lastTraffic = System.currentTimeMillis();

  @Getter
  protected boolean start = false;

  protected Map<String, MQConnectionBundle> connections;

  public static String identificarUso(MQQueue queue) throws Exception {
    int options = queue.getOpenOptions();

    // Comprobar si se abrió para PUT
    if ((options & MQConstants.MQOO_OUTPUT) != 0) {
      log.debug("La cola está abierta para: PUT (Output)");
      return "PUT";
    }

    // Comprobar si se abrió para GET
    // Nota: Puede ser por INPUT_AS_Q_DEF, INPUT_SHARED o INPUT_EXCLUSIVE
    if ((options & (MQConstants.MQOO_INPUT_AS_Q_DEF |
        MQConstants.MQOO_INPUT_SHARED |
        MQConstants.MQOO_INPUT_EXCLUSIVE)) != 0) {
      log.debug("La cola está abierta para: GET (Input)");
      return "GET";
    }

    // Comprobar si es para consultar (BROWSE)
    if ((options & MQConstants.MQOO_BROWSE) != 0) {
      log.trace("La cola está abierta para: BROWSE");
      return "BROWSE";
    }
    return null;
  }

  public boolean isNullOrEmpty(byte[] id) {
    // 1. Si es null físicamente, lo tratamos como vacío
    if (id == null)
      return true;

    // 2. Comprobamos si todos los bytes son 0
    for (byte b : id) {
      if (b != 0) {
        return false; // En cuanto encontremos un byte distinto de 0, no está vacío
      }
    }

    return true; // Si terminó el bucle, todo eran ceros
  }

  public static byte[] generateRandomId(int length) {
    byte[] id = new byte[length];
    random.nextBytes(id);

    // Opcional: Poner un "sello" al principio para saber que es de tu simulador
    // 'S' 'I' 'M' en ASCII
    id[0] = 0x53;
    id[1] = 0x49;
    id[2] = 0x4D;

    return id;
  }

  protected String bytesToHex(byte[] bytes) {
    StringBuilder sb = new StringBuilder();
    for (byte b : bytes)
      sb.append(String.format("%02X ", b));
    return sb.toString();
  }

  protected static Map<String, List<Object>> getMultiProperties(MQMessage message)
      throws Exception {

    Enumeration<String> names = message.getPropertyNames("%");
    LinkedHashMap<String, List<Object>> result = new LinkedHashMap<>();

    names.asIterator().forEachRemaining(name -> {
      try {
        // Properties are multivalued, that is, they can have more than one value. IBM is giving the
        // different values ​​in each call to getProperty. If there are "n" values ​​then when
        // reading the value "n+1" IBM returns the first value.
        List<Object> list = new ArrayList<>();
        Object first = message.getObjectProperty(name);

        // Only 6 types of values permited in serialization:
        if (first == null || first instanceof String || first instanceof Boolean
            || first instanceof Integer || first instanceof Long || first instanceof byte[]) {

          list.add(first);

          for (int i = 0; i < 100; i++) {
            Object obj = message.getObjectProperty(name);
            boolean sameObjectComparisonMethod = !name.startsWith("JMS");
            if (sameObjectComparisonMethod || obj == null) {
              if (obj == first) {
                break;
              }
            } else {
              if (obj.equals(first)) {
                break;
              }
            }
            list.add(obj);
          }

          result.put(name, list);

        } else {

          log.warn("Type not mqlauncher serializable, custom property ({}) with value({}) of "
              + "type ({}).", name, first, first.getClass().getName());

        }
      } catch (MQException e) {
        log.error("getMultiProperties: Error reading custom properties: {}", e.getMessage());
      }

    });

    return result;
  }

  protected static boolean getSystemBoolean(String name) {
    String prop = System.getProperty(name);
    if (prop == null) {
      return false;
    } else {
      return Boolean.parseBoolean(prop);
    }
  }

  public CompletableFuture<Map<String, Integer>> getQueuesDepthReport() {
    MQQueueManager qMan = multiConfigs.qMan;
    return CompletableFuture.supplyAsync(() -> {
      if (!this.start) {
        return Map.of(); // Retornamos un mapa vacío para evitar hacer llamadas innecesarias
      } else {
        this.start = false;
      }
      Map<String, Integer> report = new ConcurrentHashMap<>();
      // Usamos parallelStream para disparar las peticiones al socket
      // Aunque el socket es serial, ganamos tiempo en el procesamiento de la librería
      queues.entrySet().parallelStream().forEach(entry -> {
        String qName = entry.getKey();
        int opts = MQConstants.MQOO_INQUIRE | MQConstants.MQOO_FAIL_IF_QUIESCING;
        MQQueue queue = null;
        try {
          queue = qMan.accessQueue(qName, opts);
          report.put(qName, queue.getCurrentDepth());
        } catch (MQException e) {
          log.error("No se pudo obtener depth de {}: RC={}", qName, e.reasonCode);
          report.put(qName, -1); // Marcamos error con -1
        } finally {
          // No cerramos la conexión porque la vamos a reutilizar, pero sí liberamos el handle
          try {
            if (queue.isOpen())
              queue.close();
          } catch (MQException e) {
            log.error("Error cerrando cola {}: RC={}", qName, e.reasonCode);
          }
        }
      });
      return report;
    });
  }

  // Llama a esto cada vez que tu Flux reciba un mensaje
  public void notifyTraffic() {
    this.lastTraffic = System.currentTimeMillis();
    this.start = true;
  }

}

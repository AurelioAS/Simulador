package com.simulador.utils;

import com.ibm.mq.MQQueue;
import com.ibm.mq.constants.MQConstants;
import java.util.Random;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Utils {

  public static final Random random = new Random();


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

  public static byte[] generateRandomId() {
    byte[] id = new byte[24];
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
}

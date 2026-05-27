/**
 * 
 */
package com.simulador.config;

import com.ibm.mq.MQException;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.MQConstants;
import java.util.Hashtable;
import lombok.Getter;


@Getter
public class MQConnectionBundle {
  private MQQueueManager            qm;
  private MQQueue                   queue;
  private Hashtable<String, Object> props;

  public MQConnectionBundle(MQQueueManager qm, MQQueue queue, Object props) {
    this.qm = qm;
    this.queue = queue;
    this.props = (Hashtable<String, Object>) props;
  }

  /**
   * 
   */
  public MQConnectionBundle() {}

  public MQConnectionBundle cloneBundle(MQConnectionBundle original) throws MQException {
    // 1. Extraer datos del manager original
    String qmName = original.getQm().getName();

    // 2. Extraer propiedades (esto requiere que las hayas guardado al crear el original)
    // Si no las tienes, tendrás que usar las que tienes en tu config YAML
    this.props = original.props;

    // 3. Crear nuevo Manager (Nueva conexión TCP)
    MQQueueManager newQm = new MQQueueManager(qmName, props);

    // 4. Abrir la misma cola con las mismas opciones
    String qName = original.getQueue().getName();
    int openOptions = MQConstants.MQOO_OUTPUT | MQConstants.MQOO_INPUT_AS_Q_DEF
        | MQConstants.MQOO_INQUIRE | MQConstants.MQOO_FAIL_IF_QUIESCING
        | MQConstants.MQPMO_SET_ALL_CONTEXT;
    MQQueue newQueue = newQm.accessQueue(qName, openOptions);

    return new MQConnectionBundle(newQm, newQueue, props);
  }
}

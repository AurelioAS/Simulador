/**
 * 
 */
package com.simulador.controller;

import com.simulador.config.SimulatorProperties;
import com.simulador.service.MqSimulatorService;
import com.simulador.service.RoundRobinExecutorPool;
import com.simulador.utils.Utils;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping("/gui")
public class WebGuiController extends Utils {

  private final MqSimulatorService  mqService;
  private final SimulatorProperties props;
  private RoundRobinExecutorPool    pool = new RoundRobinExecutorPool("SEND", 25);

  public WebGuiController(MqSimulatorService mqService, SimulatorProperties props) {
    this.mqService = mqService;
    this.props = props;
  }

  @GetMapping
  public String index(Model model) {
    model.addAttribute("queues", props.getQueues());
    model.addAttribute("payloads", props.getPayloads());
    return "simulator-gui";
  }

  @PostMapping("/send")
  @ResponseBody
  public ResponseEntity<String> handleSend(@ModelAttribute MqRequestDTO request) {

    byte[] originalCorrelation = request.getCorrelationId(); // Guardamos el valor original para usarlo en cada
                                                // iteración

    final byte[][] corr = new byte[1][];
    final byte[][] mess = new byte[1][];

    for (int i = 0; i < request.getIterations(); i++) {
      try {
        pool.execute(() -> {
          corr[0] = originalCorrelation;        // correlationId = originalCorrelation; //
                                                // Reiniciamos
                                                // el valor de correlationId para cada iteración
          if (request.getCorrelationId() == null || request.getCorrelationId().length == 0) {
            corr[0] = generateRandomId();          // correlationId = utils.generateRandomId();
          }
          if (request.isCopyCorrel()) {
            mess[0] = request.getCorrelationId().clone();          // messageId =
                                                                   // correlationId.clone();
          }
          try {
            mqService.send(request.getQueue(), request.getPayload(), corr[0], mess[0],
                request.isCopyCorrel(), request.getReplyTo(), request.getReplyToQMgr(),
                false, java.util.Optional.empty());
          } catch (Exception e) {
            e.printStackTrace();
          }
        });

      } catch (Exception e) {
        return ResponseEntity.status(500).body("Error en el envío: " + e.getMessage());
      }
    }
    String msgSuccess = (request.getIterations() > 1)
        ? String.format("Se han enviado %d mensajes a %s", request.getIterations(),
            request.getQueue())
        : "Mensaje enviado con éxito a " + request.getQueue();

    return ResponseEntity.ok(msgSuccess);
  }

  private byte[] hexStringToByteArray(String s) {
    int len = s.length();
    byte[] data = new byte[len / 2];
    for (int i = 0; i < len; i += 2) {
      data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
          + Character.digit(s.charAt(i + 1), 16));
    }
    return data;
  }


}

/**
 * 
 */
package com.simulador.controller;

import com.simulador.config.SimulatorProperties;
import com.simulador.service.MqSimulatorService;
import com.simulador.service.RoundRobinExecutorPool;
import com.simulador.utils.Utils;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

@Controller
@RequestMapping("/gui")
public class WebGuiController extends Utils {

  private final MqSimulatorService  mqService;
  private final SimulatorProperties props;
  private RoundRobinExecutorPool    pool = new RoundRobinExecutorPool("SEND", 100);

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
  public String sendMessage(@RequestParam String queue,
      @RequestParam String payload,
      @RequestParam(required = false) byte[] correlationId,
      @RequestParam(required = false) boolean copyCorrel,
      @RequestParam(defaultValue = "1") int iterations,
      @RequestParam(required = false) String replyTo,
      @RequestParam(required = false) String replyToQMgr,
      RedirectAttributes ra) {

    byte[] originalCorrelation = correlationId; // Guardamos el valor original para usarlo en cada
                                                // iteración

    final byte[][] corr = new byte[1][];
    final byte[][] mess = new byte[1][];

    for (int i = 0; i < iterations; i++) {
      try {
        pool.execute(() -> {
          corr[0] = originalCorrelation;        // correlationId = originalCorrelation; //
                                                // Reiniciamos
                                                // el valor de correlationId para cada iteración
          if (correlationId == null || correlationId.length == 0) {
            corr[0] = generateRandomId();          // correlationId = utils.generateRandomId();
          }
          if (copyCorrel) {
            mess[0] = correlationId.clone();          // messageId = correlationId.clone();
          }
          try {
            mqService.send(queue, payload, corr[0], mess[0], copyCorrel, replyTo, replyToQMgr,
                false, java.util.Optional.empty());
          } catch (Exception e) {
            e.printStackTrace();
          }
        });

      } catch (Exception e) {
        ra.addFlashAttribute("error", "Error en el envío: " + e.getMessage());
      }
    }
    String msgSuccess = (iterations > 1)
        ? String.format("Se han enviado %d mensajes a %s", iterations, queue)
        : "Mensaje enviado con éxito a " + queue;

    ra.addFlashAttribute("success", msgSuccess);
    return "redirect:/gui";
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

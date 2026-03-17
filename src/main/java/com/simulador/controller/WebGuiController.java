package com.simulador.controller;


import com.ibm.mq.MQMessage;
import com.simulador.config.SimulatorProperties;
import com.simulador.service.MqSimulatorService;
import com.simulador.utils.Utils;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

@Slf4j
@Controller
@RequestMapping("/gui")
public class WebGuiController extends Utils {

  private final MqSimulatorService  mqService;
  private final SimulatorProperties props;

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

  // 1. CLASE PARA GUARDAR EL RESULTADO (Compatible con cualquier Java)
  public static class SampleResult {
    private final int     iteration;
    private final String  messageId;
    private final boolean success;
    private final String  errorDetail;

    public SampleResult(int iteration, String messageId, boolean success, String errorDetail) {
      this.iteration = iteration;
      this.messageId = messageId;
      this.success = success;
      this.errorDetail = errorDetail;
    }

    // Getters necesarios para que Spring pueda convertirlo a JSON
    public int getIteration() {
      return iteration;
    }

    public String getMessageId() {
      return messageId;
    }

    public boolean isSuccess() {
      return success;
    }

    public String getErrorDetail() {
      return errorDetail;
    }
  }

  // 2. MÉTODO AUXILIAR PARA HEXADECIMAL
  protected String bytesToHex(byte[] bytes) {
    if (bytes == null)
      return "AUTO";
    StringBuilder sb = new StringBuilder();
    for (byte b : bytes) {
      sb.append(String.format("%02X", b));
    }
    return sb.toString();
  }

  // 3. EL CONTROLADOR ACTUALIZADO
  @PostMapping("/send")
  @ResponseBody
  public ResponseEntity<java.util.Map<String, Object>> handleSend(
      @ModelAttribute MqRequestDTO request) {

    int iterations = request.getIterations();
    int threads = (request.getThreads() > 0) ? request.getThreads() : 1;
    log.info("Iniciando simulación de envío: {} mensajes en {} hilos.", iterations, threads);
    @SuppressWarnings("deprecation")
    ExecutorService executor = Executors.newFixedThreadPool(threads,
        runnable -> {
          Thread t = new Thread(runnable);
          t.setDaemon(true); // Para que no bloqueen el cierre de la app
          t.setName("SimuSender-" + t.getId());
          return t;
        });
    CountDownLatch latch = new CountDownLatch(iterations);

    // Cola concurrente para los resultados
    ConcurrentLinkedQueue<SampleResult> detailedResults = new ConcurrentLinkedQueue<>();

    AtomicInteger successCount = new AtomicInteger(0);
    AtomicInteger errorCount = new AtomicInteger(0);

    byte[] originalCorrelation = request.getCorrelationId();
    long startTime = System.currentTimeMillis();

    for (int i = 0; i < iterations; i++) {
      final int currentIteration = i + 1;

      executor.execute(() -> {
        try {
          byte[] threadCorr = originalCorrelation;
          byte[] threadMess = null;

          if (threadCorr == null || threadCorr.length == 0) {
            threadCorr = generateRandomId(24);
          }
          if (request.isCopyCorrel()) {
            threadMess = threadCorr.clone();
          }
          MQMessage msg = new MQMessage();
          msg.setObjectProperty("token", UUID.randomUUID().toString());
          // Envío real a MQ
          mqService.send(request.getQueue(), request.getPayload(), threadCorr, threadMess,
              request.isCopyCorrel(), request.getReplyTo(), request.getReplyToQMgr(),
              false, java.util.Optional.empty());

          successCount.incrementAndGet();
          detailedResults
              .add(new SampleResult(currentIteration, bytesToHex(threadCorr), true, null));

        } catch (Exception e) {
          errorCount.incrementAndGet();
          detailedResults.add(new SampleResult(currentIteration, null, false, e.getMessage()));
        } finally {
          latch.countDown();
        }
      });
    }

    try {
      latch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      java.util.Map<String, Object> err = new java.util.HashMap<>();
      err.put("error", "Simulación interrumpida.");
      e.printStackTrace();
      return ResponseEntity.status(500).body(err);
    } finally {
      executor.shutdown();
    }

    long totalTime = System.currentTimeMillis() - startTime;

    // 4. PREPARAMOS EL JSON DE RESPUESTA
    java.util.Map<String, Object> responseBody = new java.util.HashMap<>();
    responseBody.put("timeMs", totalTime);
    responseBody.put("successes", successCount.get());
    responseBody.put("errors", errorCount.get());
    responseBody.put("queue", request.getQueue());

    // Filtramos solo los errores para no saturar la red si envías 1 millón de mensajes
    java.util.List<SampleResult> failedItems = detailedResults.stream()
        .filter(r -> !r.isSuccess())
        .collect(java.util.stream.Collectors.toList());

    responseBody.put("failedDetails", failedItems);
    log.info("Simulación de envío completada en {} ms: {} éxitos, {} errores.", totalTime,
        successCount.get(), errorCount.get());
    return ResponseEntity.ok(responseBody);
  }
}

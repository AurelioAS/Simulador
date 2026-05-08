package com.simulador.controller;

import com.simulador.config.MQConnectionBundle;
import com.simulador.config.SimulatorProperties;
import com.simulador.service.MqSimulatorService;
import com.simulador.service.SendService;
import com.simulador.utils.MessagesMgr;
import com.simulador.utils.Utils;
import jakarta.servlet.http.HttpSession;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.DependsOn;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter.SseEventBuilder;

@Slf4j
@Controller
@DependsOn("mqConnections")
@RequestMapping("/gui")

public class WebGuiController extends Utils {

  private final MqSimulatorService        mqService;
  private final SimulatorProperties       props;
  private Map<String, MQConnectionBundle> myconnections;
  private List<MQConnectionBundle>        clones = new ArrayList<>();

  // Importante: No dejar estos como variables de clase si hay varios usuarios,
  // pero los mantenemos aquí para seguir tu lógica actual.
  private byte[][]   correls = new byte[1][1];
  private SendService sendService;
  private boolean     error;
  private MessagesMgr messages;

  public WebGuiController(MqSimulatorService mqService, SimulatorProperties props,
      Map<String, MQConnectionBundle> connections, SendService sendService, MessagesMgr messages) {
    this.mqService = mqService;
    this.props = props;
    this.myconnections = connections;
    this.sendService = sendService;
    this.messages = messages;
  }

  @GetMapping("/ping")
  @ResponseBody
  public ResponseEntity<Map<String, Object>> ping(HttpSession session) {
    Map<String, Object> status = new HashMap<>();
    status.put("status", mqService.isRunning() ? "ok" : "ko");
    status.put("sessionId", session.getId());
    return ResponseEntity.ok(status);
  }

  @PostMapping("/cambiar-cola")
  public ResponseEntity<String> cambiarCola(@RequestBody Map<String, String> request) {
    log.info("Recibida solicitud de cambio de cola: {}", request.get("nuevaCola"));
    SimulatorProperties.QueueConfig qConfig = props.getQueues().get(request.get("nuevaCola"));
    MQConnectionBundle bundle = myconnections.get(qConfig.getName());

    clones.clear(); // Limpiamos los clones para forzar nuevas conexiones con la nueva cola
    while (clones.size() < Integer.parseInt(request.get("threads"))) {
      try {
        clones.add(bundle.cloneBundle(bundle));
        String txt =
            "Thread " + (clones.size()) + ": Conexión a la Cola " + qConfig.getName()
                + " establecida.";
        log.info(txt);
      } catch (Exception e) {
        log.error("Error clonando conexión: {}", e.getMessage());
        return ResponseEntity.status(500).body("Error al cambiar de cola: " + e.getMessage());
      }
    }
    // SimulatorProperties.QueueConfig qConfig = props.getQueues().get(request.get("nuevaCola"));
    return ResponseEntity.ok("Cola actualizada a: " + request.get("nuevaCola"));
  }

  @GetMapping("/diagrama")
  public String diagrama(Model model) {
    return "diagrama";
  }

  @GetMapping("/flows")
  public String flows(Model model) {
    return "flowsEditor";
  }

  @GetMapping
  public String index(HttpSession session, Model model) {
    List<Map.Entry<String, Supplier<String>>> sortedMessages = messages.getTable().entrySet()
        .stream()
        .sorted(Map.Entry.comparingByKey()) // Orden alfabético por la clave
        .collect(Collectors.toList());
    model.addAttribute("queues", props.getQueues());
    model.addAttribute("payloads", props.getPayloads());
    model.addAttribute("fire", props.isFire());
    model.addAttribute("messages", sortedMessages);
    session.setAttribute("SPRING_SECURITY_LAST_EXCEPTION", null);
    // Podemos pasar un ID de sesión al modelo para depuración visual en el HTML
    model.addAttribute("sessionId", session.getId());
    return "simulator-gui";
  }

  // --- EL MÉTODO MÁGICO PARA STREAMING ---
  @GetMapping(value = "/send", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
  public SseEmitter handleSend(@ModelAttribute MqRequestDTO request,
      @RequestParam(defaultValue = "true") boolean showModal) {

    SseEmitter emitter = new SseEmitter(0L);
    // 1. Creamos el emisor (Timeout 0 = infinito para procesos largos)
    AtomicBoolean isCompleted = new AtomicBoolean(false);
    String emitterKey = request.getSource(); // Clave para el mapa (ej: "A" o "B")

    // 2. Registramos los callbacks de limpieza REAL.
    // Solo se borra del mapa si la conexión se cierra de verdad.
    registerEmitterCallbacks(emitter, emitterKey, isCompleted);

    mqService.registrarEmitter(emitterKey, emitter);
    sendService.registrarEmitter(emitterKey, emitter);
    // 2. Ejecutamos TODO en un hilo separado para liberar a Tomcat
    CompletableFuture.runAsync(() -> {
      ExecutorService executor = null;
      SseEventBuilder eventBuilder =
          SseEmitter.event().id("CONFIG").data("showModal=" + showModal);
      try {
        emitter.send(eventBuilder);
      } catch (IOException e) {
        e.printStackTrace();
      }

      try {
        int iterations = request.getIterations();
        int threads = Math.max(request.getThreads(), 1);

        emitter.send("Iniciando simulación: " + iterations + " mensajes en " + threads + " hilos.");

        // Lógica de Correlativos
        if (request.getSource().equals("A")) {
          correls = new byte[iterations][24];
        }

        // Preparación de Conexiones (Clones)
        eventBuilder.id("connection").data("Estableciendo " + threads + " conexiones MQ...");
        emitter.send(eventBuilder);
        SimulatorProperties.QueueConfig qConfig = props.getQueues().get(request.getQueue());
        MQConnectionBundle bundle = myconnections.get(qConfig.getName());

        int iters = threads > clones.size() ? threads - clones.size() : 0;
        log.info("*** Threads a crear: {} ***", iters);
        // for (int i = 0; i < iters; i++) {
        // clones.add(bundle.cloneBundle(bundle));
        // String txt =
        // "Clon " + (i + 1) + ": Conexión a la Cola " + qConfig.getName() + " establecida.";
        // log.info(txt);
        // emitter.send(txt);
        // }
        // Configuración del Pool de ejecución
        executor = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(iterations);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);
        long startTime = System.currentTimeMillis();

        eventBuilder.id("startSend").data("Enviando mensajes a la cola: " + request.getQueue());
        emitter.send(eventBuilder);
        sendService.setStart(true);
        sendService.setContador(iterations);
        sendService.setActual(0);
        sendService.setStartTime(startTime);
        for (int i = 0; i < iterations; i++) {
          final int currentIdx = i;
          if (clones.size() < threads) {
            clones.add(bundle.cloneBundle(bundle));
            String txt =
                "Thread " + (clones.size()) + ": Conexión a la Cola " + qConfig.getName()
                    + " establecida.";
            log.info(txt);
            eventBuilder.id("Sending").data(txt);
            emitter.send(eventBuilder);
          }
          executor.execute(() -> {
            try {
              byte[] threadCorr = request.getCorrelationId();
              byte[] threadMess = null;

              // Lógica de ID que ya tenías
              if (threadCorr == null || threadCorr.length == 0) {
                threadCorr =
                    request.getSource().equals("B") ? correls[currentIdx] : generateRandomId(24);
              }
              if (request.isCopyCorrel())
                threadMess = threadCorr.clone();
              if (request.getSource().equals("A"))
                correls[currentIdx] = threadCorr;

              // Envío Real
              sendService.send(request.getQueue(), request.getPayload(), threadCorr, threadMess,
                  request.isCopyCorrel(), request.getReplyTo(), request.getReplyToQMgr(),
                  false, java.util.Optional.empty(), request.getSource(),
                  clones.get(currentIdx % threads), emitter);
              if (request.isFireEnabled()) {
                sendService.send(request.getFireTarget(), request.getFireKey(), threadCorr,
                    threadMess,
                    request.isFireCopyCorrel(), request.getReplyTo(), request.getReplyToQMgr(),
                    false, java.util.Optional.empty(), request.getSource(),
                    null, emitter);
              }
              successCount.incrementAndGet();
            } catch (Exception e) {
              errorCount.incrementAndGet();
              try {
                eventBuilder.id("error")
                    .data("Error en ítem " + (currentIdx + 1) + ": " + e.getMessage());
                emitter.send(eventBuilder);
                this.error = true;
              } catch (Exception ignored) {
              }
            } finally {
              latch.countDown();
            }
          });
          if (request.getDelay() > 0) {
          TimeUnit.MILLISECONDS.sleep(request.getDelay()); // Si se quiere un delay entre envíos
          }
        }

        latch.await(); // Esperamos a que terminen todos los hilos
        long totalTime = System.currentTimeMillis() - startTime;

        // MENSAJE FINAL (El JS busca la palabra "Completado")
        if (!error) {
          eventBuilder.id("Completado").comment("Todo completado.")
              .data(successCount.get() + " mensajes enviados en "
                  + totalTime + "ms.");
          emitter.send(eventBuilder);
        }
        // emitter.complete();

      } catch (Exception e) {
        log.error("Error en el stream");
        try {
          eventBuilder.id("criticalError").data("Error Crítico: " + e.getMessage());
          emitter.send(eventBuilder);
        } catch (Exception ignored) {
        }
        emitter.completeWithError(e);
      } finally {
        if (executor != null)
          executor.shutdown();
        // Limpieza de conexiones
        // cerrarConexiones(clones);
      }
    });

    return emitter; // Se devuelve el canal de texto al HTML al instante
  }

  /**
   * @param emitter
   * @param emitterKey
   * @param isCompleted
   */
  private void registerEmitterCallbacks(SseEmitter emitter, String emitterKey,
      AtomicBoolean isCompleted) {
    emitter.onCompletion(() -> {
      log.info("Conexión finalizada para: {}", emitterKey);
      isCompleted.set(true);
      mqService.removerEmitter(emitterKey);
      sendService.removerEmitter(emitterKey);
    });

    emitter.onTimeout(() -> {
      log.warn("Timeout en conexión: {}", emitterKey);
      isCompleted.set(true);
      mqService.removerEmitter(emitterKey);
      sendService.removerEmitter(emitterKey);
    });

    emitter.onError((ex) -> {
      log.error("Error en conexión SSE: {}", emitterKey);
      isCompleted.set(true);
      mqService.removerEmitter(emitterKey);
      sendService.removerEmitter(emitterKey);
    });
  }

  private void cerrarConexiones(MQConnectionBundle[] clones) {
    if (clones == null)
      return;
    for (MQConnectionBundle c : clones) {
      try {
        if (c != null) {
          if (c.getQueue() != null)
            c.getQueue().close();
          if (c.getQm() != null) {
            c.getQm().disconnect();
            c.getQm().close();
          }
        }
      } catch (Exception e) {
        log.warn("Error cerrando clon: {}", e.getMessage());
      }
    }
  }

  @Scheduled(fixedDelay = 30000) // Cada 1/2 minuto
  public void checkConnections() {
    if (myconnections.size() == 0) {
      log.warn("No hay conexiones registradas para monitorear.");
      clones.clear();
      return;
    }
  }
}

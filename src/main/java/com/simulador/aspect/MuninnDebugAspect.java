package com.simulador.aspect;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Aspect
@Component
@ConditionalOnProperty(name = "simu-mq.debug", havingValue = "true")
public class MuninnDebugAspect {

  private static final Logger log = LoggerFactory.getLogger(MuninnDebugAspect.class);

  // Intercepta clases enteras (@within) O métodos sueltos (@annotation)
  @Around("@within(com.simulador.aspect.LogFullDetails) || @annotation(com.simulador.aspect.LogFullDetails)")
  public Object handleLog(ProceedingJoinPoint joinPoint) throws Throwable {

    MethodSignature signature = (MethodSignature) joinPoint.getSignature();
    String methodName = signature.getName();

    // 1. PRIORIDAD: Buscar la anotación en el método
    LogFullDetails logDetails = signature.getMethod().getAnnotation(LogFullDetails.class);

    // 2. FALLBACK: Si el método no la tiene, buscarla en la clase
    if (logDetails == null) {
      logDetails = joinPoint.getTarget().getClass().getAnnotation(LogFullDetails.class);
    }

    long start = System.currentTimeMillis();

    // --- Resto de la lógica igual ---
    if (logDetails.logArguments()) {
      Object[] args = joinPoint.getArgs();
      String[] params = signature.getParameterNames();
      log.info(">>> [ENTER] {}: args {}", methodName, formatArgs(params, args));
    }

    try {
      Object result = joinPoint.proceed();

      long duration = System.currentTimeMillis() - start;
      String timeMsg = logDetails.logExecutionTime() ? " in " + duration + "ms" : "";

      if (logDetails.logResult()) {
        log.info(">>> [EXIT] {}{}: result [{}]", methodName, timeMsg, result);
      } else {
        log.info(">>> [EXIT] {}{}", methodName, timeMsg);
      }

      return result;

    } catch (Throwable e) {
      log.error(">>> [ERROR] {} failed: {}", methodName, e.getMessage());
      throw e;
    }
    }

  private String formatArgs(String[] names, Object[] values) {
    if (names == null || values == null)
      return "[]";
    StringBuilder sb = new StringBuilder("[");
    for (int i = 0; i < values.length; i++) {
      sb.append(names[i]).append("=").append(values[i]);
      if (i < values.length - 1)
        sb.append(", ");
    }
    return sb.append("]").toString();
    }
}
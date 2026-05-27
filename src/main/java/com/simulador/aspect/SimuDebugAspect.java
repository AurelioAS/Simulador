package com.simulador.aspect;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Aspect
@Component
@ConditionalOnProperty(name = "simu-mq.debug", havingValue = "true")
public class SimuDebugAspect {

  private static final Logger log = LoggerFactory.getLogger(SimuDebugAspect.class);

  @Value("${simu-mq.debugAll:false}") // Lee la propiedad, por defecto false
  private boolean isDebugEnabled;

  // Intercepta clases enteras (@within) O métodos sueltos (@annotation)
  @Around("(@within(com.simulador.aspect.LogFullDetails) || @annotation(com.simulador.aspect.LogFullDetails)) && !within(com.simulador.aspect..*)")
  public Object handleLog(ProceedingJoinPoint joinPoint) throws Throwable {

    MethodSignature signature = (MethodSignature) joinPoint.getSignature();
    String clazz = joinPoint.getTarget().getClass().getSimpleName();
    String[] paramNames = signature.getParameterNames();
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
      log.info("### [ENTER] '{}': args {}", clazz + "." + methodName, formatArgs(params, args));
    }
    try {
      Object result = joinPoint.proceed();
      long duration = System.currentTimeMillis() - start;
      String timeMsg = logDetails.logExecutionTime() ? " in " + duration + "ms" : "";
      if (logDetails.logResult()) {
        log.info("### [EXIT] '{}'{}: result [{}]", clazz + "." + methodName, timeMsg, result);
      } else {
        log.info("### [EXIT] '{}'{}", clazz + "." + methodName, timeMsg);
      }
      return result;
    } catch (Throwable e) {
      log.error("### [ERROR] '{}' failed: {}", methodName, e.getMessage());
      throw e;
    }
  }

  // Cambia temporalmente el Pointcut para que escuche TODO en un paquete
  // @Around("execution(* com.simulador..*(..))")
  // @Around("execution(* com.simulador..*(*))")
  public Object testLog(ProceedingJoinPoint joinPoint) throws Throwable {
    if (!isDebugEnabled) {
      return joinPoint.proceed();
    }
    MethodSignature signature = (MethodSignature) joinPoint.getSignature();
    String[] paramNames = signature.getParameterNames();
    String clazz = joinPoint.getTarget().getClass().getSimpleName();
    String methodName = signature.getName();
    // 1. PRIORIDAD: Buscar la anotación en el método
    LogFullDetails logDetails = signature.getMethod().getAnnotation(LogFullDetails.class);

    // 2. FALLBACK: Si el método no la tiene, buscarla en la clase
    if (logDetails == null) {
      logDetails = joinPoint.getTarget().getClass().getAnnotation(LogFullDetails.class);
    }
    // --- Resto de la lógica igual ---
      Object[] args = joinPoint.getArgs();
      String[] params = signature.getParameterNames();
      log.info("### [ENTER-2] '{}': args {}", clazz + "." + methodName, formatArgs(params, args));

    long start = System.currentTimeMillis();
    Object result = joinPoint.proceed();
    long duration = System.currentTimeMillis() - start;
    if (logDetails == null) {
      logDetails = new LogFullDetails() {
        @Override
        public Class<? extends java.lang.annotation.Annotation> annotationType() {
          return LogFullDetails.class;
        }

        @Override
        public boolean logArguments() {
          return true;
        }

        @Override
        public boolean logExecutionTime() {
          return true;
        }

        @Override
        public boolean logResult() {
          return true;
        }
      };
    }
    String timeMsg = logDetails.logExecutionTime() ? " in " + duration + "ms" : "";
    if (logDetails.logResult()) {
      log.info("### [EXIT-2] '{}'{}: result [{}]", clazz + "." + methodName, timeMsg, result);
    } else {
      log.info("### [EXIT-2] '{}'{}", clazz + "." + methodName, timeMsg);
    }
    return result;
  }

  @LogFullDetails
  public void generateReport() {
    System.out.println("Generating XLS report !!");
  }

  private String formatArgs(String[] names, Object[] values) {
    if (names == null || values == null)
      return "[]";
    StringBuilder sb = new StringBuilder("[");
    for (int i = 0; i < values.length; i++) {
      sb.append(names[i]).append("=");
      sb.append(values[i]);
      if (i < values.length - 1)
        sb.append(", ");
    }
    return sb.append("]").toString();
  }
}

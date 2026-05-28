/**
 * 
 */
package com.simulador.aspect;


import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface LogFullDetails {
    boolean logArguments() default true;
    boolean logExecutionTime() default true;
    boolean logResult() default false; // Por defecto no logueamos el retorno (privacidad)
}
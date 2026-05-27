/**
 * 
 */
package com.simulador.controller;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class WebConfig implements WebMvcConfigurer {

  @Override
  public void addResourceHandlers(ResourceHandlerRegistry registry) {
    // Esto dice: "Cualquier ruta que empiece por /javascript/..."
    // "...búscala en la carpeta /static/javascript/ de mis recursos"
    registry.addResourceHandler("/javascript/**")
        .addResourceLocations("classpath:/static/javascript/");
  }
}

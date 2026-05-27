/**
 * 
 */
package com.simulador.controller;

import com.simulador.config.SimulatorProperties;
import com.simulador.service.ConfigManagerService;
import java.util.Arrays;
import java.util.List;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;
import org.yaml.snakeyaml.Yaml;

@Controller
@RequestMapping("/config-panel")
public class ConfigPanelController {

  private final ConfigManagerService configService;
  private final SimulatorProperties  props;

  private final Environment env;

  public ConfigPanelController(ConfigManagerService configService, SimulatorProperties props,
      Environment env) {
    this.configService = configService;
    this.props = props;
    this.env = env;
  }

  @GetMapping
  public String showConfig(Model model) throws Exception {
    List<String> availableProfiles = configService.getAllAvailableProfiles();

    // 3. Ahora ya podemos usar 'env' para saber qué perfil está activo en memoria
    String activeProfile = Arrays.stream(env.getActiveProfiles())
        .findFirst()
        .orElse("default");

    model.addAttribute("availableProfiles", availableProfiles);
    model.addAttribute("activeProfile", activeProfile);

    Yaml yaml = new Yaml();
    model.addAttribute("queuesYaml", yaml.dumpAsMap(props.getQueues()));

    return "config-panel";
  }

  @PostMapping("/update-profile")
  public String updateProfile(@RequestParam String newProfile, RedirectAttributes ra) {
    try {
      configService.updateActiveProfile(newProfile);
      ra.addFlashAttribute("success",
          "Perfil cambiado a " + newProfile + ". Reinicia la app para aplicar.");
      return "redirect:/config-panel";
    } catch (Exception e) {
      ra.addFlashAttribute("error", "Error al escribir: " + e.getMessage());
      return "redirect:/config-panel";
    }
  }
}
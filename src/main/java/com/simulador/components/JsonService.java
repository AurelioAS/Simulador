package com.simulador.components;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * Provides functionality to read and write JSON, from basic POJOs.
 */
@Service
@Slf4j
public class JsonService {

  /** The object mapper. */
  private ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();

  /**
   * It does not allow the serialization of dates as timestamps, only textual format.
   */
  public JsonService() {
    log.info("Initializing JsonService with ObjectMapper: " + objectMapper);
    objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
  }

  /**
   * Method to serialize a java object to a json string.
   *
   * @param javaBean java object to serialize
   * @return java object serialized as a string
   * @throws Exception
   */
  public String encode(Object javaBean) throws Exception {
    try {
      return objectMapper.writeValueAsString(javaBean);
    } catch (JsonProcessingException e) {
      throw new Exception("Error serializing a java object into a json string", e);
    }
  }

  /**
   * Method to deserialize a json string to a java object.
   *
   * @param <T> the generic type
   * @param jsonCode json string with java object
   * @param valueType java object class
   * @return deserialized java object
   * @throws Exception
   */
  public <T> T decode(String jsonCode, Class<T> valueType) throws Exception {
    try {
      return objectMapper.readValue(jsonCode, valueType);
    } catch (JsonProcessingException e) {
      throw new Exception("Error serializing a java object into a json string", e);
    }
  }
}

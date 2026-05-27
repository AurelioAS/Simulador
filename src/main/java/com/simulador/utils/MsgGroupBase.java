package com.simulador.utils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.stream.Stream;

public abstract class MsgGroupBase {

  public MsgGroupBase(MessagesMgr messages) {
    Stream.of(getClass().getMethods())
        .filter(m -> m.getName().startsWith("msg_"))
        .forEach(m -> {
          messages.put(m.getName().substring(4), () -> invoke(m));
        });
  }

  private String invoke(Method m) {
    try {
      return (String) m.invoke(this);
    } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

}

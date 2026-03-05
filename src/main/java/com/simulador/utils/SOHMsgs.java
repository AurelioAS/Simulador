package com.simulador.utils;

import java.nio.charset.Charset;

public class SOHMsgs {

  public static final String[] MSGS = messages();

  private static String[] messages() {
      try {
          return messagesAux();
      } catch (Exception e) {
          e.printStackTrace();
          return null;
      }
  }
  
  private static String[] messagesAux() throws Exception {
      
      String[] res = new String[4];
      
      res[0] = DumpRead.readMsgS("soh-rr-1", Charset.forName("CP284"));
      res[1] = DumpRead.readMsgS("soh-rr-2", Charset.forName("CP284"));
      res[2] = DumpRead.readMsgS("soh-rr-3");
      res[3] = DumpRead.readMsgS("soh-oneway-pseudoParser", Charset.forName("CP284"));

      return res;
  }

}

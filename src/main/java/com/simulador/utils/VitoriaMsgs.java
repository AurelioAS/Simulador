package com.simulador.utils;

public class VitoriaMsgs extends MsgGroupBase {

  public VitoriaMsgs(MessagesMgr messages) {
    super(messages);
  }

  public String msg_vito1_zos() {
    return "MANGO     01371SANTANDER      00047400000075FERNANDO MARTIN000055";
  }

  public String msg_vito1_gra() {
    return "MANGO     01371SANTANDER      00047400000075FERNANDO MARTIN000053";
  }

  public String msg_vito1_resp() {
    return "040000000075000055xxxx000000000022002100";
  }

  public String msg_vito2_zos() {
    return "XXXX4";
  }

  public String msg_vito2_gra() {
    return "XXXX5";
  }

  public String msg_vito3_req() {
    return "XXXX6";
  }

}

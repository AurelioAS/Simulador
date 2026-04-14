package com.simulador.utils;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class MessagesMgr {

  private static final Charset CS_284 = Charset.forName("Cp284");

  private Path filepath;

  private Map<String, Supplier<String>> table = new HashMap<>();

  @Cacheable(value = "msgs", key = "#idMsg")
  public String createStrPayload(String idMsg) {
    log.debug("Creating payload for idMsg: {}", idMsg);
    if (idMsg.equals("msg1")) {
      return msg1();
    } else if (idMsg.equals("msg2")) {
      return msg2();
    } else if (idMsg.equals("msg3")) {
      return msg3();
    } else if (idMsg.equals("msg4")) {
      return msg4();
    } else if (idMsg.equals("msg5")) {
      return msg5();
    } else if (idMsg.equals("msg6")) {
      return msg6();
    } else if (idMsg.equals("msg7")) {
      return msg7();
    } else if (idMsg.equals("msg8")) {
      return msg8();
    } else if (idMsg.equals("msg9")) {
      return msg9();
    } else if (idMsg.equals("msg10")) {
      return msg10();
    } else if (idMsg.equals("msg11")) {
      return msg11();
    } else if (idMsg.equals("msg12")) {
      return msg12();
    } else if (idMsg.equals("msg13")) {
      return msg13();
    } else if (idMsg.equals("msg14")) {
      return msg14();
    } else if (idMsg.equals("msg15")) {
      return msg15();
    } else if (idMsg.equals("msg16")) {
      return msg16();
    } else if (idMsg.equals("msgLynx1")) {
      return msgLynx1();
    } else if (idMsg.equals("msgLynx2")) {
      return msgLynx2();
    } else if (idMsg.equals("msgT3270-1")) {
      return msgT3270_1();
    } else if (idMsg.equals("msgT3270-2")) {
      return msgT3270_2();
    } else if (idMsg.equals("msgTB60-1")) {
      return msgTB60_1();
    } else if (idMsg.equals("msgTB60-2")) {
      return msgTB60_2();
    } else if (idMsg.equals("msgTB60-3")) {
      return msgTB60_3();
    } else if (idMsg.equals("msgOBII_1")) {
      return msgOBII_1();
    } else if (idMsg.equals("msgOBII_2")) {
      return msgOBII_2();
    } else if (idMsg.equals("msgOBIIcsv3-1")) {
      return msgOBIIcsv3();
    } else if (idMsg.equals("msgMT103-1")) {
      return msgMT103_1();
    } else if (idMsg.equals("msgMT103-2")) {
      return msgMT103_2();
    } else if (idMsg.equals("msgMTACK-1")) {
      return msgMTACK_1();
    } else if (idMsg.equals("msgMTACK-2")) {
      return msgMTACK_2();
    } else if (idMsg.equals("msg-soh-rr-1")) {
      return msgSohRR1();
    } else if (idMsg.equals("msg-soh-rr-2")) {
      return msgSohRR2();
    } else if (idMsg.equals("msg-soh-rr-3")) {
      return msgSohRR3();
    } else if (idMsg.equals("msgMultiple-201-ZOS")) {
      return msgMultiple_201_ZOS();
    } else if (idMsg.equals("msgMultiple-201-GRA")) {
      return msgMultiple_201_GRA();
    } else if (idMsg.equals("msgMultiple-207-ZOS")) {
      return msgMultiple_207_ZOS();
    } else if (idMsg.equals("msgMultiple-207-GRA")) {
      return msgMultiple_207_GRA();
    } else if (idMsg.equals("msgMultiple-201-ZOS-MSGDIST")) {
      return msgMultiple_201_ZOS_MSGDIST();
    } else if (idMsg.equals("msgMultiple-301")) {
      return msgMultiple_301();
    } else if (idMsg.equals("msgMultiple-307")) {
      return msgMultiple_307();
    } else if (idMsg.equals("msgRecordParser-1")) {
      return msgRecord1();
    } else if (idMsg.equals("msgRecordParser-2")) {
      return msgRecord2();
    } else if (idMsg.equals("msgRecordParser-3")) {
      return msgRecord3();
    } else if (idMsg.equals("msgTB60Record-1")) {
      return msgTB60Record_1();
    } else if (idMsg.equals("msgTB60Record-2")) {
      return msgTB60Record_2();
    } else if (idMsg.equals("msgTB60Record-3")) {
      return msgTB60Record_3();
    } else if (idMsg.equals("msgRECORDOBII-1")) {
      return msgRECORDOBII_1();
    } else if (idMsg.equals("msgRECORDOBII-2")) {
      return msgRECORDOBII_2();
    } else if (idMsg.equals("msgEmbeddedXmlOBII-1")) {
      return msgEMBEDDEDXMLOBII_1();
    } else if (idMsg.equals("msgEmbeddedXmlOBII-2")) {
      return msgEMBEDDEDXMLOBII_2();
    } else if (idMsg.equals("msgEmbeddedXmlOBII-3")) {
      return msgEMBEDDEDXMLOBII_3();
    } else if (idMsg.equals("msgT3270-sat-1")) {
      return satT3270Msg1();
    } else if (idMsg.equals("msgT3270-sat-2")) {
      return satT3270Msg2();
    } else if (idMsg.equals("msgT3270-net-1")) {
      return satT3270Net1();
    } else if (idMsg.equals("trxop")) {
      return tbqqReq();
    } else if (idMsg.equals("trxop-resp1")) {
      return tbqqResp1();
    } else if (idMsg.equals("trxop-resp2")) {
      return tbqqResp2();
    } else if (idMsg.equals("sat500223")) {
      return sat500223Req();
    } else if (idMsg.equals("sat500223-resp1")) {
      return sat500223Resp1();
    } else if (idMsg.equals("sat500223-resp2")) {
      return sat500223Resp2();
    } else if (idMsg.equals("trxopBPG8L")) {
      return trxopBPGLReq();
    } else if (idMsg.equals("trxopBPG8L-resp1")) {
      return trxopBPGLResp1();
    } else if (idMsg.equals("trxopBPG8L-resp2")) {
      return trxopBPGLResp2();
    } else if (idMsg.equals("sat005891")) {
      return sat005891Req();
    } else if (idMsg.equals("sat005891-resp1")) {
      return sat005891Resp1();
    } else if (idMsg.equals("sat005891-resp2")) {
      return sat005891Resp2();
    } else if (idMsg.equals("msgOBIIcsv1-1")) {
      return msgOBIIcsv1();
    } else if (idMsg.equals("msgOBIIcsv2-1")) {
      return msgOBIIcsv2();
    } else if (idMsg.equals("msgPrc-1")) {
      return msgPrc1();
    } else if (idMsg.equals("msgSohOnewway-1")) {
      return msgSohOnewway();
    } else if (idMsg.equals("msgSpringApp-1")) {
      return msgSpringApp();
    } else if (idMsg.equals("json-req")) {
      return jsonReq();
    } else if (idMsg.equals("json-resp1")) {
      return jsonResp1();
    } else if (idMsg.equals("json-resp2")) {
      return jsonResp2();
    } else if (idMsg.equals("msg-moses-1")) {
      return msgMoses1();
    } else if (table.containsKey(idMsg)) {
      return table.get(idMsg).get();
    } else {
      return ("NOT_FOUND:Mensaje '" + idMsg + "' no encontrado");
    }
  }
  
  public void put(String idMsg, Supplier<String> msgSupp) {
    log.debug("Registration of msg cons in table: {}", idMsg);
    table.put(idMsg, msgSupp);
    table.put(highDashes(idMsg), msgSupp);
  }

  private String highDashes(String id) {
    return id.replace('_', '-');
  }

  private int sequencial = 0;

  private String jsonReq() {
    // return
    // "\"root\":\"{\"cinemas\":[{\"name\":\"Cinema1\"},{\"name\":\"Cinema2\"},{\"name\":\"Cinema3\"}]}\"";
    return "\"mqbody\": {\"paymentType\":\"CUST\",\"indicatorTarget2\":\"N\",\"endToEndReference\":\"MAN/000002186779\",\"paymentAmount\":\"1,44\",\"paymentCurrency\":\"EUR\",\"date\":\"31/07/2025\",\"orderingName\":\"Santander Consumer Bank AG\",\"orderingTown\":\"Mönchengladbach\",\"orderingPostcode\":\"41061\",\"orderingAccount\":\"DE53370206009901380246\",\"orderingInstitution\":\"SCFBDE33XXX\",\"beneficiaryName\":\"Lady Gaga\",\"beneficiaryAccount\":\"ES9100730100542000017862\",\"remittanceInformation\":\"prueba mq dual run   \"}";
  }

  private String jsonResp1() {
    return "{\"returnCode\": \"OK\",\"medio\": \"SCT\",\"endToEndReference\": \"MAN/000002623633\",\"electronicPaymentReference\": \"32941029632BBGQLBY\"}";
    // return
    // "{\"cinemas\":[{\"name\":\"Cinema1\",\"movies\":[{\"title\":\"Movie1\",\"synopsis\":\"Brief
    // synopsis of
    // Movie1...áñíüú\",\"actors\":[\"Actor1\",\"Actor2\",\"Actor3\",\"Actor4\",\"Actor5\"],\"showtimes\":[\"10:00\",\"12:00\",\"14:00\",\"16:00\",\"18:00\",\"20:00\"]},{\"title\":\"Movie2\",\"synopsis\":\"Brief
    // synopsis of
    // Movie2...\",\"actors\":[\"Actor6\",\"Actor7\",\"Actor8\",\"Actor9\",\"Actor10\"],\"showtimes\":[\"11:00\",\"13:00\",\"15:00\",\"17:00\",\"19:00\",\"21:00\"]}]},{\"name\":\"Cinema2\",\"movies\":[{\"title\":\"Movie3\",\"synopsis\":\"Brief
    // synopsis of
    // Movie3...\",\"actors\":[\"Actor11\",\"Actor12\",\"Actor13\",\"Actor14\",\"Actor15\"],\"showtimes\":[\"10:00\",\"12:00\",\"14:00\",\"16:00\",\"18:00\",\"20:00\"]},{\"title\":\"Movie4\",\"synopsis\":\"Brief
    // synopsis of
    // Movie4...\",\"actors\":[\"Actor16\",\"Actor17\",\"Actor18\",\"Actor19\",\"Actor20\"],\"showtimes\":[\"11:00\",\"13:00\",\"15:00\",\"17:00\",\"19:00\",\"21:00\"]}]},{\"name\":\"Cinema3\",\"movies\":[{\"title\":\"Movie5\",\"synopsis\":\"Brief
    // synopsis of
    // Movie5...\",\"actors\":[\"Actor21\",\"Actor22\",\"Actor23\",\"Actor24\",\"Actor25\"],\"showtimes\":[\"10:00\",\"12:00\",\"14:00\",\"16:00\",\"18:00\",\"20:00\"]},{\"title\":\"Movie6\",\"synopsis\":\"Brief
    // synopsis of
    // Movie6...\",\"actors\":[\"Actor26\",\"Actor27\",\"Actor28\",\"Actor29\",\"Actor30\"],\"showtimes\":[\"11:00\",\"13:00\",\"15:00\",\"17:00\",\"19:00\",\"21:00\"]}]},{\"name\":\"Cinema4\",\"movies\":[{\"title\":\"Movie7\",\"synopsis\":\"Brief
    // synopsis of
    // Movie7...\",\"actors\":[\"Actor31\",\"Actor32\",\"Actor33\",\"Actor34\",\"Actor35\"],\"showtimes\":[\"10:00\",\"12:00\",\"14:00\",\"16:00\",\"18:00\",\"20:00\"]},{\"title\":\"Movie8\",\"synopsis\":\"Brief
    // synopsis of
    // Movie8...\",\"actors\":[\"Actor36\",\"Actor37\",\"Actor38\",\"Actor39\",\"Actor40\"],\"showtimes\":[\"11:00\",\"13:00\",\"15:00\",\"17:00\",\"19:00\",\"21:00\"]}]}]}";
  }

  private String jsonResp2() {
    sequencial++;
    return "{\"returnCode\": \"OK\",\"medio\": \"SCT\",\"endToEndReference\": \"MAN/000002623633\",\"electronicPaymentReference\": \"32941029632BBGQLBY\"}";
    // return "{\"sequencial\":" + sequencial
    // +",\"cinemas\":[{\"name\":\"Cinema1\",\"movies\":[{\"title\":\"Movie1\",\"synopsis\":\"Brief
    // synopsis of
    // Movie1...áñíüú\",\"actors\":[\"Actor1\",\"Actor2\",\"Actor3\",\"Actor4\",\"Actor5\"],\"showtimes\":[\"10:00\",\"12:00\",\"14:00\",\"16:00\",\"18:30\",\"20:00\"]},{\"title\":\"Movie2diff\",\"synopsis\":\"Brief
    // synopsis of
    // Movie2...\",\"actors\":[\"Actor6\",\"Actor7\",\"Actor8\",\"Actor9\",\"Actor10\"],\"showtimes\":[\"11:00\",\"13:00\",\"15:00\",\"17:00\",\"19:00\",\"21:00\"]}]},{\"name\":\"Cinema2\",\"movies\":[{\"title\":\"Movie3\",\"synopsis\":\"Brief
    // synopsis of
    // Movie3...\",\"actors\":[\"Actor11\",\"Actor12\",\"Actor13\",\"Actor14\",\"Actor15\"],\"showtimes\":[\"10:00\",\"12:00\",\"14:00\",\"16:00\",\"18:00\",\"20:00\"]},{\"title\":\"Movie4\",\"synopsis\":\"Brief
    // synopsis of
    // Movie4...\",\"actors\":[\"Actor16\",\"Actor17\",\"Actor18\",\"Actor19\",\"Actor20\"],\"showtimes\":[\"11:00\",\"13:00\",\"15:00\",\"17:00\",\"19:00\",\"21:00\"]}]},{\"name\":\"Cinema3\",\"movies\":[{\"title\":\"Movie5\",\"synopsis\":\"Brief
    // synopsis of
    // Movie5...\",\"actors\":[\"Actor21\",\"Actor22\",\"Actor23\",\"Actor24\",\"Actor25\"],\"showtimes\":[\"10:00\",\"12:00\",\"14:00\",\"16:00\",\"18:00\",\"20:00\"]},{\"title\":\"Movie6\",\"synopsis\":\"Brief
    // synopsis of
    // Movie6...\",\"actors\":[\"Actor26\",\"Actor27\",\"Actor28\",\"Actor29\",\"Actor30\"],\"showtimes\":[\"11:00\",\"13:00\",\"15:00\",\"17:00\",\"19:00\",\"21:00\"]}]},{\"name\":\"Cinema4\",\"movies\":[{\"title\":\"Movie7\",\"synopsis\":\"Brief
    // synopsis of
    // Movie7...\",\"actors\":[\"Actor31\",\"Actor32\",\"Actor33\",\"Actor34\",\"Actor35\"],\"showtimes\":[\"10:00\",\"12:00\",\"14:00\",\"16:00\",\"18:00\",\"20:00\"]},{\"title\":\"Movie8\",\"synopsis\":\"Brief
    // synopsis of
    // Movie8...\",\"actors\":[\"Actor36\",\"Actor37\",\"Actor38\",\"Actor39\",\"Actor40\"],\"showtimes\":[\"11:00\",\"13:00\",\"15:00\",\"17:00\",\"19:00\",\"21:00\"]}]}]}";
  }


  private String tbqqReq() {
    return "TBQQL12             J00013000100010001                   N127.000.000.001ENB465880A2BBA57001FA39838B465880A2BBA02001FAB9844N00000                              IParticulares_ENS       180.101.136.010                        BKS     000017¤¤{\"secondary\":\"N\"}¤¤¤¤";
  }

  private String tbqqResp2() {
    return "0     N 00010001                                                                                          00000024000000¤¤bbbbAAzzAAAkAAAAAAABAAACAAAJAAALAAATAB12ALTAALVAAYAYAYDEAYD1AYD2DSADEMAREMP8FGHGMAMANYNYPARBPDAV¤PDAV¤                            1111    000111110001    000100010001000600060001000600010006    0006¤0006¤ASDASDASD                                         ASDASDASD                                         ASDASDASD                                         ASDASDASD                                         ASDASDASD                                         ASDASDASD                                         ASDASDASD                                         ASDASDASD                                         ASDASDASD                                         ASDASDASDD                                        ASDASDASD                                         ASDASDASD                                         ASDASDASD                                         ASDASDASD                                         ASDASDASD                                         ASDASDASD                                         ASDASDASD                                         ASDASDASD                                         ASDASDASD                                         ASDASDASD                                         ASDASDASD                                         ASDASDASD                                         ASDASDASD                                         ASDASDASD                                         ¤";
  }

  private String tbqqResp1() {
    return "0     N 00010001                                                                                          00000024000000¤¤aaaaAAzzAAAkAAAAAAABAAACAAAJAAALAAATAB12ALTAALVAAYAYAYDEAYD1AYD2DSADEMAREMP8FGHGMAMANYNYPARBPDAV¤PDAV¤                            1111    000111110001    000100010001000600060001000600010006    0006¤0006¤ASDASDASD                                         ASDASDASD                                         ASDASDASD                                         ASDASDASD                                         ASDASDASD                                         ASDASDASD                                         ASDASDASD                                         ASDASDASD                                         ASDASDASD                                         ASDASDASDD                                        ASDASDASD                                         ASDASDASD                                         ASDASDASD                                         ASDASDASD                                         ASDASDASD                                         ASDASDASD                                         ASDASDASD                                         ASDASDASD                                         ASDASDASD                                         ASDASDASD                                         ASDASDASD                                         ASDASDASD                                         ASDASDASD                                         ASDASDASD                                         ¤";
  }

  private String trxopBPGLReq() {
    return "BPG8L12             J00001000100010001                   N               E B4658B231E87D000AE0D8D08UNCON#231E870F00AFF3A216N00000                              PartenonSampleApp       180.101.139.035                        UNVSPRNG000150¤¤{\"secondary\":\"N\",\"UNIVERSAL_CONNECTOR_DATA\":{\"APP_NAME\":\"PartenonSampleApp\",\"PROJECT_NAME\":null,\"SERVER_IP\":\"180.101.139.35\",\"TECHNOLOGY\":\"UNVSPRNG\"}}501¤502¤20190101¤0049¤";
  }

  private String trxopBPGLResp1() {
    return "0     S 00010001                                                                                          00000001000000¤¤501¤502¤0049¤VISA CLASSIC AFFINITY                             ¤";
  }

  private String trxopBPGLResp2() {
    return "0     S 00010001                                                                                          00000001000000¤¤501¤502¤0048¤VISA CLASSIC AFFINITY                             ¤";
  }

  private String sat500223Req() {
    return "50175002230000000003E`S`DEISM0  `********````A`";
  }

  private String sat500223Resp1() {
    return "50175002230000000003E        S0000000`OPERACION REALIZADA CORRECTAMENTE                                                                                                                                                                                                                         `2023-05-19-11.34.39.978356`0030`0001`......`00050090358S`NACS`........`........................................`SUANCES MARTINEZ, IGNACIO.....`          `          `          `1A97359F7461`DTODES  ISFDESA RECDES  RECTECNOYTDEMENUYTLBDESA`..................`........................`................................................................................................................................................................................................................................................`OREX`OPA1OPA2`";
  }

  private String sat500223Resp2() {
    return "50185002230000000003N        S0000000`OPERACION REALIZADA CORRECTAMENTE                                                                                                                                                                                                                         `2023-05-18-11.34.39.978356`0030`0001`......`00050090358S70`NACS`........`........................................`SUANCES MARTINEZ, IGNACIO.....`          `          `          `1A97359F7461`DTODES  ISFDESA RECDES  RECTECNOYTDEMENUYTLBDESA`..................`........................`................................................................................................................................................................................................................................................`OREX`OPA1OPA2`";
  }

  private String sat005891Req() {
    return "00010058916000000002E             N                B4658B771B740C00DC1C7108UNCON#771B748000DC1C7109N0000                              PartenonSampleApp       180.101.139.119                        UNVSPRNG000151¤{\"secondary\":\"N\",\"UNIVERSAL_CONNECTOR_DATA\":{\"APP_NAME\":\"PartenonSampleApp\",\"PROJECT_NAME\":null,\"SERVER_IP\":\"180.101.139.119\",\"TECHNOLOGY\":\"UNVSPRNG\"}}¤";
  }

  private String sat005891Resp1() {
    return "00010058916000000002E        N0000000¤OPERACION CORRECTA                                                                                                                                                                                                                                        ¤-                                                 ¤00238  ¤CUSUR¤A39000013   ¤   ¤0030¤28¤70547¤01¤28014¤20231115¤20231116¤20231117¤20231120¤20231116¤0001¤0049¤S¤N¤S¤MADRID, ALCALA, 28                                ¤BANCO SANTANDER, S.A.                             ¤MADR ALCA¤MADRID, ALCALA    ¤BANCO SANTANDER¤MADRID                        ¤USU.CANAL CREDINET,            ¤ALCALA                                            ¤28  ¤            ¤ES¤A¤URB¤S¤1¤CL¤00178¤GCIARQCAGCICBMYAGCICBTOAGCICSOITGCICSOTTGCICSVGAGCICUKEARCLICA2 RE@SAT19YSDETAGEYUPSDEP YURG10  ¤E421B832D4697858¤100421¤";
  }

  private String sat005891Resp2() {
    return "00020058916000000002E        N0000000¤OPERACION CORRECTA                                                                                                                                                                                                                                        ¤-                                                 ¤00238  ¤CUSUR¤A39000013   ¤   ¤0030¤28¤70547¤01¤28014¤20231115¤20231116¤20231117¤20231120¤20231116¤0001¤0049¤S¤N¤S¤MADRID, ALCALA, 29                                ¤BANCO SANTANDER, S.A.                             ¤MADR ALCA¤MADRID, ALCALA    ¤BANCO SANTANDER¤SEVILLA                       ¤USU.CANAL CREDINET,            ¤ALCALA                                            ¤28  ¤            ¤ES¤A¤URB¤S¤1¤CL¤00178¤GCIARQCAGCICBMYAGCICBTOAGCICSOITGCICSOTTGCICSVGAGCICUKEARCLICA2 RE@SAT19YSDETAGEYUPSDEP YURG10  ¤E421B832D4697858¤100421¤";
  }

  private String msg1() {
    return PASMsgsExamples.MSGS[0];
    // return load(0);
    // return "@PRMDESPAMPAMPMPPEN004MQGRPAST1
    // UK_PASCLOG......................................@SAT0012SP ********MQGRPAST1
    // ....UK_PASPPENVREQB ....MQGRPAST1.OFF ....UK_PASCSRECANSB ....MUQ1 ....UK_PASCINCI
    // ....0SPSSSS22MP0D QC1CDPL 000600 30000 420000NONE NONE 5 0 C1832D . PS700******** 0 005
    // ................. @SAT0012D
    // 18321000001O00N10611201647622647162528220002000000000250000000000250000000000250002020100816152380010100210000000310000826260101K050004722003282620201008
    // 000000000000000347347347110000101000100000000000028216001651 000 Shabby Co Bon Voyage TRAV
    // London 05302013409 09022200003665 1122.........8268268260000 00000003473473471110081615230016
    // 00000000 ";
  }

  private String msg2() {
    return PASMsgsExamples.MSGS[1];
    // return load(1);
    // return "@PRMDESPAMPAMPMPPEN004MQGRPAST1
    // UK_PASCLOG......................................@SAT0012SP ********MQGRPAST1
    // ....UK_PASPPENVREQB ....MQGRPAST1.OFF ....UK_PASCSRECANSB ....MUQ1 ....UK_PASCINCI
    // ....0SPSSSS22MP0D QC1CDPL 000600 30000 420000NONE NONE 5 0 C1832D . PS700******** 0 005
    // ................. @SAT0012D
    // 18321000001O00N10611201647622647162528220002000000000250000000000250000000000250002020100816152380010100210000000310000826260101K050004722003282620201008
    // 000000000000000347347347110000101000100000000000028216001651 000 Shabby Co Bon Voyage TRAV
    // London 05302013409 09022200003665 1122.........8268268260000 00000003473473471110081615230016
    // 00000000 ";
  }

  private String msg3() {
    return changeMsg3(msg2());
    // return "@PRMDESPAMPAMPMPPEN004MQGRPAST1
    // UK_PASCLOG......................................@SAT0012SP ********MQGRPAST1
    // ....UK_PASPPENVREQB ....MQGRPAST1.OFF ....UK_PASCSRECANSB ....MUQ1 ....UK_PASCINCI
    // ....0SPSSSS22MP0D QC1CDPL 000600 30000 420000NONE NONE 5 0 C1832D . PS700******** 0 005
    // ................. @SAT0012D
    // 18321000001O00N10611201647622647162528220002000000000250000000000250000000000250002020100816152380010100210000000310000826260101K050004722003282620201008
    // 000000000000000347347347110000101000100000000000028216001651 000 Shabby Co Bon Voyage TRAV
    // London 05302013409 09022200003665 1122.........8268268260000 00000003473473471110081615230016
    // 00000000 ";
  }

  private String msg4() {
    return PASMsgsExamples.MSGS[3];
    // return load(1);
    // return "@PRMDESPAMPAMPMPPEN004MQGRPAST1
    // UK_PASCLOG......................................@SAT0012SP ********MQGRPAST1
    // ....UK_PASPPENVREQB ....MQGRPAST1.OFF ....UK_PASCSRECANSB ....MUQ1 ....UK_PASCINCI
    // ....0SPSSSS22MP0D QC1CDPL 000600 30000 420000NONE NONE 5 0 C1832D . PS700******** 0 005
    // ................. @SAT0012D
    // 18321000001O00N10611201647622647162528220002000000000250000000000250000000000250002020100816152380010100210000000310000826260101K050004722003282620201008
    // 000000000000000347347347110000101000100000000000028216001651 000 Shabby Co Bon Voyage TRAV
    // London 05302013409 09022200003665 1122.........8268268260000 00000003473473471110081615230016
    // 00000000 ";
  }

  private String msg5() {
    return PASMsgsExamples.MSGS[4];
    // return load(1);
    // return "@PRMDESPAMPAMPMPPEN004MQGRPAST1
    // UK_PASCLOG......................................@SAT0012SP ********MQGRPAST1
    // ....UK_PASPPENVREQB ....MQGRPAST1.OFF ....UK_PASCSRECANSB ....MUQ1 ....UK_PASCINCI
    // ....0SPSSSS22MP0D QC1CDPL 000600 30000 420000NONE NONE 5 0 C1832D . PS700******** 0 005
    // ................. @SAT0012D
    // 18321000001O00N10611201647622647162528220002000000000250000000000250000000000250002020100816152380010100210000000310000826260101K050004722003282620201008
    // 000000000000000347347347110000101000100000000000028216001651 000 Shabby Co Bon Voyage TRAV
    // London 05302013409 09022200003665 1122.........8268268260000 00000003473473471110081615230016
    // 00000000 ";
  }

  private String msg6() {
    return PASMsgsExamples.MSGS[5];
  }

  private String msg7() {
    return PASMsgsExamples.MSGS[6];
  }

  private String msg8() {
    return PASMsgsExamples.MSGS[7];
  }

  private String msg9() {
    return PASMsgsExamples.MSGS[8];
  }

  private String msg10() {
    return PASMsgsExamples.MSGS[9];
  }

  private String msg11() {
    return PASMsgsExamples.MSGS[10];
  }

  private String msg12() {
    return PASMsgsExamples.MSGS[11];
  }

  private String msg13() {
    return PASMsgsExamples.MSGS[12];
  }

  private String msg14() {
    return PASMsgsExamples.MSGS[13];
  }

  private String msg15() {
    return PASMsgsExamples.MSGS[14];
  }

  private String msg16() {
    return PASMsgsExamples.MSGS[15];
  }

  private String msgPrc1() {
    return PASMsgsExamples.MSGS[16];
  }

  private String changeMsg3(String payload) {
    // se cambia la sesion de pas
    payload = change(payload, 780, "0032941");
    // se cambian los saldos
    return change(payload, 880, "112233");
  }

  private String change(String orig, int off, String s) {
    return orig.substring(0, off) + s + orig.substring(off + s.length());
  }

  private String msgLynx1() {
    return "<log:LogRecord LogType=\"4\" GlobalID=\"G061633678CBBBHZB       \" Lang=\"ES\"><Created>2021-04-06T11:08:38.35+01:00</Created><GroupID Type=\"2\">1633678CBBBHZB       406</GroupID><GroupID Type=\"3\">4061633678CBBBHZB       </GroupID><CreatedBy xsi:type=\"log:BSType\"><categoryName>root.operation.internal.ChequeDebit</categoryName><state Type=\"OPERATION\" Name=\"OK\"/></CreatedBy><LogData DataType=\"1\"><]£CDATA£<logRecordAppData><sortCode>090222</sortCode><accountNumber>10136577 </accountNumber><partenonAccountNumber>001522683008501325</partenonAccountNumber><customerType>J</customerType><customerNumber>000752936</customerNumber><productType>300</productType><txnDateTime>20210406110838</txnDateTime><creditDebit>1</creditDebit><txnCode>684</txnCode><sourceEntity>0932</sourceEntity><branch>0000</branch><draweeSortCode>302880</draweeSortCode><draweeAccNo>11111111</draweeAccNo><chequeNumber>000162</chequeNumber><partenonDraweeAccNo>                  </partenonDraweeAccNo><partenonChequeNo>02986669</partenonChequeNo><amount>00000000000030000</amount><currency>826</currency><chequePayDecision>00</chequePayDecision><returnReasonCode>00</returnReasonCode><chequeDepositDate>20210406</chequeDepositDate><depositCentre>0000</depositCentre><partenonReference>00151633678CBBBHZB</partenonReference><depositChannel>00</depositChannel><userid>00000000</userid><fraudReasonCode>000000</fraudReasonCode><LDAPUID>00000000</LDAPUID><suspectedFraud>0</suspectedFraud><representedCheque>0</representedCheque><cardNumber>0000000000000000000</cardNumber><thirdPartyDepositChannel>AUTO</thirdPartyDepositChannel></logRecordAppData>!!></LogData>                                   <InfraData xsi:type=\"log:BKSAuditInfraData\"><UserID>        </UserID><IPAddress>              </IPAddress><CalledServiceName>OperationContainerService</CalledServiceName><ScenarioName>CHEQUE_IMAGING</ScenarioName><SessionID>                    </SessionID><SessionIDSec>                    </SessionIDSec><ServerID>                    </ServerID></InfraData><LogLevel>10</LogLevel><LogLevel>LynxNRT</LogLevel></log:LogRecord>";
  }

  private String msgLynx2() {
    return "<log:LogRecord LogType=\"4\" GlobalID=\"G061633678CBBBPPP       \" Lang=\"ES\"><Created>2021-04-06T11:11:47.44+01:00</Created><GroupID Type=\"2\">1633678CBBBPPP       406</GroupID><GroupID Type=\"3\">4061633678CBBBPPP       </GroupID><CreatedBy xsi:type=\"log:BSType\"><categoryName>root.operation.internal.ChequeDebit</categoryName><state Type=\"OPERATION\" Name=\"OK\"/></CreatedBy><LogData DataType=\"1\"><]£CDATA£<logRecordAppData><sortCode>090222</sortCode><accountNumber>10136577 </accountNumber><partenonAccountNumber>001522683008501325</partenonAccountNumber><customerType>J</customerType><customerNumber>000752936</customerNumber><productType>300</productType><txnDateTime>20210406110838</txnDateTime><creditDebit>1</creditDebit><txnCode>684</txnCode><sourceEntity>0932</sourceEntity><branch>0000</branch><draweeSortCode>302880</draweeSortCode><draweeAccNo>11111111</draweeAccNo><chequeNumber>000162</chequeNumber><partenonDraweeAccNo>                  </partenonDraweeAccNo><partenonChequeNo>02986669</partenonChequeNo><amount>00000000000030000</amount><currency>826</currency><chequePayDecision>05</chequePayDecision><returnReasonCode>01</returnReasonCode><chequeDepositDate>20210406</chequeDepositDate><depositCentre>0000</depositCentre><partenonReference>00151633678CBBBPPP</partenonReference><depositChannel>00</depositChannel><userid>00000000</userid><fraudReasonCode>000000</fraudReasonCode><LDAPUID>00000000</LDAPUID><suspectedFraud>0</suspectedFraud><representedCheque>0</representedCheque><cardNumber>0000000000000000000</cardNumber><thirdPartyDepositChannel>AUTO</thirdPartyDepositChannel></logRecordAppData>!!></LogData>                                   <InfraData xsi:type=\"log:BKSAuditInfraData\"><UserID>        </UserID><IPAddress>              </IPAddress><CalledServiceName>OperationContainerService</CalledServiceName><ScenarioName>CHEQUE_IMAGING</ScenarioName><SessionID>                    </SessionID><SessionIDSec>                    </SessionIDSec><ServerID>                    </ServerID></InfraData><LogLevel>10</LogLevel><LogLevel>LynxNRT</LogLevel></log:LogRecord>";
  }

  private String msgRecord1() {
    return "11=452154^A23=PX-25^A37=RAUL GONZALEZ^A45=1300.00^A";
  }

  private String msgRecord2() {
    return "11=452154^A23=PX-37^A37=RAUL GONZALEZ^A45=1307.50^A";
  }

  private String msgRecord3() {
    return "trece=PX-25^A29=GRAN VIA^A57=TARIFA-1^A";
  }

  private String msgMT103_1() {
    return "ERU1290 +004+000+001                       0202U4S3101083303000IZBZK03 T083204T008320000031010833                    00000000000793298400000000000000000000000000000000000000000000000000000000031010990051312100000000000000000000000000                                                                            000000000000000000                                                                                000.00000000  000000000000000000                                                                                000.00000000                                          OONNNOOPPJ0                                                                                   0                0                                      202500916368570                                         15   2025-12-240001-01-012025-12-242025-12-242025-12-242025-06-192025-12-24 0000000000000.00 0000000000021.1901 0000000000021.190245 0000000000000.000000 0000000000000.000000 0000000000000.000000 0000000000000.000000 0000000000000.000000 0000000000000.000000 0000000000000.000000 0000000000000.000000 0000000000000.000000 0000000000000.000000 0000000000000.000000 0000000000000.000000 0000000000000.000000 0000000000000.000000 0000000000000.000000 0000000000000.000000 0000000000000.000000 0000000000000.000000 0000000000000.000000 0000000000000.000000 0000000000000.000000 0000000000000.000000                                          KN NNN              00.0000.0000.0000.0000.0000.00                     0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000                  0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00                                  0000.00                                    0000.00                                                                            0000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00                           00000000000.0000                                                         00000000000000000000                                                                                                                                                                       F910747572 000000000            0526608000000532930078302052660800000052025-06-192025-12-24           A70          00000000000000000                    03000004310001-01-01                    0000000000000.00 0000000000000.00                              0000000000000                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                ";
    // return " PMCP2BRC70 N BSCHGB2L XXXIRVTGB2X XXX103N NNESPMC0000000126062STP
    // eba4b30a-ad78-492f-b335-3837cdc5700b 02754GUFA02802596.001 CREDSDVA
    // GBP0000000002942282022-04-04GBP000000000294228000000000000 SUCURSAL LONDRES 00000 0 0 0 0
    // BSCHGB2LXXX 14601 MGTCBEBEXXX IRVTUS3NIBK IRVTGB2XXXX1287278260 IRVTUS3NIBK0 0 0 0 SHA
    // 000000000000000 000000000000000 000000000000000 000000000000000 000000000000000
    // 000000000000000 000000000000000 000000000000000 000000000000000 000000000000000
    // 000000000000000BNF CCLAIM GB00BDX8CX86 35672";
  }

  private String msgMT103_2() {
    return "ERU1290 +004+000+002                       0202U4S3101083303000IZBZK03 T083204T008320000031010833                    00000000000793298400000000000000000000000000000000000000000000000000000000031010990051312100000000000000000000000000                                                                            000000000000000000                                                                                000.00000000  000000000000000000                                                                                000.00000000                                          OONNNOOPPJ0                                                                                   0                0                                      202500916368570                                         15   2025-12-240001-01-012025-12-242025-12-242025-12-242025-06-192025-12-24 0000000000000.00 0000000000021.1901 0000000000021.190245 0000000000000.000000 0000000000000.000000 0000000000000.000000 0000000000000.000000 0000000000000.000000 0000000000000.000000 0000000000000.000000 0000000000000.000000 0000000000000.000000 0000000000000.000000 0000000000000.000000 0000000000000.000000 0000000000000.000000 0000000000000.000000 0000000000000.000000 0000000000000.000000 0000000000000.000000 0000000000000.000000 0000000000000.000000 0000000000000.000000 0000000000000.000000 0000000000000.000000                                          KN NNN              00.0000.0000.0000.0000.0000.00                     0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000                  0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00                                  0000.00                                    0000.00                                                                            0000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00                           00000000000.0000                                                         00000000000000000000                                                                                                                                                                       F910747572 000000000            0526608000000532930078302052660800000052025-06-192025-12-24           A70          00000000000000000                    03000004310001-01-01                    0000000000000.00 0000000000000.00                              0000000000000                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                ";
    // return " PMCP2BRC70 N BSCHGB2L XXXIRVTGB2X XXX103N NNESPMC0000000126077STP
    // eba4b30a-ad78-492f-b335-3837cdc5700b 02754GUFA02802596.001 CREDSDVA
    // GBP0000000002942282022-04-04GBP000000000294228000000000000 SUCURSAL LONDRES 00000 0 0 0 0
    // BSCHGB2LXXX 14601 MGTCBEBEXXX IRVTUS3NIBK IRVTGB2XXXX1287278260 IRVTUS3NIBK0 0 0 0 SHA
    // 000000000000000 000000000000000 000000000000000 000000000000000 000000000000000
    // 000000000000000 000000000000000 000000000000000 000000000000000 000000000000000
    // 000000000000000BNF CCLAIM GB00BDX8CX86 35672";
  }

  private String msgMTACK_1() {
    return "ERU1290 +004+000+000                       086000202U4S3101083303000IZBZK03 T083204T008320000031010833                    00000000000793298400000000000000000000000000000000000000000000000000000000031010990051312100000000000000000000000000                                                                            000000000000000000                                                                                000.00000000  000000000000000000                                                                                000.00000000                                          OONNNOOPPJ0                                                                                   0                0                                      202500916368570                                         15   2025-12-240001-01-012025-12-242025-12-242025-12-242025-06-192025-12-24 0000000000000.00 0000000000021.1901 0000000000021.190245 0000000000000.000000 0000000000000.000000 0000000000000.000000 0000000000000.000000 0000000000000.000000 0000000000000.000000 0000000000000.000000 0000000000000.000000 0000000000000.000000 0000000000000.000000 0000000000000.000000 0000000000000.000000 0000000000000.000000 0000000000000.000000 0000000000000.000000 0000000000000.000000 0000000000000.000000 0000000000000.000000 0000000000000.000000 0000000000000.000000 0000000000000.000000 0000000000000.000000                                          KN NNN              00.0000.0000.0000.0000.0000.00                     0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000                  0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00                                  0000.00                                    0000.00                                                                            0000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00                           00000000000.0000                                                         00000000000000000000                                                                                                                                                                       F910747572 000000000            0526608000000532930078302052660800000052025-06-192025-12-24           A70          00000000000000000                    03000004310001-01-01                    0000000000000.00 0000000000000.00                              0000000000000                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                ";
    // return "ERU1290 +004+000+000 086000202U4S3101083303000IZBZK03 T083204T008320000031010833
    // 00000000000792846500000000000792846500000000000000000000000000000000000000031010990051597200000000990046825731010833
    // Benni Musterkunde 000000000000000000 100.00000000NW000000000000000000 000.00000000 OONNNOOPPJ
    // 0000000000000.00 00000000000.0000 202600916378725
    // 154202026-01-160001-01-012026-01-162026-01-162026-01-162025-04-262026-01-16 0000000000000.00
    // 0000000000108.3301 0000000000108.330245 0000000000000.000000 0000000000000.000000
    // 0000000000000.000000 0000000000000.000000 0000000000000.000000 0000000000000.000000
    // 0000000000000.000000 0000000000000.000000 0000000000000.000000 0000000000000.000000
    // 0000000000000.000000 0000000000000.000000 0000000000000.000000 0000000000000.000000
    // 0000000000000.000000 0000000000000.000000 0000000000000.000000 0000000000000.000000
    // 0000000000000.000000 0000000000000.000000 0000000000000.000000 0000000000000.000000 KN NNN
    // 25.0025.0000.0005.5000.0000.00 0000000000000.00 0000000000000.00 0000000000000.00
    // 0000000000108.33 0000000000000.00 0000000000108.33 0000000000027.08 0000000000001.48
    // 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000
    // 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00
    // 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00
    // 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00
    // 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00
    // 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00
    // 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00
    // 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00
    // 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000 0000000000000.00
    // 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00
    // 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00
    // 0000000000000.00 00000000000.0000 0000000000000.00 0000000000000.00 0000000000000.00
    // 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00
    // 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 00000000000.0000
    // 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00
    // 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00 0000000000000.00
    // 0000000000000";
    // return "0001234567890123456N000PMC0000000126062AAABBBCCCCDDDDDD2022-07-121641-121641";
  }

  private String msgMTACK_2() {
    return "0001234567890123456N000PMC0000000126062AAABBBCCCCDDDDDD2022-07-121641";
  }

  private String msgT3270_1() {
    String cad = new String(T3270MsgsExamples.MSGS[0], CS_284);
    String[] nombres = {"FRACTA", "FRACTB", "FRACTC", "FRACTD", "FRACTA"};
    int indiceAleatorio = ThreadLocalRandom.current().nextInt(nombres.length);
    String nombreAleatorio = nombres[indiceAleatorio];
    cad = cad.replaceAll("FRACTO", nombreAleatorio);

    return new String(T3270MsgsExamples.MSGS[0], CS_284);
  }

  private String msgT3270_2() {
    return new String(T3270MsgsExamples.MSGS[1], CS_284);
  }

  private String msgTB60_1() {
    String cad = TB60MsgsExamples.MSGS[0];
    String[] nombres = {"X230526A", "X230526B", "X230526C", "X230526D", "X230526E"};

    int indiceAleatorio = ThreadLocalRandom.current().nextInt(nombres.length);
    String nombreAleatorio = nombres[indiceAleatorio];
    cad = cad.replaceAll("X2302569", nombreAleatorio);
    return cad;
    // return TB60MsgsExamples.MSGS[0];
  }

  private String msgTB60_2() {
    return TB60MsgsExamples.MSGS[1];
  }

  private String msgTB60_3() {
    return TB60MsgsExamples.MSGS[2];
  }

  private String msgTB60Record_1() {
    return TB60MsgsExamples.MSGSRECORD[0];
  }

  private String msgTB60Record_2() {
    return TB60MsgsExamples.MSGSRECORD[1];
  }

  private String msgTB60Record_3() {
    return TB60MsgsExamples.MSGSRECORD[2];
  }

  private String msgSohRR1() {
    return SOHMsgs.MSGS[0];
  }

  private String msgSohRR2() {
    return SOHMsgs.MSGS[1];
  }

  private String msgSohRR3() {
    return SOHMsgs.MSGS[2];
  }

  private String msgOBII_1() {
    return "SANT0007XXL1234567BBVA0013LLL7654321AB00ESTO ES UNA\r\nDESCRIPCION";
  }

  private String msgOBII_2() {
    return "SANT0009XXL8910112BBVA0013LLL7654321AB00ESTO ES UNA\r\nDESCRIPCION";
  }

  private String msgOBIIcsv3() {
    return "VALOR1, VALOR3, VALOR7, VALOR2";
  }

  private String msgMultiple_201_ZOS() {
    return "This is test 201          201123456781347-12471234561234567";
  }

  private String msgMultiple_201_GRA() {
    return "This is test 201          201123456781347-12471234567654321";
  }

  private String msgMultiple_207_ZOS() {
    return "This is test 207          2076661234567890123  2023-01-318889876543210987  87654321123456789";
  }

  private String msgMultiple_207_GRA() {
    return "This is test 207          2076661234567890123  2023-01-318889876543210987  87654321987654321";
  }

  private String msgMultiple_201_ZOS_MSGDIST() {
    return "This is test 201          201123456781374-24578123695214789";
  }

  private String msgMultiple_301() {
    return "This is test 301          301123456781347-RAUL        GONZALEZ BLANCO     1234567";
  }

  private String msgMultiple_307() {
    return "This is test 307          307VROBERTOCARLOS  1985-01-26777GRANADA   123456789";
  }

  private String msgRECORDOBII_1() {
    return "11=452154^A23=PX-25^A37=RAUL GONZALEZ^A45=1300.00^A";
  }

  private String msgRECORDOBII_2() {
    return "11=452154^A23=PX-37^A37=RAUL GONZALEZ^A45=1307.50^A";
  }

  private String msgEMBEDDEDXMLOBII_1() {
    return "<?xml version=\"1.0\" ?><FxFlowMD_root xmlns=\"http://www.tibco.com/schemas/FxFlowMD/FxFlowMD.xsd\">      <ApplicationID>Aggregator</ApplicationID><Message>&lt;?xml version=&quot;1.0&quot; ?&gt;&lt;event&gt;&lt;okErrorCode&gt;KO&lt;/okErrorCode&gt;&lt;uniqueId&gt;ECUR  001500754410060599-000000000000000&lt;/uniqueId&gt;&lt;ids&gt;&lt;systemDealId&gt;&lt;system&gt;ECUR  0015&lt;/system&gt;&lt;dealId&gt;001500754410060599&lt;/dealId&gt;&lt;/systemDealId&gt;&lt;/ids&gt;&lt;eventType&gt;Insert&lt;/eventType&gt;&lt;eventDate&gt;2023-04-03&lt;/eventDate&gt;&lt;eventTime&gt;12:39:50&lt;/eventTime&gt;&lt;exceptionMessage&gt;DuplicatedOperation&lt;/exceptionMessage&gt;&lt;sequenceStatus&gt;Completed&lt;/sequenceStatus&gt;&lt;sequenceDetail&gt;Entry Aggregator&lt;/sequenceDetail&gt;&lt;webPortal&gt;NO&lt;/webPortal&gt;&lt;deal&gt;&lt;producto&gt;441004&lt;/producto&gt;&lt;currencyPair&gt;EUR/GBP&lt;/currencyPair&gt;&lt;direction&gt;BUY&lt;/direction&gt;&lt;amount1&gt;1000,00&lt;/amount1&gt;&lt;amount2&gt;855,95&lt;/amount2&gt;&lt;valDate1&gt;2023-03-31&lt;/valDate1&gt;&lt;dealtCurrency&gt;EUR&lt;/dealtCurrency&gt;&lt;blockTrade&gt;NO&lt;/blockTrade&gt;&lt;entity&gt;0015&lt;/entity&gt;&lt;/deal&gt;&lt;/event&gt;</Message></FxFlowMD_root>";
  }

  private String msgEMBEDDEDXMLOBII_2() {
    return "<?xml version=\"1.0\" ?><FxFlowMD_root xmlns=\"http://www.tibco.com/schemas/FxFlowMD/FxFlowMD.xsd\">      <ApplicationID>Aggregator</ApplicationID><Message>&lt;?xml version=&quot;1.0&quot; ?&gt;&lt;event&gt;&lt;okErrorCode&gt;KO&lt;/okErrorCode&gt;&lt;uniqueId&gt;ECUR  001500754410060599-000000000000000&lt;/uniqueId&gt;&lt;ids&gt;&lt;systemDealId&gt;&lt;system&gt;ECUR  0015&lt;/system&gt;&lt;dealId&gt;001500754410060599&lt;/dealId&gt;&lt;/systemDealId&gt;&lt;/ids&gt;&lt;eventType&gt;Insert&lt;/eventType&gt;&lt;eventDate&gt;2023-04-03&lt;/eventDate&gt;&lt;eventTime&gt;18:05:25&lt;/eventTime&gt;&lt;exceptionMessage&gt;DuplicatedOperation&lt;/exceptionMessage&gt;&lt;sequenceStatus&gt;Completed&lt;/sequenceStatus&gt;&lt;sequenceDetail&gt;Entry Aggregator&lt;/sequenceDetail&gt;&lt;webPortal&gt;NO&lt;/webPortal&gt;&lt;deal&gt;&lt;producto&gt;441004&lt;/producto&gt;&lt;currencyPair&gt;EUR/GBP&lt;/currencyPair&gt;&lt;direction&gt;BUY&lt;/direction&gt;&lt;amount1&gt;1750,00&lt;/amount1&gt;&lt;amount2&gt;687,95&lt;/amount2&gt;&lt;valDate1&gt;2023-03-31&lt;/valDate1&gt;&lt;dealtCurrency&gt;EUR&lt;/dealtCurrency&gt;&lt;blockTrade&gt;NO&lt;/blockTrade&gt;&lt;entity&gt;0015&lt;/entity&gt;&lt;/deal&gt;&lt;/event&gt;</Message></FxFlowMD_root>";
  }

  private String msgEMBEDDEDXMLOBII_3() {
    return "<?xml version=\"1.0\" ?><FxFlowMD_root xmlns=\"http://www.tibco.com/schemas/FxFlowMD/FxFlowMD.xsd\">      <ApplicationID>Aggregator</ApplicationID><Message>&lt;?xml version=&quot;1.0&quot; ?&gt;&lt;event&gt;&lt;okErrorCode&gt;KO&lt;/okErrorCode&gt;&lt;uniqueId&gt;ECUR  001500754410060599-000000000000000&lt;/uniqueId&gt;&lt;ids&gt;&lt;systemDealId&gt;&lt;system&gt;ECUR  0015&lt;/system&gt;&lt;dealId&gt;001500754410060599&lt;/dealId&gt;&lt;/systemDealId&gt;&lt;/ids&gt;&lt;eventType&gt;Insert&lt;/eventType&gt;&lt;eventDate&gt;2023-04-03&lt;/eventDate&gt;&lt;eventTime&gt;18:05:25&lt;/eventTime&gt;&lt;exceptionMessage&gt;DuplicatedOperation&lt;/exceptionMessage&gt;&lt;sequenceStatus&gt;Completed&lt;/sequenceStatus&gt;&lt;sequenceDetail&gt;Entry Aggregator&lt;/sequenceDetail&gt;&lt;webPortal&gt;NO&lt;/webPortal&gt;&lt;deal&gt;&lt;producto&gt;441004&lt;/producto&gt;&lt;currencyPair&gt;EUR/GBP&lt;/currencyPair&gt;&lt;direction&gt;BUY&lt;/direction&gt;&lt;amount1&gt;1750,00&lt;/amount1&gt;&lt;amount2&gt;687,95&lt;/amount2&gt;&lt;valDate1&gt;2023-03-31&lt;/valDate1&gt;&lt;dealtCurrency&gt;EUR&lt;/dealtCurrency&gt;&lt;blockTrade&gt;NO&lt;/blockTrade&gt;&lt;entity&gt;0015&lt;/entity&gt;&lt;/deal&gt;&lt;/event&gt;</Message></FxFlowMD_root>";
  }

  private String msgOBIIcsv1() {
    return "VALOR1, VALOR3, VALOR4, VALOR2";
  }

  private String msgOBIIcsv2() {
    String valor = "";
    for (int i = 1; i <= 78; i++) {
      if (i != 78) {
        valor += "VALOR" + i + "|";
      } else {
        valor += "VALOR" + i;
      }
    }
    return valor;
  }

  private String satT3270Msg1() {
    try {
      byte[] array = DumpRead.readMsg("t3270-sat-1");
      return new String(array, CS_284);
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      return null;
    }
  }

  private String satT3270Msg2() {
    try {
      byte[] array = DumpRead.readMsg("t3270-sat-2");
      return new String(array, CS_284);
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      return null;
    }
  }

  private String satT3270Net1() {
    try {
      byte[] array = DumpRead.readMsg("t3270-net-1");
      return new String(array, CS_284);
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      return null;
    }
  }

  private String msgSohOnewway() {
    return SOHMsgs.MSGS[3];
  }

  private String msgSpringApp() {
    return "190120111000PRUEBA TRANSAC. ARRPPRUEBA RTMAAAAAAAAAA";
  }

  private String msgMoses1() {
    try {
      byte[] array = DumpRead.readMsg("msg-moses-1");
      return new String(array, CS_284);
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      return null;
    }
  }

  @SuppressWarnings("unused")
  private String load(int numline) {
    List<String> lines;

    try {
      if (filepath == null)
        filepath = find("data_pas_req_resp.log");
      lines = Files.readAllLines(filepath, StandardCharsets.ISO_8859_1);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    String line = lines.get(numline);
    String msg = line.substring(line.indexOf('\'') + 1, line.lastIndexOf('\''));
    return msg;
  }

  private Path find(String fileName) throws IOException {
    return Files.find(Paths.get("."), 10, (p, basicFileAttributes) -> {
      if (Files.isDirectory(p) || !Files.isReadable(p))
        return false;
      return p.getFileName().toString().equalsIgnoreCase(fileName);
    }).findFirst().get();
  }


}

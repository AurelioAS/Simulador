package com.simulador.utils;

public class TB60MsgsExamples {

  private static final String req_tmp =
      "120241201213556                  000765075041                                                              012691413K0119:0006                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              20100076507504100000000007034677020241206SOCIEDAD AGRICOLA SANTA ROSA SPA        00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000069002030549000001836640902CR0         135004000001000000000070346770000000000000000003500000000CLP                                                                                                                                                                                                                                                                       301000001000764646444AGRICOLA MORENO SPA                     000                                        000               000               04020000000000021003519060001000000000000                                        000000024843000N   000000000000000000000000000                                                                                                        000000000000                                                                                                                                                                                                                                                                                                                             601000001000000000623+0000024843000                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         301000002000130041035CARLOS ROBERTO MUSALEM CUMSILLE         000                                        000               000               04020000000000020802240060001000000000000                                        000000042840000N   000000000000000000000000000                                                                                                        000000000000                                                                                                                                                                                                                                                                                                                             601000002000000052452+0000014280000                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         601000002000000052518+0000014280000                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         601000002000000052567+0000014280000                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         301000003000812908006COMPAÑÍA AGROPECUARIA COPEVAL S.A.      000                                        000               000               04020000000000020801165100001000000000000                                        000000086037000N   000000000000000000000000000                                                                                                        000000000000                                                                                                                                                                                                                                                                                                                             601000003000005782648+0000040341000                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         601000003000005782999+0000045696000                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         301000004000775197730FOOD TRUCK BARBARA PIEDAD LOPEZ PARRAGUE000                                        000               000               04020000000000902709290780012000000000000                                        000000026190900N   000000000000000000000000000                                                                                                        000000000000                                                                                                                                                                                                                                                                                                                             601000004000000000041+0000026190900                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         301000005000766602347IGNAMEC LIMITADA                        000                                        000               000               04020000000000393703705520012000000000000                                        000000074970000N   000000000000000000000000000                                                                                                        000000000000                                                                                                                                                                                                                                                                                                                             601000005000000000261+0000033320000";
  private static final String req     =
      "008700410840000077002100520810000049A22B33XA001002003004005000001100000120000013";
  private static final String resp1   =
      "008700410840000077002100520810000049XA01TODO BIEN   ..................";
  private static final String resp2   =
      "008700410840000077002100520810000049XA02FUE MAL     ..................";

  private static final String reqRecord   = "trece=PX-25^A29=GRAN VIA^A57=TARIFA-1^A";
  private static final String respRecord1 = "once=452154^A23=PX-25^A37=RAUL GONZALEZ^A45=1300.00^A";
  private static final String respRecord2 = "once=452154^A23=PX-37^A37=RAUL GONZALEZ^A45=1307.50^A";

  public static final String[] MSGS = {
      req,
      resp1,
      resp2
  };

  public static final String[] MSGSRECORD = {
      reqRecord,
      respRecord1,
      respRecord2
  };

}


// - name: TBYMQEDC-LOCAL-PRU
// fields:
// - name: TBYMQEDC-RG-ASOIB
// fields:
// - name: TBYMQEDC-ASI-CONTRATO-L
// fields:
// - name: TBYMQEDC-ASI-IDEMPR-L
// length: 4
// - name: TBYMQEDC-ASI-IDCENT-L
// length: 4
// - name: TBYMQEDC-ASI-IDPROD-L
// length: 3
// - name: TBYMQEDC-ASI-NUMCTRC-L
// length: 7
// - name: TBYMQEDC-ASI-CONTRATO-G
// fields:
// - name: TBYMQEDC-ASI-IDEMPR-G
// length: 4
// - name: TBYMQEDC-ASI-IDECENT-G
// length: 4
// - name: TBYMQEDC-ASI-IDPROD-G
// length: 3
// - name: TBYMQEDC-ASI-NUMCTRC-G
// length: 7
// - name: TBYMQEDC-ASI-CODPROD
// length: 3
// - name: TBYMQEDC-ASI-CODSPROD
// length: 3
// - name: TBYMQEDC-ASI-ACCION
// length: 1
// - name: TBYMQEDC-ASI-SUBACCION
// length: 1
// - name: TBYMQEDC-TBL-TABIBAN
// occurs: 5
// fields:
// - name: TBYMQEDC-ASI-TABIBAN
// length: 3
// - name: TBYMQEDC-TBL-TABBIC
// occurs: 3
// fields:
// - name: TBYMQEDC-ASI-TABBIC
// length: 7
// - name: TBYMQSDC-LOCAL-GLOBAL-PRU
// fields:
// - name: TBYMQSDC-CONTRATO-G
// fields:
// - name: TBYMQSDC-IDEMPR-G
// length: 4
// - name: TBYMQSDC-IDCENT-G
// length: 4
// - name: TBYMQSDC-IDPROD-G
// length: 3
// - name: TBYMQSDC-NUMCTRC-G
// length: 7
// - name: TBYMQSDC-CONTRATO-L
// fields:
// - name: TBYMQSDC-IDEMPR-L
// length: 4
// - name: TBYMQSDC-IDCENT-L
// length: 4
// - name: TBYMQSDC-IDPROD-L
// length: 3
// - name: TBYMQSDC-NUMCTRC-L
// length: 7
// - name: TBYMQSDC-ACCION
// length: 1
// - name: TBYMQSDC-SUBACCION
// length: 1
// - name: TBYMQSDC-CODRES
// length: 2
// - name: TBYMQSDC-DESCRIPCION
// length: 12
// - name: FILLER
// length: 18

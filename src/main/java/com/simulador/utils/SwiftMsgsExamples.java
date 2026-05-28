package com.simulador.utils;

public class SwiftMsgsExamples {

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
		
		String[] res = new String[1];

		res[0] = "  PMCP2BRC70                 N        BSCHGB2L XXXIRVTGB2X XXX103N   NNESPMC0000000126062STP        eba4b30a-ad78-492f-b335-3837cdc5700b                                                                                                                                                                                                            00000GUFA02802596.001                 CREDSDVA                                                                                                                                                                                                                                                                                                                                                   GBP0000000002942282022-04-04GBP000000000294228000000000000                                                                   SUCURSAL LONDRES                                                      00000                                                                 0                                 0                                 0                                 0                                                                                                                                                                                                                                      BSCHGB2LXXX 14601                                                                                                                                                                                                            MGTCBEBEXXX                                                                                                                                                                                                                  IRVTUS3NIBK                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         IRVTGB2XXXX1287278260                                                                                                                                                                    IRVTUS3NIBK0                                 0                                 0                                 0                                                                                                                                                                             SHA   000000000000000   000000000000000   000000000000000   000000000000000   000000000000000   000000000000000   000000000000000   000000000000000   000000000000000   000000000000000   000000000000000BNF     CCLAIM GB00BDX8CX86 35672";

		return res;
	}

}

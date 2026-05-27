package com.simulador.utils;

public class PASMsgsExamples {

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
		
		String[] res = new String[17];
		
		res[0] = DumpRead.readMsgS("pas-pcas-1");
		res[1] = DumpRead.readMsgS("pas-pcas-2");
		res[2] = "";
		res[3] = DumpRead.readMsgS("pas-pcas-4");
		res[4] = DumpRead.readMsgS("pas-pcas-5");
		res[5] = DumpRead.readMsgS("pcas-pas-soh");
		res[6] = DumpRead.readMsgS("pcas-pas-soh2");
		res[7] = DumpRead.readMsgS("pcas-pas-soh3");
		res[8] = DumpRead.readMsgS("pcas-pas-soh4");
		res[9] = DumpRead.readMsgS("pcas-pas-soh5");
		res[10] = DumpRead.readMsgS("pcas-pas-soh-saldospcas1");
		res[11] = DumpRead.readMsgS("pcas-pas-soh-saldospcas2");
		res[12] = DumpRead.readMsgS("pcas-pas-soh-ctaspcas1");
		res[13] = DumpRead.readMsgS("pcas-pas-soh-ctaspcas2");
		res[14] = DumpRead.readMsgS("pcas-pas-soh-titularsat1");
		res[15] = DumpRead.readMsgS("pcas-pas-soh-titularsat2");
		res[16] = DumpRead.readMsgS("pas-prc-1");
		
		return res;
	}

}

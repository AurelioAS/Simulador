package com.simulador.utils;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.codec.binary.Hex;

public class DumpRead {

	public static byte[] readMsg(String name) throws Exception {
		Path f = search(name);
		List<String> lines = Files.readAllLines(f, StandardCharsets.ISO_8859_1);
		String hex = lines.stream()
			.filter(line -> line.matches("^\\d+:.*"))  // lineas que empiezan por digitos y dos puntos ':'
			.map(line -> line.split(":")[1].split("\\|")[0].trim().replace(" ", ""))  // me quedo con el medio y quito blancos
			.collect(Collectors.joining());  // uno todas las lines
		return Hex.decodeHex(hex);
	}
	
	public static String readMsgS(String name) throws Exception {
		return new String(readMsg(name), StandardCharsets.ISO_8859_1);
	}
	
    public static String readMsgS(String name, Charset cs) throws Exception {
      return new String(readMsg(name), cs);
  }
  
	private static Path search(String name) throws IOException {
		String name2 = name+".txt";
		return Files.find(Paths.get("."), 10, (p, basicFileAttributes) -> {
	        if (Files.isDirectory(p) || !Files.isReadable(p))
	            return false;
	        return p.getFileName().toString().equalsIgnoreCase(name2);
	    }).findFirst().get();
	}

	public static void main(String[] args) {
		String line = "000200:  20 20 20 20 2a 2a 2a 2a 2a 2a 2a 2a 4d 51 47 52 50 41 53 54   |     ********MQGRPAST |";
		String s = line.split(":")[1].split("\\|")[0]; //[1].trim().replace(" ", "");
		System.out.println(s);
	}
}

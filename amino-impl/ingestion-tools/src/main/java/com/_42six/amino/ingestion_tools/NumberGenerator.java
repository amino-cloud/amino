package com._42six.amino.ingestion_tools;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class NumberGenerator {

	private static final int OneMillion = 1000000;
	private static final int TenMillion = 10000000;
	private static final int OneThousand = 1000;

	public static void main(String[] args) throws IOException {
		createFile("numbers-1m.txt", OneMillion);
		//createFile("numbers-10m.txt", TenMillion);
		createFile("numbers-1k.txt", OneThousand);
	}
	
	private static void createFile(String fileName, int count) throws IOException {
		File f = new File("src/main/resources/data/" + fileName);

		BufferedWriter out = new BufferedWriter(new FileWriter(f));
		for (int i = 1; i <= count; ++i) {
			out.write(String.valueOf(i));
			if (i != count) {
				out.write("\r\n");
			}
		}
		out.close();
	}

}

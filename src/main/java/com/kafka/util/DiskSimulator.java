package com.kafka.util;

import java.util.HashMap;
import java.util.Map;

public class DiskSimulator {
	private static Map<String, String> files;

	static {
		files = new HashMap<>();
	}

	public static void store(String filePath, String data) {
		if (files.containsKey(filePath)) {
			throw new RuntimeException("File already exists. File path: " + filePath);
		}
		files.put(filePath, data);
	}

	public static String read(String filePath) {
		if (!files.containsKey(filePath)) {
			throw new RuntimeException("File does not exist. File path: " + filePath);
		}
		return files.get(filePath);
	}

	public static void remove(String filePath) {
		if (!files.containsKey(filePath)) {
			throw new RuntimeException("File does not exist. File path: " + filePath);
		}
		files.remove(filePath);
	}
}

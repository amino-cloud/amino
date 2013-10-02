package com._42six.amino.common;

import org.apache.hadoop.io.Text;

public class TextUtils {
	public static byte [] getBytes(Text text) {
		byte [] bytes = text.getBytes();
		if(bytes.length != text.getLength()) {
			bytes = new byte[text.getLength()];
			System.arraycopy(text.getBytes(), 0, bytes, 0, bytes.length);
		}
		return bytes;
	}
}

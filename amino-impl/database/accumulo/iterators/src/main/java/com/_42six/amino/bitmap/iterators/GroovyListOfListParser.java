package com._42six.amino.bitmap.iterators;

import com._42six.amino.common.bitmap.AminoBitmap;

import java.util.ArrayList;
import java.util.SortedSet;
import java.util.TreeSet;

public class GroovyListOfListParser {
	public static AminoBitmap[] createBitmap(String listOfLists) {
		if (listOfLists == null || listOfLists.isEmpty()) {
			throw new IllegalArgumentException(
					"The list of lists can not be null!");
		}
		// normalize our string
		listOfLists = nomralizeListOfListString(listOfLists);
		// TODO: Add error checks
		ArrayList<AminoBitmap> bitmaps = new ArrayList<>();
		int openParenCount = 0;
		String currentCSV = "";
		for (int i = 0; i < listOfLists.length(); i++) {
			switch (listOfLists.charAt(i)) {
			case '[':
				openParenCount++;
				currentCSV = "";
				break;
			case ']':
				openParenCount--;
				if (!currentCSV.isEmpty()) {
					AminoBitmap aminoBitmap = createBitmapFromString(currentCSV);
					bitmaps.add(aminoBitmap);
					currentCSV = "";
				}
				break;
			default:
				currentCSV += listOfLists.charAt(i);
				break;
			}
		}

		if (openParenCount > 0) {
			throw new IllegalArgumentException(
					"There is a stray parenthesies" );
		}

		return bitmaps.toArray(new AminoBitmap[bitmaps.size()]);
	}

	private static AminoBitmap createBitmapFromString(String bitCSV) {

		AminoBitmap retBitmap = null;

		String[] bits = bitCSV.split(",");
		SortedSet<Integer> bitset = new TreeSet<>();

		for (String bit : bits) {
			Integer intBit = Integer.parseInt(bit.trim());
			bitset.add(intBit);
		}
		if (!bitset.isEmpty()) {
			retBitmap = new AminoBitmap();
			for (Integer bit : bitset) {
				retBitmap.set(bit);
			}
		}

		return retBitmap;
	}
	
	private static String nomralizeListOfListString(String listOfLists) {
		return listOfLists.replaceAll("\\s+", "");
	}
}

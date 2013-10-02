package com._42six.amino.bitmap.ewahtester;

import java.math.BigInteger;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import com.googlecode.javaewah.EWAHCompressedBitmap;


public class EwahTester {
	/* Since our times are coming back as longs, a BigInteger is needed to keep track of the average sums */
	static BigInteger trueTimesSum;
	static BigInteger falseTimesSum;
	
	/**
	 * Check to see if a bitmap has the amount of number of bits set with the mask
	 * @param masterBitmap to check against
	 * @param testBitmap to check with
	 * @param numberOfBits that should be set in the testBitmap after anding it with the master bitmap
	 * @return true or false if the testBitmap AND the masterBitmap have the appropriate number of bits set
	 */
	public static boolean matches(EWAHCompressedBitmap masterBitmap, EWAHCompressedBitmap testBitmap, int numberOfBits) {
		return testBitmap.andCardinality(masterBitmap) == numberOfBits;
	}
	
	/**
	 * Create a bitmap with the positions from the list set
	 * @param positions in the bitmap that should be set to true
	 * @return the newly created bitmap
	 */
	public static EWAHCompressedBitmap createBitmap(List<Integer> positions) {
		EWAHCompressedBitmap bmp = new EWAHCompressedBitmap();
		for(int bit : positions) {
			bmp.set(bit);
		}
		return bmp;
	}
	
	/**
	 * Create a bitmap for the given long by setting the appropriate bits
	 * @param number used to create the bitmap for
	 * @return the new bitmap for the number
	 */
	public static EWAHCompressedBitmap createBitmap(long number) {
		EWAHCompressedBitmap bmp = new EWAHCompressedBitmap();
		bmp.add(number);
		return bmp;
	}
	
	
	/**
	 * Create a 'random' bitmap by setting random bits in the bitmap
	 * @param numberOfBitsToSet the number of bits that at most can be set
	 * @param highestPossibleBit the highest possible bit that can be set
	 * @param r
	 * @return
	 */
	public static EWAHCompressedBitmap createRandomBitmap(int numberOfBitsToSet, int highestPossibleBit, Random r) {
		Set<Integer> positionsSet = new HashSet<Integer>();
		for(int i=0;i < numberOfBitsToSet; i++) {
			positionsSet.add(r.nextInt(highestPossibleBit));
		}

		LinkedList<Integer> positions = new LinkedList<Integer>(positionsSet);
		// EWAH does not play friendly with creating the bitmaps if they are not in sorted order (ascending)
		Collections.sort(positions);
		return createBitmap(positions);
	}
	
	/**
	 * This method will return a subset or a mask, given a bitmap it will pick a random set of bits up to numberOfBits and will create a suitable mask, so that when you 'AND' it, you will get a positive result.
	 * @param seed bitmap used to create a mask for
	 * @param numberOfBits the number of bits total needed for the mask
	 * @param r the random number generator used
	 * @return a new 'mask' bitmap
	 * @throws Exception if the number of bits is greater then the cardinality, because then we do not have the appropriate amount to create a mask for
	 */
	public static EWAHCompressedBitmap getMatchingBitmap(EWAHCompressedBitmap seed, int numberOfBits, Random r) throws Exception {
		if(numberOfBits > seed.cardinality()) {
			throw new IllegalArgumentException("The matching Bitmap can only have up to the same amount of cardinality as the seed bitmap!");
		}
	
		List<Integer> positions = seed.getPositions(); // get the integer indexes of the set bits in the bitmap
		final int listSize = positions.size();
		int randomIndex;
		do {
			randomIndex = r.nextInt(listSize);
		} while((randomIndex + numberOfBits) > listSize);

		// Now set all the bits for the new bitmap
		EWAHCompressedBitmap retVal = new EWAHCompressedBitmap();
		for(int i=randomIndex; i < randomIndex + numberOfBits; i++) {
			retVal.set(positions.get(i));
		}
		return retVal;
	}
	
	
	/**
	 * Execute the test and keep track of the number of nano second that it took for the operation to complete
	 * @param mask to use to check for the bits that are set
	 * @param test the value to test against
	 * @param numberOfBits the number of bits to see that are set
	 */
	public static void executeAndProfileTest(EWAHCompressedBitmap mask, EWAHCompressedBitmap test, int numberOfBits) {
		 long startTime = System.nanoTime();
		 boolean matched = matches(mask, test,numberOfBits);
		 long result = System.nanoTime() - startTime;
		 if(matched) {
			trueTimesSum = trueTimesSum.add(new BigInteger(Long.toString(result)));
		 } else {
			 falseTimesSum = falseTimesSum.add(new BigInteger(Long.toString(result)));
		 }
	}
	
	/**
	 * Run the actual tests
	 * @param r the random number generator to use
	 * @param maxNumberOfBitsToSet in the random bitmaps that are created
	 * @param highestBitToBeSet in the random bitmaps that are created
	 * @param numberOfTestsToRun to for this particual harness
	 * @throws Exception in case there are problems with the BigIntegers and for illegal arguments
	 */
	public static void runTests(Random r, int maxNumberOfBitsToSet, int highestBitToBeSet, int numberOfTestsToRun) throws Exception {
		// Reset our Counters
		trueTimesSum = BigInteger.ZERO;
		falseTimesSum = BigInteger.ZERO;
		
 		EWAHCompressedBitmap mask = createRandomBitmap(maxNumberOfBitsToSet, highestBitToBeSet, r);
		EWAHCompressedBitmap test;
		if(numberOfTestsToRun % 2 != 0) {
			throw new IllegalArgumentException("The number of tests should be divisible by 2!");
		}
		
		int numberOfBits = r.nextInt(mask.cardinality());
		while(numberOfBits < 1)
			numberOfBits++;
		
		for(int i=0;i<numberOfTestsToRun;i++) {
			if(i%2 ==0) {
				test = getMatchingBitmap(mask, numberOfBits, r);
			} else {
				test = createRandomBitmap(maxNumberOfBitsToSet, highestBitToBeSet, r);
			}
			executeAndProfileTest(mask, test, numberOfBits);
		}
		
		int halfNumberOfTestsToRun = numberOfTestsToRun / 2;
		System.out.println("True times avg (ns): " + trueTimesSum.divide(new BigInteger(Integer.toString(halfNumberOfTestsToRun))));
		System.out.println("False times avg (ns): " + falseTimesSum.divide(new BigInteger(Integer.toString(halfNumberOfTestsToRun))));
	}
	
	
	
	public static void main(String [] args) throws Exception {
		Random r = new Random();
		// small tests
		System.out.println("Small tests maximum of 30 bits set with the highest bit being set as bit 40");
		runTests(r, 30, 40, 100);
		
		// large tests
		System.out.println("Small tests maximum of 100 bits set with the highest bit being set as bit 250");
		runTests(r, 100, 250, 100);
	}
}

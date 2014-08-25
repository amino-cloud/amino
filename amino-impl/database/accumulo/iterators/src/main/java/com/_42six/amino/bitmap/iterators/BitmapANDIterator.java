package com._42six.amino.bitmap.iterators;

import com._42six.amino.common.bitmap.AminoBitmap;
import com._42six.amino.common.bitmap.BitmapUtils;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.accumulo.core.data.*;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

/**
 *  This iterator will loop through the entries in the Bitmap_byBucket table.  The entries in that table should look like:
 *  Row ID                                          Column Family  Column Qualifier     Value
 *  Shard_Number:Data Source:Bucket Name            Bucket Value   Hash Salt            Amino Bitmap (Feature Facet Value Index)
 *  
 */
public class BitmapANDIterator extends WrappingIterator implements OptionDescriber {

  static final long serialVersionUID = 42L;

  public static final Logger log = Logger.getLogger(BitmapANDIterator.class);
  
  public static final String OPTION_BITS = "bits";
  
  private ArrayList<CompareBits> bitsPerHash = null;
  
  private Key topKey = null;
  final Value topValue = new Value(new byte[0]);
 
  /**
   * Return the current matching key
   */
  @Override
  public Key getTopKey() {
    return this.topKey;
  }

  /*
   * (non-Javadoc)
   * @see org.apache.accumulo.core.iterators.WrappingIterator#getTopValue()
   */
  @Override
  public Value getTopValue() {
	  // We do not really care about the value
	  return topValue;
  }

  /**
   * Check to see if there is something more to this iterator, 
   * if we have a topKey define or the source iterator has more then we do
   */
  @Override
  public boolean hasTop() {
	return this.topKey != null || super.hasTop();
  }

  /**
   * Initialize the iterator, with another iterator
   */
  @Override
  public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
    this.setSource(source);
    this.init(options);
  }
  
  /**
   *  Setup the iterator based on the user options
   * @param options map of user given options which should just be 
   *                OPTION_BITS (a list of lists) and 
   *                the OPTION_TARGET (number of bits to match)
   * 
   */
  private void init(Map<String, String> options) {
      if(this.validateOptions(options) ){
		  String bits = options.get(OPTION_BITS);
          bitsPerHash = new Gson().fromJson(bits, new TypeToken<ArrayList<CompareBits>>(){}.getType());
    } else {
    	throw new IllegalArgumentException("All iterator options not set!\n" + this.describeOptions());
    }
  }
  
  /**
   * Check to see if has the appropriate number of bits for a bitmap
   * @param golden the master bitmap to check against
   * @param testValue the value form the database to check against
   * @return false if the bits don't match the golden, true otherwise
   */
  public boolean filter(CompareBits golden, Value testValue) {
      AminoBitmap testBitmap = BitmapUtils.fromValue(testValue.get());

      // Check all of the non-Range features and make sure they are all present
      if(testBitmap.andCardiniality(golden.getNonRangeBitmap()) != golden.getNonRangeCardinality()){
          return false;
      }

      // For each of the range features, make sure that at least one of them was set
      for(AminoBitmap bitmap : golden.rangeBitmaps){
          if(testBitmap.andCardiniality(bitmap) < 1){
              return false;
          }
      }

      return true;
  }
  
 /**
  * Describe the options that the user can define for the iterator.  Very useful from the accumulo shell
  *
  * @return a object representing the options that are available for the user to specify on the 
  *         iterator
  */
  @Override
  public IteratorOptions describeOptions() {
    String iterName = "Amino Bitmap AND Iterator";
    String iterDesc = "Generates a bitmap with the bits defined in the option set. It filters bitmap values that do not have all the bits set.";
    Map<String,String> optionMap = new HashMap<>();
    optionMap.put(OPTION_BITS, "List of ConfigBits per hash to check against");
    return new IteratorOptions(iterName, iterDesc, optionMap, null);
  }

  /**
   * Make sure that the user has set both the OPTION_BITS and the OPTION_TARGET
   */
  @Override
  public boolean validateOptions(Map<String, String> options) {
    return options.containsKey(OPTION_BITS);
  }

  /**
   * We will not allow the user to deep copy the itearator
   */
  @Override
  public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
	  throw new UnsupportedOperationException("Deep Copy is not allowed with this iterator");
  }
  
  /**
   * Validate that we are in the same row and column family pair
   * @param testKey is the key that we are going to use to test against
   * @param expectedRow is the expected row we want to be in
   * @param expectedColumnFamily is the expected column family that we want to be in
   * @return true or false if we are in both that row and column family
   */
  private boolean sameRowAndColumnFamily(Key testKey, Text expectedRow, Text expectedColumnFamily) {
	 return (testKey.getRow().equals(expectedRow)) && (testKey.getColumnFamily().equals(expectedColumnFamily));
  }
  
  
  /**
   * This method will get the next matching key.
   * The algorithm is as follows:
   * while the iterator has a key to evaulate
   * gather the row, column family information
   *   foreach hash in the row, column family combination matching the row and column family we just gathered
   *      verify that the bitmap in the values has the appropriate number of bits matching
   *      if it does not
   *        set the number of matching hash seeds to -1 and break out of the loop
   *      if it does
   *        increment the number of matching hash seeds
   *        move to the next key, column family, column qualifier
   *   if we have the same number of matching hash seeds for how long the iterator is then set the key and return from the function
   *   else check to see if there are more keys in the source iterator, and if so iterate to the next key
   *     
   */
  private void getNextKey() throws IOException {
	  if(topKey != null)
		  return;
	  
	  SortedKeyValueIterator<Key, Value> sourceIter = getSource();
	 
	  // We have exhausted all the keys in the source iterator
	  if(!sourceIter.hasTop())
		  return;
	  
	  int numberOfMatchingHashSeeds;
	  while(sourceIter.hasTop()) {
		  // Get the row information
		  final Key compareKey = sourceIter.getTopKey();
		  final Text compareRow = compareKey.getRow();
		  final Text compareColumnFamily = compareKey.getColumnFamily();
		  numberOfMatchingHashSeeds = 0;
		  // loop through the source iterator while the row key and column family match
		  while(sourceIter.hasTop() && sameRowAndColumnFamily(sourceIter.getTopKey(), compareRow, compareColumnFamily)) {
			  int hashSeed = Integer.parseInt(sourceIter.getTopKey().getColumnQualifier().toString());
			  // if our hash seed is greater then our masterBitmap break the loop or this value does not match then break the loop
			  if(hashSeed >= bitsPerHash.size() || !filter(bitsPerHash.get(hashSeed), sourceIter.getTopValue())) {
				  log.debug("Failed to match using key: " + sourceIter.getTopKey().toStringNoTime());
				  numberOfMatchingHashSeeds = -1;
				  break;
			  }
			  ++numberOfMatchingHashSeeds;
			  sourceIter.next();
		  }
		  
		  // We found a match, so lets send it back
		  if(numberOfMatchingHashSeeds == bitsPerHash.size()) {
			  log.debug("Found a match for: " + compareKey.toStringNoTime());
			  topKey = compareKey;
			  return;
		  } else {
			  log.debug(sourceIter.getTopKey().toStringNoTime() + " matched " + numberOfMatchingHashSeeds + " out of " + bitsPerHash.size() + " hash seeds!");
		  }
		
		  // we have exhuasted all the keys
		  if(!sourceIter.hasTop()) {
			  return;
		  }
		  sourceIter.next();
	  }
  }
  
  /**
   * Get the next key
   */
  @Override
  public void next() throws IOException {
	topKey = null;
	getNextKey();
  }

	/**
	 * Assume that we were given a previous key and seek to the next 'row' that matches
	 * @param range is the range that we want to seek to
	 * @param columnFamilies the columnFamilies that we want to use
	 * @param inclusive whether or not we are inclusive
	 */
    @Override
	public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
		topKey = null;
		Key startKey = range.getStartKey();
		if(startKey != null && startKey.getColumnFamilyData().length() == 0 &&
		   startKey.getColumnQualifierData().length() == 0 &&
		   startKey.getColumnVisibilityData().length() == 0 &&
		   startKey.getTimestamp() == Long.MAX_VALUE && 
		   !range.isStartKeyInclusive()) {
			Key followingKey = startKey.followingKey(PartialKey.ROW);
			if(range.getEndKey() != null && followingKey.compareTo(range.getEndKey()) > 0)
				return;
			range = new Range(startKey.followingKey(PartialKey.ROW), true, range.getEndKey(), range.isEndKeyInclusive());
		}
		super.seek(range, columnFamilies, inclusive);
		getNextKey();
	}

    /**
     * Class containing all of the bits that we need to check against.
     */
    public static class CompareBits{
        AminoBitmap nonRangeBitmap;
        int nonRangeCardinality;
        List<AminoBitmap> rangeBitmaps;

        public CompareBits() {
            nonRangeBitmap = new AminoBitmap();
            nonRangeCardinality = 0;
            rangeBitmaps = new ArrayList<>();
        }

        public CompareBits(AminoBitmap nonRangeBitmap, int nonRangeCardinality, List<AminoBitmap> rangeBitmaps){
            this.nonRangeBitmap = nonRangeBitmap;
            this.nonRangeCardinality = nonRangeCardinality;
            this.rangeBitmaps = rangeBitmaps;
        }

        public void incrementNonRangeCardinality(){
            this.nonRangeCardinality += 1;
        }

        public AminoBitmap getNonRangeBitmap() {
            return nonRangeBitmap;
        }

        public void setNonRangeBitmap(AminoBitmap nonRangeBitmap) {
            this.nonRangeBitmap = nonRangeBitmap;
        }

        public int getNonRangeCardinality() {
            return nonRangeCardinality;
        }

        public void setNonRangeCardinality(int nonRangeCardinality) {
            this.nonRangeCardinality = nonRangeCardinality;
        }

        public List<AminoBitmap> getRangeBitmaps() {
            return rangeBitmaps;
        }

        public void setRangeBitmaps(ArrayList<AminoBitmap> rangeBitmaps) {
            this.rangeBitmaps = rangeBitmaps;
        }

    }

}

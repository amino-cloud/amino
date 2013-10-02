package com._42six.amino.bitmap.iterators;

import com.google.common.collect.HashMultiset;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 *  This iterator loops through the amino_reverse_feature_lookup table and finds bucket values.  It keeps track of how
 *  many values have been seen for a particular value, and once it hits the salt number of values, we know we have a
 *  legitimate number of results and the value can be emitted.
 *
 *  amino_reverse_feature_lookup
 *  Row ID            Column Family                               Column Qualifier  |  Value
 *  -------------------------------------------------------------------------------------------
 *  Shard_Number      Index Position#Datasource#BucketName#salt   BucketValue       | <empty>
 *  
 */
public class ReverseFeatureCombiner extends WrappingIterator implements OptionDescriber {

    public static final Logger log = Logger.getLogger(ReverseFeatureCombiner.class);

    /** The option to be passed in declaring how many salts are in use. */
    public static final String OPTION_SALTS = "num_salts";

    /** Option to be passed in to limit the number of results that can be returned */
    public static final String OPTION_MAX_RESULTS = "max_results";

    // Keeps track of the count of values that have been seen
    private final HashMultiset<String> seenValues = HashMultiset.create();

    private int numSalts;
    private int resultsReturned = 0;
    private int maxResults;

    private static final Value INVALID_VALUE = new Value(new byte[0]);
    private static final Key BLANK_KEY = new Key();

    private Value topValue = INVALID_VALUE;

//    /*
//    * (non-Javadoc)
//    * @see accumulo.core.iterators.WrappingIterator#getTopValue()
//    */
    @Override
    public Value getTopValue() {
        //log.error("Sending back topValue: " + topValue.toString());
        final Value v = topValue;
        topValue = INVALID_VALUE;
        return v;
    }

    @Override
    public Key getTopKey(){
        return BLANK_KEY;
    }

    @Override
    public boolean hasTop(){
        return (resultsReturned <= maxResults) && ((topValue.compareTo(INVALID_VALUE) != 0) || super.hasTop());
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
    *                OPTION_SALTS - The number of salts that are in use
     *               OPTION_MAX_RESULTS - The maximum number of results to return before giving up.  Default 4000.  Any
     *               non-positive value means no max.
    */
    private void init(Map<String, String> options) {
        if(this.validateOptions(options) ){
            // Check to see if the number of salts were set.  If not, assume 1.
            if(options.containsKey(OPTION_SALTS)){
                numSalts = Integer.parseInt(options.get(OPTION_SALTS));
            } else {
                numSalts = 1;
            }

            // See if the max results was set. If not, set to 4000. Anything 0 or less means no max results.
            if(options.containsKey(OPTION_MAX_RESULTS)){
                maxResults = Integer.valueOf(options.get(OPTION_MAX_RESULTS));
                if(maxResults <= 0){
                    maxResults = Integer.MIN_VALUE;
                }
            } else {
                maxResults = 4000;
            }
        } else {
            throw new IllegalArgumentException("Invalid options for iterator\n" + this.describeOptions());
        }
    }

    /**
    * Describe the options that the user can define for the iterator.  Very useful from the accumulo shell
    *
    * @return a object representing the options that are available for the user to specify on the
    *         iterator
    */
    @Override
    public IteratorOptions describeOptions() {
        String iterName = "Amino Reverse Feature Combiner Iterator";
        String iterDesc = "Looks up bucket values for the reverse job, deconflicting any collisions that might have occurred";
        Map<String,String> optionMap = new HashMap<String,String>();
        optionMap.put(OPTION_SALTS, "Number of salts in use.  Default is one");
        optionMap.put(OPTION_MAX_RESULTS, "Maximum number of results to return");

        return new IteratorOptions(iterName, iterDesc, optionMap, null);
    }

    /**
    * Make sure any options passed in contain valid values
    */
    @Override
    public boolean validateOptions(Map<String, String> options) {
        if(options.containsKey(OPTION_SALTS)){
            try{
                int salts = Integer.parseInt(options.get(OPTION_SALTS));
                if (salts <= 0){
                    return false;
                }
            } catch(NumberFormatException ex) {
                return false;
            }
        }

        if(options.containsKey(OPTION_MAX_RESULTS)){
            try{
                Integer.parseInt(options.get(OPTION_MAX_RESULTS));
            } catch (NumberFormatException ex){
                return false;
            }
        }

        return true;
    }

    /**
    * We will not allow the user to deep copy the iterator
    */
    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
        throw new UnsupportedOperationException("Deep Copy is not allowed with this iterator");
    }


    /**
    * Get the next key
    */
    @Override
    public void next() throws IOException {
        final SortedKeyValueIterator<Key, Value> sourceIter = getSource();

        while(sourceIter.hasTop()){
            final Key currentKey = sourceIter.getTopKey();
            final String value = currentKey.getColumnQualifier().toString();

            sourceIter.next();

            // Log the value and check if it should be emitted.  If we haven't hit the max or salt number of values,
            // then return an INVALID_KEY to signify that the value is an intermediate result.
            seenValues.add(value);
            final int count = seenValues.count(value);

            //log.error(String.format("Added value %s with count %d", value, count));

            if(count == numSalts){
                // Got a valid value.  Increment the counters and then "emit" the value
                topValue = new Value(value.getBytes());
                resultsReturned++;
                //log.error("Returning topvalue => " + topValue.toString());
                return;
            } else if(count > numSalts){
                // This should never happen.  It either means you passed in the wrong numSalts or there was a problem
                // with the hashing algorithms
                throw new IOException("The value '" + value + "' shows up in the table more than numSalts times");
            } else {
                // Haven't gotten enough values to make a conclusive result.  If this is a BatchScanner, then emit an
                // INVALID_VALUE so that the caller knows this is an intermediate value.
                topValue = INVALID_VALUE;
                //log.error("count wasn't enough");
            }
        }
    }

    /**
     * Assume that we were given a previous key and seek to the next 'row' that matches
     * @param range is the range that we want to seek to
     * @param columnFamilies the columnFamilies that we want to use
     * @param inclusive whether or not we are inclusive
     */
    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        getSource().seek(range, columnFamilies, inclusive);
        next();
    }
}

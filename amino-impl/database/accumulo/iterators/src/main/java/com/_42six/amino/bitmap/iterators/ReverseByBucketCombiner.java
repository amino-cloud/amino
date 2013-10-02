package com._42six.amino.bitmap.iterators;

import com._42six.amino.common.bitmap.AminoBitmap;
import com._42six.amino.common.bitmap.BitmapUtils;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.googlecode.javaewah.EWAHCompressedBitmap;
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
 *  This iterator loops through the amino_reverse_bitmap_byBucket_numbers table, combining bitmaps of feature values
 *  for a given shard:salt rowID.  It assumes that the table is the following schema:
 *
 *  amino_reverse_bitmap_byBucket
 *  Row ID            Column Family                       Column Qualifier   |    Value
 *  -------------------------------------------------------------------------------------------------------------------
 *  Shard:Salt      DatasourceId#BucketName#FeatureID     FeatureValue       |    AminoBitmap of matching bucket values
 *
 *  Per shard:salt, this will AND together all of the nominal values that it finds, in addition to AND'ing together the
 *  OR result of ratio features.  If a similar ratio is passed in (i.e. Has Digit 3-5 and Has Digit 7-9) that will be
 *  treated as one feature of (Has Digit 3-5 OR 7-9) before being AND'ed with the other features.
 */
public class ReverseByBucketCombiner extends WrappingIterator implements OptionDescriber {

    public static final Logger log = Logger.getLogger(ReverseByBucketCombiner.class);

    public static final Key INVALID_KEY = new Key("INVALID", "", "", 0);

    // OPTIONS

    /** The option to pass in to signify the feature IDs to AND together.  This should be an array of nominal IDs */
    public static final String OPTION_AND_IDS = "and_ids";

    /** The option to pass in to signify the feature IDs to OR together (within themselves).  This should be an array of ratio IDs */
    public static final String OPTION_OR_IDS = "or_ids";

    /** The option that signifies how many Ranges are being iterated over */
    public static final String OPTION_NUM_RANGES = "num_ranges";

    public static final String OPTION_BITMAP_MEM_THRESHOLD = "max_bitmap_memory_bytes";

    // PRIVATE VARS
    private int numberOfRanges = 0; // The number of Ranges being compared.
    private int rangesCounted = 0; // Keeps track of how many times getNext() was called
    private Text currentRow = new Text(); // The row that we are currently iterating over

    private long bitmapMemoryThreshold = 100 * 1024 * 1024; // Amount of memory that we want to allocate towards
    // holding bitmaps in memory.  Default to 100MB

    // The cf/cq pairs of features to be AND'ed
    private Set<AbstractMap.SimpleImmutableEntry<String, String>> andIDs = new HashSet<AbstractMap.SimpleImmutableEntry<String, String>>();

    // The cf of features to be OR'd
    private Set<String> orIds = new HashSet<String>();

    // To keep track of which features we actually got back to make sure we got back values of every feature type
    private HashMap<AbstractMap.SimpleImmutableEntry<String, String>, Boolean> andFeatureIds = new HashMap<AbstractMap.SimpleImmutableEntry<String, String>, Boolean>();
    private HashMap<String, Boolean> orFeatureIds = new HashMap<String, Boolean>();


    private Key topKey = null;
    private Value topValue = BitmapUtils.toValue(new AminoBitmap());
    AminoBitmap topValueBitmap = null;
 
    /**
    * Return the current matching key
    */
    @Override
    public Key getTopKey() {
        return (rangesCounted < numberOfRanges) ? INVALID_KEY : this.topKey;
    }

    /**
    * Returns the value as an AminoBitmap
    */
    @Override
    public Value getTopValue() {
        return this.topValue;
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
    * Setup the iterator based on the user options
    * @param options map of user given options which consists of:
    *                OPTION_AND_IDS - JSON String array of AbstractMap.SimpleImmutableEntry<String, String> cf/cq's
    *                whose values should be AND'ed together
    *                OPTION_OR_IDS  - JSON String array of AbstractMap.SimpleImmutableEntry<String, String> cf/cq's
    *                whose values first need to be OR'd before being AND'd together with other features
    *                OPTION_NUM_RANGES - The number of Ranges being iterated over
    *                OPTION_BITMAP_MEM_THRESHOLD - Amount of memory that is acceptible to use before having to "page" our bitmaps
    */
    private void init(Map<String, String> options) {
        if(!validateOptions(options)){
            throw new IllegalArgumentException("All iterator options not set!\n" + this.describeOptions());
        }

        final String ranges = options.get(OPTION_NUM_RANGES);
        numberOfRanges = Integer.parseInt(ranges);

        final Gson gson = new Gson();

        // Keep track of what types of feature IDs need to be AND'd
        final String ands = options.get(OPTION_AND_IDS);
        if(ands != null){
            andIDs = gson.fromJson(ands,new TypeToken<Set<AbstractMap.SimpleImmutableEntry<String, String>>>(){static final long serialVersionUID = 426L;}.getType());

            for(AbstractMap.SimpleImmutableEntry<String, String> entry : andIDs){
                andFeatureIds.put(entry, false);
            }
        }

        // Keep track of what types of feature IDs need to be OR'd
        final String ors = options.get(OPTION_OR_IDS);
        if(ors != null){
            orIds = gson.fromJson(ors,new TypeToken<Set<String>>(){static final long serialVersionUID = 426L;}.getType());

            // To make sure we see at least one
            for(String id: orIds){
                orFeatureIds.put(id, false);
            }
        }

        if(options.containsKey(OPTION_BITMAP_MEM_THRESHOLD)){
            bitmapMemoryThreshold = Long.parseLong(options.get(OPTION_BITMAP_MEM_THRESHOLD));

            // Size 0 means that we don't want to limit the bitmaps.  Set to MAX
            if(bitmapMemoryThreshold == 0){
                bitmapMemoryThreshold = Long.MAX_VALUE;
            }
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
        final String iterName = "Amino Reverse ByBucket Combiner Iterator";
        final String iterDesc = "Looks up bucket values for the reverse job based on requested features. ";
        final Map<String,String> optionMap = new HashMap<String,String>();

        optionMap.put(OPTION_AND_IDS, "Feature IDs that just need to be AND'ed together with other features");
        optionMap.put(OPTION_OR_IDS, "Feature IDs that need to first be OR togeter before AND'ing with other features");
        optionMap.put(OPTION_NUM_RANGES, "The number of Ranges that are being scanned over");
        optionMap.put(OPTION_BITMAP_MEM_THRESHOLD, "The threshold of memory to consume before the iterator starts paging the bitmaps for comparison");

        return new IteratorOptions(iterName, iterDesc, optionMap, null);
    }

    /**
    * Make sure that the user has set OPTION_SALTS and either OPTION_OR_IDS or OPTION_AND_IDS
    */
    @Override
    public boolean validateOptions(Map<String, String> options) {
        // Make sure the number of ranges were passed in and they they are a positive number
        if(!options.containsKey(OPTION_NUM_RANGES)){
            return false;
        } else {
            if(Integer.parseInt(options.get(OPTION_NUM_RANGES)) <= 0){
                return false;
            }
        }

        // Make sure we got at least an OR or AND feature
        if(!(options.containsKey(OPTION_OR_IDS) || options.containsKey(OPTION_AND_IDS))){
            return false;
        }

        // If the memory threshold was passed in, make sure it's non-negative.  0 means MAX
        if(options.containsKey(OPTION_BITMAP_MEM_THRESHOLD)){
            if(Integer.parseInt(options.get(OPTION_BITMAP_MEM_THRESHOLD)) < 0){
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

    public enum FeatureType{
        AND,
        OR
    }

    // Resets everything if we encounter a new rowID
    private void reset(){
        rangesCounted = 0;
        topValue = BitmapUtils.toValue(new AminoBitmap());
        topKey = INVALID_KEY;
        topValueBitmap = null;
    }

    /**
    * This method will combine the rows of a single rowID based on the features that were passed in.  There should be
     * only one key/value per RowID/iterator
    */
    private void getNextKey() throws IOException {

        final SortedKeyValueIterator<Key, Value> sourceIter = getSource();

        // We have exhausted all the keys in the source iterator
        if(!sourceIter.hasTop()){
            // Compute the topValue if this is the last Range
            computeTopValue();
            return;
        }

        // Keep track of how many Ranges we've combined
        rangesCounted++;

        Key compareKey = null;
        while(sourceIter.hasTop()) {
            // Get the row information
            compareKey = sourceIter.getTopKey();

            // Check to see if we've moved on to a new shard:salt
            if(currentRow.compareTo(compareKey.getRow()) != 0){
                reset();
                currentRow = compareKey.getRow();
                rangesCounted = 1; // TODO really need to fix this spaghetti logic and other parts of this function.
            }

            final Text compareColumnFamily = compareKey.getColumnFamily();
            final String cq = compareKey.getColumnQualifier().toString();
            FeatureType type;

            // Determine the type of the current feature and mark that we have seen the feature (this is needed, for example,
            // if the requester asks for Has Digit 4 and there were no values with a 4 and thus no row in the db)
            if(orIds.contains(compareColumnFamily.toString())){
                type = FeatureType.OR;
                orFeatureIds.put(compareColumnFamily.toString(), true);
            } else {
                final AbstractMap.SimpleImmutableEntry<String, String> e = new AbstractMap.SimpleImmutableEntry<String, String>(compareColumnFamily.toString(), cq);
                if(andIDs.contains(e)){
                    type = FeatureType.AND;
                    andFeatureIds.put(e, true);
                } else {
                    sourceIter.next();
                    throw new IOException("CF: " + compareColumnFamily.toString() + " | CQ: " + cq + " was not passed in as an option to the iterator");
                }
            }

            AminoBitmap runningBitmap = null; // The culmination to combine with the topValueBitmap
            long currentBytes = 0;
            final HashSet<EWAHCompressedBitmap> currentBitmaps = new HashSet<EWAHCompressedBitmap>();

            // Loop through all of the rows of the feature type
            while(sourceIter.hasTop() && compareColumnFamily.equals(sourceIter.getTopKey().getColumnFamily())){
                AminoBitmap currentBitmap = BitmapUtils.fromValue(sourceIter.getTopValue());
                if(type == FeatureType.AND){
                    // Since we are looping over just similar cf's, need to note when we got a new cq
                    andFeatureIds.put(new AbstractMap.SimpleImmutableEntry<String, String>(compareColumnFamily.toString(),
                            sourceIter.getTopKey().getColumnQualifier().toString()), true);

                    // Set topValueBitmap here if it's null.  Pre-initializing causes problems when trying to AND
                    // together bitmaps. You'd have to pre-initialize to a bitmap of all 1's, which would be a waste of
                    // time and space.
                    if(topValueBitmap == null){
                        topValueBitmap = currentBitmap;
                    } else {
                        topValueBitmap.AND(currentBitmap);
                    }
                } else {
                    currentBytes += currentBitmap.sizeInBytes();

                    // Must OR together values first before ANDing them with the topValueBitmap
                    if(runningBitmap == null){
                        runningBitmap = currentBitmap;
                    } else {
                        // Using the static "or" method can be MUCH faster than OR'ng individual bitmaps together.  Keep
                        // track of bitmaps until we hit a threshold and then or what we have together.
                        currentBitmaps.add(currentBitmap.getBitmap());

                        if(currentBytes >= bitmapMemoryThreshold){
                            currentBitmaps.add(runningBitmap.getBitmap());
                            runningBitmap.setBitmap(EWAHCompressedBitmap.or(currentBitmaps.toArray(new EWAHCompressedBitmap[currentBitmaps.size()])));

                            log.info("Hit memory threshold. Combining " + currentBitmaps.size() + " bitmaps");
                            // Get rid of the old references so that the memory can be gc'd if need be
                            currentBitmaps.clear();
                            currentBytes = 0;
                        }
                    }
                }

                sourceIter.next();
            }

            // Set the topKey so we know what shard/salt we were dealing with.  This could be passed in as an option instead
            topKey = compareKey;

            // The OR values needed to be combined first before doing the AND
            if(runningBitmap != null){
                // Add in any remaining OR values
                if(!currentBitmaps.isEmpty()){
                    currentBitmaps.add(runningBitmap.getBitmap());
                    runningBitmap.setBitmap(EWAHCompressedBitmap.or(currentBitmaps.toArray(new EWAHCompressedBitmap[currentBitmaps.size()])));

                    // Get rid of the old references so that the memory can be gc'd if need be
                    currentBitmaps.clear();
                }

                // Set topValueBitmap here if it's null.  Pre-initializing causes problems when trying to AND
                // together bitmaps. You'd have to pre-initialize to a bitmap of all 1's, which would be a waste of
                // time and space.
                if(topValueBitmap == null){
                    topValueBitmap = runningBitmap;
                } else {
                    topValueBitmap.AND(runningBitmap);
                }
            }
        }

        computeTopValue();
    }

    /**
     * The topKey and topValue will only be valid after we have checked every Range
     */
    private void computeTopValue(){
        if(rangesCounted >= numberOfRanges){
            // Check to make sure that every featureId was seen at least once.  If you don't do this then you won't know
            // in the above code that you never got any of the missing feature
            if(!(andFeatureIds.containsValue(false) || orFeatureIds.containsValue(false))){
                // Success!  Set the result Key/Value
                topValue = BitmapUtils.toValue(topValueBitmap);
            }
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
        final Key startKey = range.getStartKey();
        if(startKey != null && startKey.getColumnFamilyData().length() == 0 &&
            startKey.getColumnQualifierData().length() == 0 &&
            startKey.getColumnVisibilityData().length() == 0 &&
            startKey.getTimestamp() == Long.MAX_VALUE &&
            !range.isStartKeyInclusive())
        {
            final Key followingKey = startKey.followingKey(PartialKey.ROW);
            if(range.getEndKey() != null && followingKey.compareTo(range.getEndKey()) > 0){
                return;
            }
            range = new Range(startKey.followingKey(PartialKey.ROW), true, range.getEndKey(), range.isEndKeyInclusive());
        }
        getSource().seek(range, columnFamilies, inclusive);
        getNextKey();
    }
}

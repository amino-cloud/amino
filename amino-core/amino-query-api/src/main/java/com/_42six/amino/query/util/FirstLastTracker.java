package com._42six.amino.query.util;

import org.apache.hadoop.io.Text;

import java.util.HashMap;
import java.util.Map;

/**
 * A class for keeping track what the lexicographically first and last values are for a given entry
 */
public class FirstLastTracker {

    public enum StoreGoal {
        FIRST, LAST
    }

    /**
     * Updates the backing store with the key/vaue
     * @param key The index key to the backing store
     * @param value The value to associate with the key
     * @param goal Is the value related to being first or last
     */
    public void updateStore(String key, Text value, StoreGoal goal) {
        if (backingStore.get(key) == null) {
            backingStore.put(key, new HashMap<StoreGoal, Text>());
        }

        updateEntry(backingStore.get(key), value, goal);
    }

    public void updateEntry(Map<StoreGoal, Text> entry, Text value, StoreGoal goal) {
        if (entry.get(goal) == null) {
            entry.put(goal, value);
        }

        if (goal == StoreGoal.FIRST) {
            entry.put(goal, (entry.get(goal).compareTo(value) > 0) ? value : entry.get(goal));
        } else if(goal == StoreGoal.LAST){
            entry.put(goal, (entry.get(goal).compareTo(value) < 0) ? value : entry.get(goal));
        }
    }

    /**
     * @return the "Last" value that would come lexographically first
     */
    public String getEarliestLast() {
        return findLexographicalEndpointAcrossBackingStore(StoreGoal.LAST);
    }

    /**
     * @return the "First" value that would come lexographically last
     */
    public String getLatestFirst() {
        return findLexographicalEndpointAcrossBackingStore(StoreGoal.FIRST);
    }

    private String findLexographicalEndpointAcrossBackingStore(StoreGoal goal) {
        Text retVal = null;

        for(Map<StoreGoal, Text> it : backingStore.values()){
            final Text value = it.get(goal);
            if (retVal == null) {
                retVal = new Text(value);
            }

            if (goal == StoreGoal.FIRST) {
                retVal = (retVal.compareTo(value) > 0) ? retVal : value;
            } else if (goal == StoreGoal.LAST){
                retVal = (retVal.compareTo(value) < 0) ? retVal : value;
            }
        }

        return (retVal != null) ? retVal.toString() : null;
    }

    final Map<String, Map<StoreGoal, Text>> backingStore = new HashMap<>();
}

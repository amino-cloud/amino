package com._42six.amino.query.util;

import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

public class FirstLastTrackerIT {
    @Test
    public void updateEntryShouldLexographicallySortFirst() {
        FirstLastTracker tracker = new FirstLastTracker();
        LinkedHashMap<FirstLastTracker.StoreGoal, Text> entry = new LinkedHashMap<FirstLastTracker.StoreGoal, Text>(1);
        entry.put(FirstLastTracker.StoreGoal.FIRST, new Text("B"));
        tracker.updateEntry(entry, new Text("C"), FirstLastTracker.StoreGoal.FIRST);
        Assert.assertEquals("B", entry.get(FirstLastTracker.StoreGoal.FIRST).toString());
        tracker.updateEntry(entry, new Text("A"), FirstLastTracker.StoreGoal.FIRST);
        Assert.assertEquals("A", entry.get(FirstLastTracker.StoreGoal.FIRST).toString());
    }

    @Test
    public void updateEntryShouldLexographicallySortLast() {
        FirstLastTracker tracker = new FirstLastTracker();
        LinkedHashMap<FirstLastTracker.StoreGoal, Text> entry = new LinkedHashMap<FirstLastTracker.StoreGoal, Text>(1);
        entry.put(FirstLastTracker.StoreGoal.LAST, new Text("B"));
        tracker.updateEntry(entry, new Text("A"), FirstLastTracker.StoreGoal.LAST);
        Assert.assertEquals("B", entry.get(FirstLastTracker.StoreGoal.LAST).toString());
        tracker.updateEntry(entry, new Text("C"), FirstLastTracker.StoreGoal.LAST);
        Assert.assertEquals("C", entry.get(FirstLastTracker.StoreGoal.LAST).toString());
    }

    @Test
    public void backingStoreHasOneHashMapPerKeyProvided() {
        FirstLastTracker tracker = new FirstLastTracker();
        tracker.updateStore("foo", new Text("A"), FirstLastTracker.StoreGoal.FIRST);
        Map<String, Map<FirstLastTracker.StoreGoal, Text>> b = tracker.backingStore;
        Assert.assertEquals(1, b.size());
        tracker.updateStore("bar", new Text("A"), FirstLastTracker.StoreGoal.FIRST);
        Assert.assertEquals(2, b.size());
        Assert.assertNotNull(b.get("foo"));
        Assert.assertNotNull(b.get("bar"));
    }

    @Test
    public void backingStoreMapsHaveKeysProperlyAssigned() {
        FirstLastTracker tracker = new FirstLastTracker();
        tracker.updateStore("foo", new Text("B"), FirstLastTracker.StoreGoal.FIRST);
        tracker.updateStore("foo", new Text("B"), FirstLastTracker.StoreGoal.LAST);
        tracker.updateStore("foo", new Text("A"), FirstLastTracker.StoreGoal.FIRST);
        tracker.updateStore("foo", new Text("C"), FirstLastTracker.StoreGoal.LAST);
        Map<FirstLastTracker.StoreGoal, Text> entry = tracker.backingStore.get("foo");
        Assert.assertEquals("A", entry.get(FirstLastTracker.StoreGoal.FIRST).toString());
        Assert.assertEquals("C", entry.get(FirstLastTracker.StoreGoal.LAST).toString());
    }

    @Test
    public void getEarliestLast() {
        FirstLastTracker tracker = new FirstLastTracker();
        tracker.updateStore("bar", new Text("Y"), FirstLastTracker.StoreGoal.LAST);
        tracker.updateStore("baz", new Text("X"), FirstLastTracker.StoreGoal.LAST);
        tracker.updateStore("foo", new Text("Z"), FirstLastTracker.StoreGoal.LAST);
        Assert.assertEquals("X", tracker.getEarliestLast());
    }

    @Test
    public void getLatestFirst() {
        FirstLastTracker tracker = new FirstLastTracker();
        tracker.updateStore("bar", new Text("B"), FirstLastTracker.StoreGoal.FIRST);
        tracker.updateStore("foo", new Text("C"), FirstLastTracker.StoreGoal.FIRST);
        tracker.updateStore("baz", new Text("A"), FirstLastTracker.StoreGoal.FIRST);
        Assert.assertEquals("C", tracker.getLatestFirst());
    }

    @Test
    public void soupToNuts() {
        FirstLastTracker tracker = new FirstLastTracker();
        tracker.updateStore("foo", new Text("Autoharp"), FirstLastTracker.StoreGoal.FIRST);
        //We should wind up with Xylophone instead of Violin for foo
        tracker.updateStore("foo", new Text("Violin"), FirstLastTracker.StoreGoal.LAST);
        tracker.updateStore("foo", new Text("Xylophone"), FirstLastTracker.StoreGoal.LAST);
        tracker.updateStore("bar", new Text("Bagpipes"), FirstLastTracker.StoreGoal.FIRST);
        tracker.updateStore("bar", new Text("Yotar"), FirstLastTracker.StoreGoal.LAST);
        //We should wind up with Clarinet instead of Drum for baz
        tracker.updateStore("baz", new Text("Clarinet"), FirstLastTracker.StoreGoal.FIRST);
        tracker.updateStore("baz", new Text("Drum"), FirstLastTracker.StoreGoal.FIRST);
        tracker.updateStore("baz", new Text("Zither"), FirstLastTracker.StoreGoal.LAST);
        Assert.assertEquals("Clarinet", tracker.getLatestFirst());
        Assert.assertEquals("Xylophone", tracker.getEarliestLast());
    }

    @Test
    public void numbersDataSetSoupToNuts() {
        FirstLastTracker t = new FirstLastTracker();
        t.updateStore("1186613585", new Text("10"), FirstLastTracker.StoreGoal.FIRST);
        t.updateStore("1186613585", new Text("998"), FirstLastTracker.StoreGoal.LAST);
        t.updateStore("1668757391", new Text("8"), FirstLastTracker.StoreGoal.FIRST);
        t.updateStore("1668757391", new Text("899"), FirstLastTracker.StoreGoal.LAST);
        t.updateStore("1668757391", new Text("9"), FirstLastTracker.StoreGoal.FIRST);
        t.updateStore("1668757391", new Text("999"), FirstLastTracker.StoreGoal.LAST);
        t.updateStore("628381357", new Text("940"), FirstLastTracker.StoreGoal.FIRST);
        t.updateStore("628381357", new Text("940"), FirstLastTracker.StoreGoal.LAST);
        t.updateStore("628381357", new Text("941"), FirstLastTracker.StoreGoal.FIRST);
        t.updateStore("628381357", new Text("941"), FirstLastTracker.StoreGoal.LAST);
        t.updateStore("628381357", new Text("942"), FirstLastTracker.StoreGoal.FIRST);
        t.updateStore("628381357", new Text("942"), FirstLastTracker.StoreGoal.LAST);
        t.updateStore("628381357", new Text("943"), FirstLastTracker.StoreGoal.FIRST);
        t.updateStore("628381357", new Text("943"), FirstLastTracker.StoreGoal.LAST);
        t.updateStore("628381357", new Text("944"), FirstLastTracker.StoreGoal.FIRST);
        t.updateStore("628381357", new Text("944"), FirstLastTracker.StoreGoal.LAST);
        t.updateStore("628381357", new Text("945"), FirstLastTracker.StoreGoal.FIRST);
        t.updateStore("628381357", new Text("945"), FirstLastTracker.StoreGoal.LAST);
        t.updateStore("628381357", new Text("946"), FirstLastTracker.StoreGoal.FIRST);
        t.updateStore("628381357", new Text("946"), FirstLastTracker.StoreGoal.LAST);
        t.updateStore("628381357", new Text("947"), FirstLastTracker.StoreGoal.FIRST);
        t.updateStore("628381357", new Text("947"), FirstLastTracker.StoreGoal.LAST);
        t.updateStore("628381357", new Text("948"), FirstLastTracker.StoreGoal.FIRST);
        t.updateStore("628381357", new Text("948"), FirstLastTracker.StoreGoal.LAST);
        t.updateStore("628381357", new Text("949"), FirstLastTracker.StoreGoal.FIRST);
        t.updateStore("628381357", new Text("949"), FirstLastTracker.StoreGoal.LAST);
        t.updateStore("628381357", new Text("950"), FirstLastTracker.StoreGoal.FIRST);
        t.updateStore("628381357", new Text("950"), FirstLastTracker.StoreGoal.LAST);

        Assert.assertEquals("940", t.getLatestFirst());
        Assert.assertEquals("950", t.getEarliestLast());
    }

}

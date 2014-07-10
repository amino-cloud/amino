package com._42six.amino.common.bigtable.impl;

import com._42six.amino.data.DataLoader;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;

public abstract class AccumuloDataLoader implements DataLoader {

    private static final Logger logger = LoggerFactory.getLogger(AccumuloDataLoader.class);

    public static final String CFG_INSTANCE = "dataloader.instance";
    public static final String CFG_ZOOKEEPERS = "dataloader.zookeepers";
    public static final String CFG_TABLE = "dataloader.table";
    public static final String CFG_USERNAME = "dataloader.username";
    public static final String CFG_PASSWORD = "dataloader.password";
    public static final String CFG_AUTHS = "dataloader.authorizations";

    public static final String CFG_ROW_IDS = "dataloader.rowIds";
    public static final String CFG_ROW_REGEX = "dataloader.regex";

    /**
     * All of the buckets that this DataLoader knows about
     */
    // TODO - Load configurations from the setConfig method
    protected static final Hashtable<Text, Text> bucketsAndDisplayNames = new Hashtable<Text, Text>();

    @SuppressWarnings("rawtypes")
    RecordReader recordReader = null;
    protected Configuration config;

    @Override
    public InputFormat<Key, Value> getInputFormat() {
        return new AccumuloInputFormat();
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void setRecordReader(RecordReader recordReader) throws IOException {
        this.recordReader = recordReader;
    }

    @Override
    public void initializeFormat(Job job) throws IOException {
        final Configuration conf = job.getConfiguration();
        final String instanceName = conf.get(CFG_INSTANCE);
        final String zookeeperInfo = conf.get(CFG_ZOOKEEPERS);
        final String tableName = conf.get(CFG_TABLE);
        final String userName = conf.get(CFG_USERNAME);
        final String password = conf.get(CFG_PASSWORD);
        final String authorizations = conf.get(CFG_AUTHS);
        final String rowIds = conf.get(CFG_ROW_IDS, "");
        final String rowRegex = conf.get(CFG_ROW_REGEX, null);

        logger.info("Grabbing data from table: " + tableName);

        try {
            AccumuloInputFormat.setZooKeeperInstance(job, new ClientConfiguration().withInstance(instanceName).withZkHosts(zookeeperInfo));

            try {
                AccumuloInputFormat.setConnectorInfo(job, userName, new PasswordToken(password.getBytes("UTF-8")));
            } catch (AccumuloSecurityException e) {
                throw new IOException("Error setting Accumulo connector info", e);
            }

            AccumuloInputFormat.setInputTableName(job, tableName);
            AccumuloInputFormat.setScanAuthorizations(job, new Authorizations(authorizations.getBytes()));
            AccumuloInputFormat.addIterator(job, new IteratorSetting(30, WholeRowIterator.class));

            // If a row regex is set, use that to determine which rows to fetch.  If it's not, use the comma seperated
            // list of rowIDs instead
            if(rowRegex != null){
                logger.info("Using rowId regex: " + rowRegex);
                // Name is needed for ACCUMULO-1267
                final IteratorSetting regexSetting = new IteratorSetting(20, "Warehaus Row Regex", RegExFilter.class);
                RegExFilter.setRegexs(regexSetting, rowIds, null, null, null, false);
                AccumuloInputFormat.addIterator(job, regexSetting);
            } else {
                final List<Range> ranges = new ArrayList<Range>();

                // If the rowIds was empty (or not provided at all) then fetch all of the rows
                if(rowIds.isEmpty()){
                    logger.info("No specific RowID requested.  Fetching all rows.");
                    ranges.add(new Range());
                } else{
                    for(String rowId : rowIds.split(",")){
                        ranges.add(new Range(rowId));
                        logger.info("Fetching RowID: " + rowId);
                    }
                }
                AccumuloInputFormat.setRanges(job, ranges);
            }
        } catch (IllegalStateException e){
            logger.warn("Attempting to initalizeFormat when it's already been initalized");
        }
    }

    @Override
    public MapWritable getNext() throws IOException {
        Key key;
        Value value;
        try {
            if (!this.recordReader.nextKeyValue()) {
                return null;
            }

            key = (Key) this.recordReader.getCurrentKey();
            value = (Value) this.recordReader.getCurrentValue();
        } catch (InterruptedException e) {
            return null;
        }

        if (key == null || value == null) {
            if (key == null) {
                logger.error("Key was null");
            }
            if (value == null) {
                logger.warn("Value was null");
            }
            return null;
        }

        return processWholeRow(key, value);
    }

    /**
     * Process a whole row from the WholeRowIterator.  This will generally be of the form:
     * <p/>
     * for(Map.Entry<Key,Value> entry : WholeRowIterator.decodeRow(key,value).entrySet()){
     * process values from entry
     * place results into outputMap
     * }
     *
     * @param key   The Key returned from the WholeRowIterator
     * @param value The Value returned from the WholeRowIterator
     * @return MapWritable with all of the bucketed values
     */
    protected abstract MapWritable processWholeRow(Key key, Value value) throws IOException;

    @Override
    public List<Text> getBuckets() {
        return new LinkedList<Text>(bucketsAndDisplayNames.keySet());
    }

    @Override
    public Hashtable<Text, Text> getBucketDisplayNames() {
        return bucketsAndDisplayNames;
    }

    @Override
    public void setConfig(Configuration config) {
        this.config = config;
    }

    @Override
    public boolean canReadFrom(InputSplit inputSplit) {
        return true;
    }

    @Override
    public MapWritable getNextKey(MapWritable currentKey) throws IOException,
            InterruptedException {
        return currentKey;
    }
}

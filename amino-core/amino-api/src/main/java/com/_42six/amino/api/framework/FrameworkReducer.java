package com._42six.amino.api.framework;

import com._42six.amino.api.job.AminoJob;
import com._42six.amino.api.job.AminoReducer;
import com._42six.amino.api.model.DatasetCollection;
import com._42six.amino.common.AminoWritable;
import com._42six.amino.common.Bucket;
import com._42six.amino.common.BucketStripped;
import com._42six.amino.common.service.datacache.BucketCache;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public final class FrameworkReducer extends Reducer<BucketStripped, MapWritable, Bucket, AminoWritable> {

	private AminoJob aminoJob;
	private List<AminoReducer> reducerList = new ArrayList<AminoReducer>();
	private Map<String, Text> sortFields;
	private Set<String> dedupDatasets = new HashSet<String>();
	private long timestamp;
	private BucketCache bucketCache;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		
		timestamp = Calendar.getInstance().getTimeInMillis();		

		Configuration config = context.getConfiguration();
		
		bucketCache = new BucketCache(config);
		
		String[] sortDatasetName = config.getStrings("amino.sort.name.datasource");
		String[] sortDatasetField = config.getStrings("amino.sort.name.field");
		for (String dedupDatasource : config.getStringCollection("amino.datasource.dedup")) {
			dedupDatasets.add(dedupDatasource);
		}
		
		//to enable sorting, sort name and fields need to have the same number of values
		sortFields = new HashMap<String, Text>();
		if (sortDatasetName != null && sortDatasetField != null 
				&& sortDatasetName.length > 0 && sortDatasetName.length == sortDatasetField.length) {
			for (int i = 0; i < sortDatasetName.length; ++i) {
				sortFields.put(sortDatasetName[i], new Text(sortDatasetField[i]));
			}
		}

		try {
			aminoJob = AminoDriverUtils.getAminoJob(config);
		}
		catch (Exception e) {
			throw new IOException(e);
		}

		for (Class<? extends AminoReducer> cls : aminoJob.getAminoReducerClasses()) {
			try {
				AminoReducer ar = cls.newInstance();
				ar.setConfig(config);
				reducerList.add(ar);
			}
			catch (Exception e) {
				throw new IOException(e);
			}
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);
	}

	@Override
	public void reduce(BucketStripped strippedKey, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
		/**
         * TODO: we should run the data loader only once and serialize the
		 * results somewhere.  When that happens, stop doing this deep copy.
		 */
		
		// Convert the stripped bucket back into a full bucket
		Bucket key = bucketCache.getBucket(strippedKey);
		key.setTimestamp(timestamp);
		
		// Build our DatasetCollection.
		// This reads all of the values into memory!
		DatasetCollection datasets = new DatasetCollection(key, values, sortFields, dedupDatasets);
		
		// Execute each reducer using this dataset and write results to context
		for (AminoReducer ar : reducerList) {
			for (AminoWritable result : ar.reduce(datasets)) {
				context.write(key, result);
				context.progress();
			}
		}
	}
}

package com._42six.amino.api.model;

import com._42six.amino.common.Bucket;
import com._42six.amino.data.DataLoader;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.util.*;

public class DatasetCollection {

	public static final String DATASET_KEY_VALUE_DEFAULT = "AMINO_DEFAULT_DATASET";
	
	private Bucket bucketKey;
	private Map<String, Collection<Row>> sortedDatasets;
	private Map<String, Collection<Row>> unsortedDatasets;

	public DatasetCollection(Bucket key, Iterable<MapWritable> values, Map<String, Text> sortFields, Set<String> dedupDatasources) {
		bucketKey = key;
		
		unsortedDatasets = new HashMap<>();
		sortedDatasets = new HashMap<>();
		HashMap<String, Collection<RowComparable>> sortedDatasetsTemp = new HashMap<>();
		
		//add each mapwritable to its dataset, based on the dataset key
		for (MapWritable mw : values) {
			Writable datasetKeyWritable = mw.get(DataLoader.DATASET_NAME);
			
			// add to default dataset key value if no dataset key exists
			String datasetKey;
			if (datasetKeyWritable == null) {
				datasetKey = DATASET_KEY_VALUE_DEFAULT;
			}
			else {
				datasetKey = datasetKeyWritable.toString();
			}
			
			// if dataset exists in sorted collection add it to that
			if (sortedDatasetsTemp.containsKey(datasetKey)) {
				sortedDatasetsTemp.get(datasetKey).add(new RowComparable(mw, sortFields.get(datasetKey)));
			}
			// if dataset exists in unsorted collection, add it to that
			else if (unsortedDatasets.containsKey(datasetKey)) {
				unsortedDatasets.get(datasetKey).add(new Row(mw));
			}
			// if sortKey and sortField was passed, and it corresponds to this row, put it in the sorted datasets
			else if (sortFields.containsKey(datasetKey)) {
				//if we dedup this dataset, use a HashSet, otherwise use an ArrayList
				Collection<RowComparable> newCollection = 
						dedupDatasources.contains(datasetKey) ? new HashSet<RowComparable>() : new ArrayList<RowComparable>();
				newCollection.add(new RowComparable(mw, sortFields.get(datasetKey)));
				sortedDatasetsTemp.put(datasetKey, newCollection);
			}
			// otherwise, this must be an unsorted row, so add it to the unsorted datasets
			else {
				//if we dedup this dataset, use a HashSet, otherwise use an ArrayList
				Collection<Row> newCollection = 
						dedupDatasources.contains(datasetKey) ? new HashSet<Row>() : new ArrayList<Row>();
				newCollection.add(new Row(mw));
				unsortedDatasets.put(datasetKey, newCollection);
			}
		}

		// sort everything in sortedDatasets
		for (String sourceKey : sortedDatasetsTemp.keySet()) {
			
			Collection<RowComparable> dataset = sortedDatasetsTemp.get(sourceKey);
			
			if (dataset.getClass().equals(ArrayList.class)) {
				Collections.sort((ArrayList<RowComparable>)dataset);
			}
			else {
				ArrayList<RowComparable> newList = new ArrayList<>(dataset.size());
				for (RowComparable row : dataset) {
					newList.add(row);
				}
				Collections.sort(newList);
				dataset = newList;
			}
			
			Collection<Row> sortedDataset = new ArrayList<>(dataset.size());
			for (RowComparable row : dataset) {
				sortedDataset.add(row);
			}
			sortedDatasets.put(sourceKey, sortedDataset);
		}
	}
	
	public Collection<Row> getUnsortedDataset(final String datasetName) {
		return unsortedDatasets.get(datasetName);
	}
	
	public Collection<Row> getSortedDataset(final String datasetName) {
		return sortedDatasets.get(datasetName);
	}
	
	public Collection<Row> getAllDatasets() {
		Collection<Row> allDatasets = new ArrayList<>();
		for (String key : sortedDatasets.keySet()) {
			allDatasets.addAll(sortedDatasets.get(key));
		}
		for (String key : unsortedDatasets.keySet()) {
			allDatasets.addAll(unsortedDatasets.get(key));
		}
		return allDatasets;
	}
	
	public Bucket getBucketKey() {
		return bucketKey;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "DatasetCollection [bucketKey=" + bucketKey
				+ ", sortedDatasets=" + sortedDatasets + ", unsortedDatasets="
				+ unsortedDatasets + "]";
	}
	

}

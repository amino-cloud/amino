package com._42six.amino.query.services.accumulo;

import com._42six.amino.common.BucketMetadata;
import com._42six.amino.common.DatasourceMetadata;
import com._42six.amino.common.FeatureMetadata;
import com._42six.amino.common.MorePreconditions;
import com._42six.amino.common.bigtable.TableConstants;
import com._42six.amino.common.entity.Hypothesis;
import com._42six.amino.common.entity.HypothesisFeature;
import com._42six.amino.query.exception.BigTableException;
import com._42six.amino.query.exception.EntityNotFoundException;
import com._42six.amino.query.services.AminoMetadataService;
import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

/**
 * CRUD service for metadata entities.
 *
 * @author Amino Team
 */
public class AccumuloMetadataService implements AminoMetadataService {

	public static final Logger log = Logger.getLogger(AccumuloMetadataService.class);

	public AccumuloPersistenceService persistenceService;
	public AccumuloGroupService groupService;

	private Integer hashCount = null;
	private Integer shardCount = null;

	public String hypothesisTable = "amino_hypothesis";
	public String metadataTable = "amino_metadata";

	public AccumuloMetadataService() {
		// EMPTY
	}

	public AccumuloMetadataService(AccumuloPersistenceService persistenceService) {
		this.persistenceService = persistenceService;
	}

    public AccumuloMetadataService(AccumuloPersistenceService persistenceService, AccumuloGroupService groupService) {
        this.persistenceService = persistenceService;
        this.groupService = groupService;
    }

    /**
     * Adds the suffix to all of the tables
     * @param suffix The suffix to append to the tables
     */
    @Override
    public void addTableSuffix(String suffix){
        metadataTable = metadataTable + suffix;
        hypothesisTable = hypothesisTable + suffix;
    }

	public void setGroupService(AccumuloGroupService groupService){
		this.groupService = groupService;
	}

	public void setPersistenceService(AccumuloPersistenceService persistenceService) {
		this.persistenceService = persistenceService;
	}

	public void setHypothesisTable(String hypothesisTable) {
		this.hypothesisTable = hypothesisTable;
	}

	public void setMetadataTable(String metadataTable) {
		this.metadataTable = metadataTable;
	}

	public List<DatasourceMetadata> listDataSources(String[] visibility) throws IOException {
		final List<DatasourceMetadata> dataSources = new ArrayList<DatasourceMetadata>();

        Scanner metaScanner;
        try{
            metaScanner = persistenceService.createScanner(metadataTable, new Authorizations(visibility));
        } catch (TableNotFoundException ex){
            log.error("Table '" + metadataTable + "' was not found");
            throw new IOException(ex);
        }

		metaScanner.setRange(new Range(new Text(TableConstants.DATASOURCE_PREFIX), TableConstants.DATASOURCE_END));
		metaScanner.fetchColumnFamily(new Text("JSON"));
		for (Map.Entry<Key, Value> entry : metaScanner) {
			dataSources.add(new Gson().fromJson(entry.getValue().toString(), DatasourceMetadata.class));
		}

		return dataSources;
	}

	public List<FeatureMetadata> listFeatures(String datasourceId, String[] visibility) throws IOException {

		final Gson gson = new Gson();
		final List<FeatureMetadata> results = new ArrayList<FeatureMetadata>();
		final Authorizations auths = new Authorizations(visibility);

        Scanner metaScanner;
        try{
            metaScanner = persistenceService.createScanner(metadataTable, auths);
        } catch (TableNotFoundException ex){
            log.error("Table '" + metadataTable + "' was not found");
            throw new IOException(ex);
        }
		metaScanner.setRange(new Range(new Text(TableConstants.DATASOURCE_PREFIX), TableConstants.DATASOURCE_END));
		metaScanner.fetchColumnFamily(TableConstants.FEATUREIDS_FIELD);

		// Make sure the params passed in were legit
		if(!metaScanner.iterator().hasNext()){
			return null;
		}

        @SuppressWarnings("serial")
		ArrayList<String> featureIds = gson.fromJson(metaScanner.iterator().next().getValue().toString(),
                new TypeToken<ArrayList<String>>(){}.getType());

		final List<Range> featureRanges = new ArrayList<Range>();
		for(String featureId : featureIds){
			featureRanges.add(new Range(TableConstants.FEATURE_PREFIX + featureId));
		}

        // TODO - Should this be moved to where we create the features for the metadata table??
        // Add in the special RESTRICTION feature that is part of every datasource
        featureRanges.add(new Range(TableConstants.FEATURE_PREFIX+"1"));

		BatchScanner featuresScanner = null;
		try{
			featuresScanner = persistenceService.createBatchScanner(metadataTable, auths);
			featuresScanner.setRanges(featureRanges);
			featuresScanner.fetchColumnFamily(TableConstants.JSON_FIELD);
			featuresScanner.fetchColumnFamily(TableConstants.TYPE_FIELD);

            HashMap<Text, HashMap<Text, String>> featuresValues = new HashMap<Text, HashMap<Text, String>>();
			for(Map.Entry<Key, Value> entry : featuresScanner){
                // Get the hashmap for this row
                HashMap<Text, String> featureValue = featuresValues.get(entry.getKey().getRow());
                if (featureValue == null) {
                    featureValue = new HashMap<Text, String>();
                }

                // Add the JSON value/Type value
                featureValue.put(entry.getKey().getColumnFamily(), entry.getValue().toString());
                featuresValues.put(entry.getKey().getRow(), featureValue);
			}

            // Add the results
            for (HashMap<Text, String> entry : featuresValues.values()) {
                String json = entry.get(TableConstants.JSON_FIELD);
                String type = entry.get(TableConstants.TYPE_FIELD);
                results.add(FeatureMetadata.fromJson(json, type));
            }
		}  catch (TableNotFoundException ex){
            log.error("Table '" + metadataTable + "' was not found");
            throw new IOException(ex);
        } finally {
			if(featuresScanner != null) {
                featuresScanner.close();
            }
		}

		return results;
	}

	public List<BucketMetadata> listBuckets(String datasourceId, String[] visibility) throws IOException {
		final List<BucketMetadata> buckets = new ArrayList<BucketMetadata>();
		final Set<Range> bucketRanges = new HashSet<Range>();
		final Authorizations auths = new Authorizations(visibility);

		// Find all of the buckets that are associated with the datasource
        Scanner datasourceScanner;
        try{
            datasourceScanner = persistenceService.createScanner(metadataTable, auths);
        } catch (TableNotFoundException ex){
            log.error("Table '" + metadataTable + "' was not found");
            throw new IOException(ex);
        }
		datasourceScanner.setRange(new Range(TableConstants.DATASOURCE_PREFIX + datasourceId));
		datasourceScanner.fetchColumnFamily(TableConstants.BUCKETID_FIELD);
		if(!datasourceScanner.iterator().hasNext()){
			return null;
		} else {
			final ArrayList<String> bucketIds = new Gson().fromJson(datasourceScanner.iterator().next().getValue().toString(), ArrayList.class);
			for (String bucketId : bucketIds) {
	            bucketRanges.add(new Range(TableConstants.BUCKET_PREFIX + bucketId));
			}
		}

        BatchScanner bucketScanner = null;
		// Serialize all of the associated buckets
		try {
            bucketScanner = persistenceService.createBatchScanner(metadataTable, auths);
            bucketScanner.setRanges(bucketRanges);
            bucketScanner.fetchColumnFamily(new Text("JSON"));
			for (Map.Entry<Key, Value> entry : bucketScanner) {
                buckets.add(BucketMetadata.fromJson(entry.getValue().toString()));
			}

		} catch (TableNotFoundException ex){
            log.error("Table '" + metadataTable + "' was not found");
            throw new IOException(ex);
        }  finally {
			if(bucketScanner != null){
                bucketScanner.close();
            }
		}

		return buckets;
	}

	public DatasourceMetadata getDataSource(String dataSourceId, String[] visibility) throws IOException {
        return (DatasourceMetadata) getEntity(dataSourceId, TableConstants.DATASOURCE_PREFIX, DatasourceMetadata.class, visibility);
	}

	public FeatureMetadata getFeature(String id, String[] visibility) throws IOException {
		return getFeature(id, new Authorizations(visibility));
	}

	protected FeatureMetadata getFeature(String id, Authorizations auths) throws IOException {
            String json = getEntityString(id, TableConstants.FEATURE_PREFIX, auths);
            return FeatureMetadata.fromJson(json);
	}

	public BucketMetadata getBucket(String id, String[] visibility) throws IOException {
		return (BucketMetadata) getEntity(id, TableConstants.BUCKET_PREFIX, BucketMetadata.class, visibility);
	}

	public BucketMetadata getBucket(String id, Authorizations auths) throws IOException {
		return (BucketMetadata) getEntity(id, TableConstants.BUCKET_PREFIX, BucketMetadata.class, auths);
	}

	/**
	 * Fetches the Hypothesis
	 *
	 * @param userId       The ID of the user making the request
	 * @param owner        The owner field of the hypthesis to fetch
	 * @param hypothesisId The ID of the hypothesis to fetch
	 * @param visibility   The security visibilities for the database
	 */
	public Hypothesis getHypothesis(String userId, String owner, String hypothesisId, String[] visibility) throws IOException {
		Preconditions.checkNotNull(visibility);
		return getHypothesis(userId, owner, hypothesisId, new Authorizations(visibility));
	}

	/**
	 * Fetches the Hypothesis
	 *
	 * @param userId       The ID of the user making the request
	 * @param owner        The owner field of the hypothesis to fetch
	 * @param hypothesisId The ID of the hypothesis to fetch
	 * @param auths        The security authorizations for the database
	 */
	public Hypothesis getHypothesis(String userId, String owner, String hypothesisId, Authorizations auths) throws IOException {
		MorePreconditions.checkNotNullOrEmpty(userId, "The userId can not be empty");
		MorePreconditions.checkNotNullOrEmpty(owner, "The owner can not be empty");
		MorePreconditions.checkNotNullOrEmpty(hypothesisId, "The hypothesisId can not be empty");
		Preconditions.checkNotNull(auths, "Must pass in authorizations");

		Set<String> groups = null;// null if we don't need it, set if we have to look up what group the requester is in

		// Check to see if we are checking against the owner of the hypothesis.  If not, we need to
		// make sure that the requester is allowed to fetch the hypothesis
		if (userId.compareTo(owner) != 0) {
			groups = groupService.getGroupsForUser(userId, auths);
			if (groups.size() <= 0) {
				throw new EntityNotFoundException("No hypothesis found with id of " + ((hypothesisId != null) ? hypothesisId : "Empty"));
			}
		}

		// Fetch the Hypothesis
        Scanner scan;
        try{
            scan = persistenceService.createScanner(hypothesisTable, auths);
        }  catch (TableNotFoundException ex){
            log.error("Table '" + hypothesisTable + "' was not found");
            throw new IOException(ex);
        }
		scan.setRange(new Range(owner));
		scan.fetchColumnFamily(new Text(hypothesisId));

		Iterator<Map.Entry<Key, Value>> itr = scan.iterator();
		scan.iterator();
		if (!itr.hasNext()) {
			throw new EntityNotFoundException("No hypothesis found with id of <" + hypothesisId + "> and owner <" + owner + ">");
		}

		// Found the record.  Serialize the Hypothesis to return
		Hypothesis hypothesis = new Hypothesis();
		hypothesis.owner = owner;
		hypothesis.id = hypothesisId;
		hypothesis.hypothesisFeatures = new HashSet<HypothesisFeature>();
		Map.Entry<Key, Value> nextEntry = null;
		while (itr.hasNext()) {
			nextEntry = itr.next();
			addHypothesisComponent(hypothesis, nextEntry);
		}

		// TODO see if we still need this
		if(nextEntry != null){
			hypothesis.btVisibility = nextEntry.getKey().getColumnVisibility().toString();
		}

		// Make sure that the requester is allowed to view this hypothesis
		if (groups != null && Collections.disjoint(groups, hypothesis.canView)) {
			throw new EntityNotFoundException("User <" + userId + "> is not allowed to view Hypothesis with id <" + hypothesisId + ">");
		}

		return hypothesis;
	}

	public List<Hypothesis> listHypotheses(String userId, String[] visibility) throws IOException {
		MorePreconditions.checkNotNullOrEmpty(userId, "Owner string can not be empty");
		Preconditions.checkNotNull(visibility, "visibility can not be null");

        Scanner scan;
        try{
            scan = persistenceService.createScanner(hypothesisTable, new Authorizations(visibility));
        } catch (TableNotFoundException ex){
            log.error("Table '" + hypothesisTable + "' was not found");
            throw new IOException(ex);
        }

		// Restrict the rows that we get back to just the ones the userId should be able to see
		scan.setRange(new Range(userId));

		List<Hypothesis> entities = new ArrayList<Hypothesis>();
		Hypothesis activeEntity = null;

		// Loop through the hypotheses until we've collected our row amount or run out
		//Iterator<Map.Entry<Key, Value>> itr = scan.iterator();
		for(Map.Entry<Key, Value> entry : scan){
			String id = entry.getKey().getColumnFamily().toString();
      String owner = entry.getKey().getRow().toString();

			// Hypothesis are made up of multiple rows.  Since the results are sorted, if we
			// come across a new row, then need to create a new Hypothesis
			if (activeEntity == null || id.compareTo(activeEntity.id) != 0){
				activeEntity = new Hypothesis();
				activeEntity.id = id;
        activeEntity.owner = owner;
				activeEntity.hypothesisFeatures = new HashSet<HypothesisFeature>();
				entities.add(activeEntity);
			}

			addHypothesisComponent(activeEntity, entry);
		}


		return entities;
	}



	public Hypothesis createHypothesis(Hypothesis hypothesis, String userId, String[] visibility) throws Exception {
		hypothesis.created = System.currentTimeMillis();
        hypothesis.updated = hypothesis.created;
		return persistHypothesis(hypothesis, userId);
	}

	public Hypothesis updateHypothesis(Hypothesis hypothesis, String requester, String[] visibility) throws Exception {
		return updateHypothesis(hypothesis, requester, new Authorizations(visibility));
	}

	public Hypothesis updateHypothesis(final Hypothesis hypothesis, String requester, Authorizations auths) throws Exception {
		Preconditions.checkNotNull(hypothesis, "hypothesis can not be null");
		Preconditions.checkNotNull(auths, "auths can not be null");
		MorePreconditions.checkNotNullOrEmpty(requester, "Must provide a requester for updateHypothesis");

		// Check to make sure that the requester can "edit" this hypothesis
		if (requester.compareTo(hypothesis.owner) != 0) {
			Set<String> groups = groupService.getGroupsForUser(requester, auths);

			// Make sure the requester even has a chance of being in an edit group
			if (groups == null || groups.size() <= 0) {
				// TODO change to better exception
				throw new EntityNotFoundException("User " + requester + " can not edit this hypothesis");
			}

			// Figure out which groups can actually edit the hypothesis
			Scanner scan = persistenceService.createScanner(hypothesisTable, auths);
			scan.setRange(new Range(hypothesis.owner));
			scan.fetchColumn(new Text(hypothesis.id), new Text("canEdit"));

			Iterator<Map.Entry<Key, Value>> itr = scan.iterator();
			if (!itr.hasNext()) {
				// TODO Throw more appropriate error
				throw new EntityNotFoundException("canEdit property not found for hypothesis " + hypothesis.id);
			}


			// Parse the list and see if any of the groups the requester is in intersects with whom can edit it
			List<String> editList = new Gson().fromJson(itr.next().getValue().toString(), List.class);
			if (Collections.disjoint(editList, groups)) {
				// TODO Throw more appropriate exception
				throw new EntityNotFoundException("User " + requester + " can not edit this hypothesis");
			}
		}

		// "Update" the hypothesis by removing the old one and inserting the new one
		try {
			deleteHypothesis(hypothesis.owner, hypothesis.id, auths);
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e); // TODO change the exception
		}
		hypothesis.updated = System.currentTimeMillis();
		return persistHypothesis(hypothesis, requester);
	}


	public void deleteHypothesis(String owner, String id, String[] visibility) {
		try {
			deleteHypothesis(owner, id, new Authorizations(visibility));
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e); // TODO change the exception
		}
	}

	/**
	 * Removes a particular Hypothesis from the tables
	 *
	 * @param owner      The owner id of the Hypothesis
	 * @param id         The Hypothesis ID to delete
	 */
	public void deleteHypothesis(String owner, String id, Authorizations auths) throws IOException {
		MorePreconditions.checkNotNullOrEmpty(owner, "Can not delete Hypothesis with empty owner");
		MorePreconditions.checkNotNullOrEmpty(id, "Can not delete Hypothesis with empty ID");
		Preconditions.checkNotNull(auths);

		final Gson gson = new Gson();

		BatchDeleter deleter;
		BatchDeleter groupLutDeleter;
		try {
			// Retrieve the groups that have access to this hypothesis
			Scanner scanner = persistenceService.createScanner(hypothesisTable, auths);
			scanner.setRange(new Range(owner));
			scanner.fetchColumn(new Text(id), new Text("canView"));
			final List<Range> groupRanges = new ArrayList<Range>();
			for(Map.Entry<Key, Value> entry : scanner) {
                @SuppressWarnings("serial")
				ArrayList<String> groups = gson.fromJson(entry.getValue().toString(),
                        new TypeToken<ArrayList<String>>(){}.getType());
				for(String group : groups) {
					groupRanges.add(new Range(group));
				}
			}

			// Remove the hypothesis from the hypothesis table
			deleter = persistenceService.createBatchDeleter(hypothesisTable, auths);
			deleter.setRanges(new ArrayList<Range>(Arrays.asList(new Range(owner))));
			deleter.fetchColumnFamily(new Text(id));
			deleter.delete();

			//Remove the group to hypothesis pairings from the group_hypothesisLUT
			if(groupRanges.size() > 0){
				groupLutDeleter = persistenceService.createBatchDeleter(groupService.getGroupHypothesisLUT(), auths); // TODO Do this in the groupService
				groupLutDeleter.setRanges(groupRanges);
				groupLutDeleter.fetchColumn(new Text(owner), new Text(id));
				groupLutDeleter.delete();
			}
		}  catch (TableNotFoundException ex){
            throw new IOException(ex);
        }  catch (MutationsRejectedException ex){
            throw new IOException(ex);
        }  finally {
			// Close up any of the deleters we might have created
			// TODO The version of Accumulo that we have doesn't have the delete method !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
			// if(deleter != null){ deleter.close(); }
			// if(groupLutDeleter != null){ groupLutDeleter.close(); }
		}
	}

	public Integer getShardCount(boolean useCachedValue) throws BigTableException {
		if (!useCachedValue || this.shardCount == null) {
            final Scanner scan;
            try {
                final Set<String> auths = persistenceService.getLoggedInUserAuthorizations();
                scan = persistenceService.createScanner(metadataTable, new Authorizations(auths.toArray(new String[auths.size()])));
            } catch (TableNotFoundException e) {
                throw new BigTableException(e);
            }
            scan.setRange(new Range(TableConstants.SHARDCOUNT_FIELD));

			Iterator<Map.Entry<Key, Value>> itr = scan.iterator();
			if (itr.hasNext()) {
				this.shardCount = Integer.parseInt(itr.next().getValue().toString());
			} else {
				throw new EntityNotFoundException("No shard count was found in the Amino metadata table");
			}
		}

		return this.shardCount;
	}

	public Integer getShardCount() throws BigTableException {
		return getShardCount(true);
	}

	public Integer getHashCount(boolean useCachedValue) throws BigTableException {
		if (!useCachedValue || this.hashCount == null) {
            final Scanner scan;
            try {
                final Set<String> auths = persistenceService.getLoggedInUserAuthorizations();
                scan = persistenceService.createScanner(metadataTable, new Authorizations(auths.toArray(new String[auths.size()])));
            } catch (TableNotFoundException e) {
                throw new BigTableException(e);
            }
            scan.setRange(new Range(TableConstants.HASHCOUNT_FIELD));

			Iterator<Map.Entry<Key, Value>> itr = scan.iterator();
			if (itr.hasNext()) {
				this.hashCount =  Integer.parseInt(itr.next().getValue().toString());
			} else {
				throw new EntityNotFoundException("No hash count was found in the Amino metadata table");
			}
		}

		return this.hashCount;
	}

	public Integer getHashCount() throws BigTableException {
		return getHashCount(true);
	}

	////////////////////////////////////
	// Private methods
	////////////////////////////////////

	private Hypothesis persistHypothesis(final Hypothesis hypothesis, String requester) throws Exception {
		// Validate parameters
		Preconditions.checkNotNull(hypothesis);
		Preconditions.checkNotNull(hypothesis.canEdit);
		Preconditions.checkNotNull(hypothesis.canView);
		MorePreconditions.checkNotNullOrEmpty(hypothesis.bucketid, "Can not have null bucketid for hypothesis");
		MorePreconditions.checkNotNullOrEmpty(hypothesis.datasourceid, "Can not have null datasourceid for hypothesis");
		MorePreconditions.checkNotNullOrEmpty(hypothesis.name, "Can not have null name string for hypothesis");
		MorePreconditions.checkNotNullOrEmpty(hypothesis.visibility, "Can not have null visibility strings for hypothesis");
		MorePreconditions.checkNotNullOrEmpty(hypothesis.btVisibility, "Can not have null btVisibility strings for hypothesis");
		MorePreconditions.checkNotNullOrEmpty(hypothesis.owner, "Can not have null owner strings for hypothesis");
		Preconditions.checkNotNull(hypothesis.queries, "Can not have null queries");
		Preconditions.checkNotNull(hypothesis.created, "Created time can not be null");
		Preconditions.checkNotNull(hypothesis.updated, "Updated time can not be null");

		// TODO Add in logic to make sure that the person adding the hypothesis should be able to do this
//		if (requester.compareTo(hypothesis.owner) != 0) {
//			// throw new IllegalArgumentException("Only the owner can create the Hypothesis")
//		}

		final Gson gson = new Gson();
		final String ownerId = hypothesis.owner;

		// Create a UUID for the hypothesis if it doesn't have one already
		hypothesis.id = (hypothesis.id != null) ? hypothesis.id :UUID.randomUUID().toString();
		final String uuid = hypothesis.id;

		// Create a uuid for each of the features (not sure what we do with it though...)
        for(HypothesisFeature hf : hypothesis.hypothesisFeatures) {
            if(hf.id == null){
                hf.id = UUID.randomUUID().toString();
            }
        }

		// TODO use the map<String, String> until we refactor the code the right way
		ArrayList<Mutation> hypothesisMutations = new ArrayList<Mutation>(12);

		// Add required parameters
		hypothesisMutations.add(persistenceService.createInsertMutation(ownerId, uuid, "bucket", hypothesis.btVisibility, hypothesis.bucketid));
		hypothesisMutations.add(persistenceService.createInsertMutation(ownerId, uuid, "canEdit", hypothesis.btVisibility, gson.toJson(hypothesis.canEdit)));
		hypothesisMutations.add(persistenceService.createInsertMutation(ownerId, uuid, "canView", hypothesis.btVisibility, gson.toJson(hypothesis.canView)));
		hypothesisMutations.add(persistenceService.createInsertMutation(ownerId, uuid, "created", hypothesis.btVisibility, String.valueOf(hypothesis.created)));
		hypothesisMutations.add(persistenceService.createInsertMutation(ownerId, uuid, "datasource", hypothesis.btVisibility, hypothesis.datasourceid));
		hypothesisMutations.add(persistenceService.createInsertMutation(ownerId, uuid, "executed", hypothesis.btVisibility, String.valueOf(hypothesis.executed)));
		hypothesisMutations.add(persistenceService.createInsertMutation(ownerId, uuid, "name", hypothesis.btVisibility, hypothesis.name));
		hypothesisMutations.add(persistenceService.createInsertMutation(ownerId, uuid, "queries", hypothesis.btVisibility, gson.toJson(hypothesis.queries)));
		hypothesisMutations.add(persistenceService.createInsertMutation(ownerId, uuid, "updated", hypothesis.btVisibility, String.valueOf(hypothesis.updated)));
		hypothesisMutations.add(persistenceService.createInsertMutation(ownerId, uuid, "visibility", hypothesis.btVisibility, hypothesis.visibility));

		// Add the optional parameters if they are available
		if (hypothesis.justification != null) {
			hypothesisMutations.add(persistenceService.createInsertMutation(ownerId, uuid, "justification", hypothesis.btVisibility, hypothesis.justification));
		}

		// Add all of the features
		hypothesisMutations.add(persistenceService.createInsertMutation(ownerId, uuid, "features", hypothesis.btVisibility, gson.toJson(hypothesis.hypothesisFeatures)));

		// Save it off	
		persistenceService.insertRows(hypothesisMutations, hypothesisTable);

		// And now we need to add this hypothesis to the group LUT
		if(hypothesis.canView != null && hypothesis.canView.size() > 0){
			final ArrayList<Mutation> lutList = new ArrayList<Mutation>();
			for (String it : hypothesis.canView){
				lutList.add(persistenceService.createInsertMutation(it, ownerId, uuid, hypothesis.btVisibility, ""));
			}
			persistenceService.insertRows(lutList, groupService.getGroupHypothesisLUT()); // TODO Move this to the groupService
		}
		return hypothesis;
	}

	/**
	 * Helper function for adding components to a Hypothesis based on the values from a amino_hypothesis table record
	 *
	 * @param hypothesis  The Hypothesis to add to
	 * @param entry       The bt Entry to pull values from
	 * @param fieldsToAdd A List<String> of fields that we want to serialize for the hypothesis
	 */
	public static void addHypothesisComponent(Hypothesis hypothesis, Map.Entry<Key, Value> entry, List<String> fieldsToAdd) {
		String cq 	= entry.getKey().getColumnQualifier().toString();
		String value = entry.getValue().toString();

		if ((fieldsToAdd == null || fieldsToAdd.contains(cq)) && !value.equals("null")){
			if (cq.equals("features")) {
				hypothesis.hypothesisFeatures = new Gson().fromJson(value, new TypeToken<Set<HypothesisFeature>>(){}.getType());
			} else if (cq.equals("bucket")) {
				hypothesis.bucketid = value;
			} else if (cq.equals("datasource")) {
				hypothesis.datasourceid = value;
			} else if (cq.equals("justification")) {
				hypothesis.justification = value;
			} else if (cq.equals("name")) {
				hypothesis.name = value;
			} else if (cq.equals("visibility")) {
				hypothesis.visibility = value;
			} else if (cq.equals("canEdit")) {
				hypothesis.canEdit =  new Gson().fromJson(value, new TypeToken<ArrayList<String>>(){}.getType());
			} else if (cq.equals("canView")) {
				hypothesis.canView = new Gson().fromJson(value, new TypeToken<ArrayList<String>>(){}.getType());
			} else if (cq.equals("created")) {
				hypothesis.created = Long.parseLong(value);
			} else if (cq.equals("executed")) {
				hypothesis.executed = Long.parseLong(value);
			} else if (cq.equals("updated")) {
				hypothesis.updated = Long.parseLong(value);
			} else if (cq.equals("queries")) {
				hypothesis.queries = new Gson().fromJson(value, TreeSet.class);
			} else {
				log.warn("Don't know how to add a '" + cq + "' Hypothesis component");
			}
		}
	}

	/**
	 * Helper function for adding components to a Hypothesis based on the values from a amino_hypothesis table record
	 *
	 * @param hypothesis  The Hypothesis to add to
	 * @param entry       The bt Entry to pull values from
	 */
	public static void addHypothesisComponent(Hypothesis hypothesis, Map.Entry<Key, Value> entry) {
		addHypothesisComponent(hypothesis, entry, null);
	}


    /**
     * Gets the Entity as a JSON string
     */
    private String getEntityString(String id, String entityPrefix, Authorizations auths) throws IOException {
        Scanner scan;
        try{
            scan = persistenceService.createScanner(metadataTable, auths);
        } catch (TableNotFoundException ex){
            log.error("Table '" + metadataTable + "' was not found");
            throw new IOException(ex);
        }
        scan.setRange(new Range(new Text(entityPrefix + id)));
        scan.fetchColumnFamily(TableConstants.JSON_FIELD);

        Iterator<Map.Entry<Key, Value>> itr = scan.iterator();
        if(!itr.hasNext()){
            return null;
        } else {
            return itr.next().getValue().toString();
        }
    }

	private Object getEntity(String id, String columnFamily, Class conversionClass, String[] visibility) throws IOException {
		return getEntity(id, columnFamily, conversionClass, new Authorizations(visibility));
	}

	private Object getEntity(String id, String entityPrefix, Class conversionClass, Authorizations auths) throws IOException {
        final Gson gson = new Gson();
        String json = getEntityString(id, entityPrefix, auths);
        return gson.fromJson(json, conversionClass);
    }

}

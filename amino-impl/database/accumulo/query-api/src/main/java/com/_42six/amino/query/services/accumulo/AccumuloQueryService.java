package com._42six.amino.query.services.accumulo;

import com._42six.amino.bitmap.iterators.BitmapANDIterator;
import com._42six.amino.bitmap.iterators.ReverseByBucketCombiner;
import com._42six.amino.bitmap.iterators.ReverseFeatureCombiner;
import com._42six.amino.common.*;
import com._42six.amino.common.accumulo.IteratorUtils;
import com._42six.amino.common.bigtable.TableConstants;
import com._42six.amino.common.bitmap.AminoBitmap;
import com._42six.amino.common.bitmap.BitmapUtils;
import com._42six.amino.common.entity.Hypothesis;
import com._42six.amino.common.entity.HypothesisFeature;
import com._42six.amino.common.entity.QueryEntry;
import com._42six.amino.common.entity.QueryResult;
import com._42six.amino.common.query.requests.auditing.AminoAuditRequest;
import com._42six.amino.common.query.requests.bta.BtaByValuesRequest;
import com._42six.amino.common.translator.FeatureFactTranslatorImpl;
import com._42six.amino.common.translator.FeatureFactTranslatorInt;
import com._42six.amino.common.util.concurrent.FlaggableCallable;
import com._42six.amino.common.util.concurrent.TimedUserExecutionService;
import com._42six.amino.query.exception.EntityNotFoundException;
import com._42six.amino.query.services.AminoQueryService;
import com._42six.amino.query.services.audit.AuditorServiceInt;
import com._42six.amino.query.services.auth.DefaultVisibilityTranslator;
import com._42six.amino.query.services.auth.VisibilityTranslatorInt;
import com._42six.amino.query.stats.QueryStatisticsMap;
import com._42six.amino.query.util.FirstLastTracker;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.*;
import org.apache.accumulo.core.iterators.RegExIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Used to query Accumulo for the results of a hypothesis.
 *
 * @author Amino Team
 *
 */
public class AccumuloQueryService implements AminoQueryService {
	
	private static final Logger log = Logger.getLogger(AccumuloQueryService.class);

	public AuditorServiceInt auditorService;
	public VisibilityTranslatorInt visibilityTranslator;
    public AccumuloPersistenceService persistenceService;
    public AccumuloMetadataService metadataService;
    public AccumuloGroupService groupService;
    public final FeatureFactTranslatorInt translator;
	public TimedUserExecutionService timedUserExecutionService;

    public String bitLookupTable = "BitmapOutput_bitLookup";
    public String byBucketTable = "BitmapOutput_byBucket";
    public String resultsTable = "amino_query_result";
	public String featureLookupTable = "amino_feature_lookup";
	public String groupHypothesisLUT = "amino_group_hypothesisLUT";

    public String reverseByBucketTable = "amino_reverse_bitmap_byBucket";
    public String reverseFeatureLookupTable = "amino_reverse_feature_lookup";

	private boolean logQueryStats = false;

	public String auditSystemTo = "";
	public String auditVisibility = "";

    /**
     * Constructs a new AccumuloQueryService. Use of this constructor acknowledges that you will manually set the persistenceService and metadataService properties before calling any methods.
     */
    public AccumuloQueryService() {
        this.translator = new FeatureFactTranslatorImpl();
        this.visibilityTranslator = new DefaultVisibilityTranslator();
    }
	
    /**
     * Constructs a new AccumuloQueryService using the provided services for underlying data.
     * @param persistenceService        Service for persisting the data
     * @param metadataService  Service for querying metadata information
     */
    public AccumuloQueryService(AccumuloPersistenceService persistenceService, AccumuloMetadataService metadataService) {
        this.persistenceService = persistenceService;
        this.metadataService = metadataService;
        this.translator = new FeatureFactTranslatorImpl();
        this.visibilityTranslator = new DefaultVisibilityTranslator();
    }

	public void setTimedUserExecutionService(TimedUserExecutionService service){
		this.timedUserExecutionService = service;
	}
	
	public void setGroupHypothesisLUT(String lut){
		this.groupHypothesisLUT = lut;
	}
	
	public void setAuditorService(AuditorServiceInt auditorService){
		this.auditorService = auditorService;
	}
	
	public void setVisibilityTranslator(VisibilityTranslatorInt visibilityTranslator) {
		this.visibilityTranslator = visibilityTranslator;
	}

    public void setGroupService(AccumuloGroupService groupService){
        this.groupService = groupService;
    }

    public void setPersistenceService(AccumuloPersistenceService persistenceService) {
        this.persistenceService = persistenceService;
    }

    public void setMetadataService(AccumuloMetadataService metadataService) {
        this.metadataService = metadataService;
    }

    public void setBitLookupTable(String bitLookupTable) {
        this.bitLookupTable = bitLookupTable;
    }

    public void setByBucketTable(String byBucketTable) {
        this.byBucketTable = byBucketTable;
    }

    public void setLogQueryStats(boolean logQueryStats) {
        this.logQueryStats = logQueryStats;
    }

    public void setResultsTable(String resultsTable) {
        this.resultsTable = resultsTable;
    }

    public void setReverseByBucketTable(String table){
        this.reverseByBucketTable = table;
    }

    public void setReverseFeatureLookupTable(String table){
        this.reverseFeatureLookupTable = table;
    }

	public void setFeatureLookupTable(String featureLookupTable) {
		this.featureLookupTable = featureLookupTable;
	}
	
	public void setAuditSystemTo (String auditSystemTo){
		this.auditSystemTo = auditSystemTo;
	}
	
	public void setAuditVisibility (String auditVisibility){
		this.auditVisibility = auditVisibility;
	}

    /**
     * Gets the all of the results for a given user
     * @param userId     The user to get the results for
     * @param visibility The Accumulo visibilities for reading the results
     * @return All of the userId's results
     * @throws TableNotFoundException
     */
    public List<QueryResult> listResults(String userId, String[] visibility) throws IOException {
        return listResults(0L ,Long.MAX_VALUE, userId, visibility);
    }

    /**
     * Gets a batch of results for a given user. NOTE: All of the results are fetched by the server and this just iterates
     * until it gets to the start. Thus, this is only useful if there are too many results to return at once.
     *
     * @param start  The number of the result to
     * @param count     How many to return.
     * @param userid     The userid to search on.
     * @param visibility A list of string corresponding to allowed visibilities for the user.
     * @return results for the userid
     * @throws TableNotFoundException
     */
    public List<QueryResult> listResults(Long start, Long count, String userid, String[] visibility) throws IOException {
        checkState();
        Scanner scan = null;
        try {
            scan = persistenceService.createScanner(resultsTable, new Authorizations(visibility));
        } catch (TableNotFoundException e) {
            throw new IOException(e);
        }
        // Grab only the Rows that start with userid|
        scan.setRange(new Range(userid));

		Long currentPosition = 0L;
		List<QueryResult> entities = new ArrayList<QueryResult>();
		QueryResult activeEntity = null;
		Iterator<Map.Entry<Key,Value>> itr = scan.iterator();
		
		// Iterate to where we need to start
		// TODO We might be doing this for paging on the UI, but this would be very inefficient as we get to later pages.
		while(itr.hasNext() && currentPosition < start){
			itr.next();
			currentPosition++;
		}
		
		while (itr.hasNext() && (entities.size() < count) ) {
			Map.Entry<Key,Value> entry = itr.next();
			Key key = entry.getKey();
			
			// TODO Investigate the bizarre case where even though itr.hasNext() is true, but itr.next() returns an Entry that has a null key
			if(key == null)
				break;
			
			String owner = key.getRow().toString();
			String resultId = key.getColumnFamily().toString();
			String cq = key.getColumnQualifier().toString();
			if ((activeEntity == null) || resultId.compareTo(activeEntity.id) != 0) {
                final QueryResult qr = new QueryResult();
                qr.owner = owner;
                qr.id = resultId;
                qr.timestamp = Long.MAX_VALUE - Long.parseLong(resultId);
                activeEntity = qr;
				entities.add(activeEntity);
			}
			
			// We don't want to return the result_set or hypothesis_at_runtime
			if(cq.compareTo("result_set") != 0 && cq.compareTo("hypothesis_at_runtime") != 0) {
				addResultComponent(activeEntity, cq,  entry.getValue().toString());
			}
			currentPosition++;
		}
		return entities;
    }

	/**
	 * Fetches the QueryResult
	 * @param requester The ID of the person making the request
	 * @param resultOwner The owner of the QueryResult
	 * @param queryId The ID of the QueryResult to fetch
	 * @param visibility The authorization Strings
	 */
    public QueryResult getResult(String requester, String resultOwner, String queryId, String[] visibility) throws Exception {
        checkState();
		MorePreconditions.checkNotNullOrEmpty(requester);
		MorePreconditions.checkNotNullOrEmpty(resultOwner);
		MorePreconditions.checkNotNullOrEmpty(queryId);
		Preconditions.checkNotNull(visibility);

		Authorizations auths = new Authorizations(visibility);
		
		Set<String> groups = null;
					
		// If the requester isn't the owner, make sure they can see the hypothesis via groups
		if(requester.compareTo(resultOwner) != 0){
			groups = groupService.getGroups(requester, auths);
			if(groups == null || groups.size() <= 0){
				throw new EntityNotFoundException("user <" + requester + "> is not allowed to view this result");
			}
		}
		
		// Grab the Query Result
		QueryResult result = new QueryResult();
		result.id = queryId;
		result.owner = resultOwner;
		result.timestamp = Long.MAX_VALUE - Long.parseLong(queryId);
		
		Scanner scan = persistenceService.createScanner(resultsTable, auths);
		scan.setRange(new Range(resultOwner));
		scan.fetchColumnFamily(new Text(queryId));
		
        Iterator<Map.Entry<Key, Value>> itr = scan.iterator();
        if (!itr.hasNext()) {
            throw new EntityNotFoundException("No results were found for the query <"+queryId+"> with owner <"+resultOwner+">");
        }
		
		// Serialize the result
        while (itr.hasNext()) {
            Map.Entry<Key, Value> entry = itr.next();
            String cq = entry.getKey().getColumnQualifier().toString();
            String value = entry.getValue().toString();
            addResultComponent(result, cq, value);
        }
		
		//  Make sure that the requester can see this result
		if(groups != null){
			BatchScanner lutScanner = null;
			try{
				// See what groups can see this particular hypothesis
				lutScanner = persistenceService.createBatchScanner(groupHypothesisLUT, auths);
                final ArrayList<Range> groupRanges = new ArrayList<Range>(groups.size());
                for(String group : groups){
                    groupRanges.add(new Range(group));
                }
                lutScanner.setRanges(groupRanges);
                lutScanner.fetchColumn(new Text(resultOwner), new Text(result.hypothesisid));
				
				// If there were no results than none of requester's groups can see the hypothesis
				if(!scan.iterator().hasNext())
				{
					throw new EntityNotFoundException("user <"+requester+"> is not allowed to view this result");
				}				
			}  catch (Exception ex){
				log.error(ex);
				throw ex;
			} finally {
                if(lutScanner != null){
				    lutScanner.close();
                }
			}
		}
		
        return result;
    }

    /**
     * "Execute" a hypothesis.  Does not timeout.
     *
     * @param owner         The ownerId of the result
     * @param hypothesisId  The ID of the hypothesis to "run"
     * @param maxResults    The maximum number of results to generate before stopping
     * @param justification The justification string for why the query was being made
     * @param userId        The ID of the person running the result
     * @param visibility    The Accumulo visibility strings
     * @return A QueryResult with the results of the Hypothesis
     */
    public QueryResult createResult(String owner, String hypothesisId, Integer maxResults, String justification, String userId, String[] visibility)
            throws InterruptedException, ExecutionException, TimeoutException {
        return createResult(owner, hypothesisId, maxResults, justification, userId, visibility, 0, TimeUnit.MINUTES);
    }

    /**
     * "Execute" a hypothesis.
     *
     * @param owner         The ownerId of the result
     * @param hypothesisId  The ID of the hypothesis to "run"
     * @param maxResults    The maximum number of results to generate before stopping
     * @param justification The justification string for why the query was being made
     * @param userId        The ID of the person running the result
     * @param visibility    The Accumulo visibility strings
     * @param timeout       The amount of time to run before giving up, in minutes.  Pass 0 to not time out.
     * @return A QueryResult with the results of the Hypothesis
     */
    public QueryResult createResult(String owner, String hypothesisId, Integer maxResults, String justification, String userId, String[] visibility, long timeout)
            throws InterruptedException, ExecutionException, TimeoutException {
        return createResult(owner, hypothesisId, maxResults, justification, userId, visibility, timeout, TimeUnit.MINUTES);
    }

    /**
     * "Execute" a hypothesis.
     *
     * @param owner         The ownerId of the result
     * @param hypothesisId  The ID of the hypothesis to "run"
     * @param maxResults    The maximum number of results to generate before stopping
     * @param justification The justification string for why the query was being made
     * @param userId        The ID of the person running the result
     * @param visibility    The Accumulo visibility strings
     * @param timeout       The amount of time to run before giving up
     * @param units         The unit of time for the timeout parameter
     * @return A QueryResult with the results of the Hypothesis
     */
    public QueryResult createResult(String owner, String hypothesisId, Integer maxResults, String justification, String userId, String[] visibility,
		long timeout, TimeUnit units) throws InterruptedException, ExecutionException, TimeoutException {
        checkState();
		Callable<QueryResult> call = new CreateQueryResultCall(owner, hypothesisId, maxResults, justification, userId, visibility);
		return timedUserExecutionService.timedCall(call, owner, timeout, units, false);
    }

    /**
     * Removes a result from the tables
     *
     * @param owner         The ownerId of the result
     * @param id            The QueryResult ID to delete
     * @param visibility    The Accumulo visibility strings
     */
    public void deleteResult(String owner, String id, String[] visibility) throws Exception {
		// Parameter verification to make sure we don't accidently wipe out all of the results
		MorePreconditions.checkNotNullOrEmpty(owner, "Must have owner to delete result.");
		MorePreconditions.checkNotNullOrEmpty(id, "Must have id to delete result.");
		Preconditions.checkNotNull(visibility, "Must have visiibility set to delete result");
		
		final Authorizations auths = new Authorizations(visibility);
		Preconditions.checkNotNull(auths, "Invalid visibilities to delete results");
		
		// Fetch the Hypothesis ID that is associated with this QueryResult
		Scanner resultScanner = persistenceService.createScanner(resultsTable, auths);
		resultScanner.setRange(new Range(owner));
		resultScanner.fetchColumn(new Text(id), new Text("id"));
		final Iterator<Map.Entry<Key, Value>>  iter = resultScanner.iterator();
		
		// If the hypothesis still exists remove this result from the the set of queries for that hypothesis
		if(iter.hasNext()){
			final String hypothesisId = iter.next().getValue().toString();
			final Scanner hypScanner = persistenceService.createScanner(metadataService.hypothesisTable, auths);
			hypScanner.setRange(new Range(owner));
			hypScanner.fetchColumn(new Text(hypothesisId), new Text("queries"));
			final Iterator<Map.Entry<Key, Value>> hypIter = hypScanner.iterator();
			
			// Fetch queries and remove
			if(hypIter.hasNext()){
				final Gson gson = new Gson();
				Map.Entry<Key,Value> e = hypIter.next();
				Key key = e.getKey();
				SortedSet<String> queries = gson.fromJson(e.getValue().toString(), new TypeToken<SortedSet<String>>(){}.getType());
				if(queries.remove(id)){
					// If we successfully removed the id then update the record
					persistenceService.insertRow(key.getRow().toString(), key.getColumnFamily().toString(), key.getColumnQualifier().toString(),
						 key.getColumnVisibility().toString(), gson.toJson(queries), metadataService.hypothesisTable);
				}
			}
		}
		
		// Now remove the QueryResult from the table
		BatchDeleter deleter = null;
		try{
            final AccumuloScanConfig config = new AccumuloScanConfig();
            config.setRow(owner).setColumnFamily(id);
			deleter = persistenceService.createBatchDeleter(resultsTable, auths);
			persistenceService.configureBatchDeleter(deleter, config);
			deleter.delete();
		} catch(Exception ex){
			log.error(ex);
			throw ex;
	    }finally {
            if(deleter != null){
			    // deleter.close(); // TODO This version of BT doesn't implement the close method.....
            }
		}
    }

    /**
     * Finds all existing, visible hypotheses that intersect with the given bucketValues.
     *
     * @param bvRequest All of the parameters
     * @return Hypotheses that intersect with the bucketvalues
     */
	public List<Hypothesis> getHypothesesByBucketValues(BtaByValuesRequest bvRequest) throws InterruptedException, ExecutionException, TimeoutException {
		Callable<List<Hypothesis>> call = new FindHypothesesByBucketValuesCall(bvRequest);
		return timedUserExecutionService.timedCall(call, bvRequest.getAuditInfo().getDn(), bvRequest.getTimeout(), bvRequest.getTimeoutUnits(), false);
	}
	
//	/**
//	 * Finds all existing, visible hypotheses that intersect with the bucketValues.
//	 *
//	 * @param datasourceId The ID of the datasource to search through
//	 * @param bucketId The ID of the bucket to search through
//	 * @param bucketValues The collection of bucketValues to use to find Hypotheses
//	 * @param owner The owner of the Hypotheses to find/check
//	 * @param visibility Accumulo visibilities
//	 * @param justification Justification for auditing
//	 * @param hypotheses (optional) If passed in, the scanning will only happen on the given hypotheses (no db fetching for Hypotheses)
//	 * @param timeout (optional) The amount of time to wait before timing out. If <= 0, then default will be used
//	 * @param units (optional) The TimeUnit of the timeout. Defaults to seconds
//	 * @return Hypotheses that match the bucketValues
//	 */
//	public List<Hypothesis> getHypothesesByBucketValues(
//		String datasourceId, String bucketId, Collection<String> bucketValues, String owner, String[] visibility, String justification,
//		Collection<Hypothesis> hypotheses = null, long timeout =0, TimeUnit units=TimeUnit.SECONDS)
//	{
//		Callable<List<Hypothesis>> call = new FindHypothesesByBucketValuesCall(datasourceId, bucketId, bucketValues, owner, visibility, justification,  hypotheses);
//		return timedUserExecutionService.timedCall(call, owner, timeout, units, false)
//	}
		
	/**
	 * Finds all visible hypotheses that intersect with the bucketValues.  
	 * @param req The parameters
	 * @param keepWorking Flag to tell us to stop working if the method is taking too long (Needed because of stupid Accumulo Bug)
	 * @return Hypotheses that match the bucketValues
	 */
	private List<Hypothesis> findHypothesesByBucketValues(BtaByValuesRequest req, AtomicBoolean keepWorking) throws Exception {
		// Verify the parameters
        req.verify();
		final String datasourceId = MorePreconditions.checkNotNullOrEmpty(req.getDatasourceId(), "Must provide datasourceId");
		final String bucketId = MorePreconditions.checkNotNullOrEmpty(req.getBucketId(), "Must provide bucketId");
		final Set<String> bucketValues = (Set<String>) MorePreconditions.checkNotNullOrEmpty(req.getBucketValues(), "Must Provide bucketValues");
        final AminoAuditRequest auditInfo = Preconditions.checkNotNull(req.getAuditInfo(), "Must provide auditing info");
		MorePreconditions.checkNotNullOrEmpty(auditInfo.getDn(), "Must provide requester");
		MorePreconditions.checkNotNullOrEmpty(auditInfo.getJustification(), "Must provide justification");
        final String[] tokens = Preconditions.checkNotNull(req.getSecurityTokens(), "Security tokens were missing");
		final Authorizations auths = Preconditions.checkNotNull(new Authorizations(tokens), "Authorizations were null");
		
		// A mapping of bucketValue to a list of Hypothesis ID's that hit
		List<Hypothesis> matchedValues = new ArrayList<Hypothesis>();
		Collection<Hypothesis> hypothesesToSearch;

		// Lookup values
		//final Integer hashCount = metadataService.getHashCount()
		final Integer shardCount = metadataService.getShardCount();
		final BucketMetadata  bucket = Preconditions.checkNotNull(metadataService.getBucket(bucketId, auths),
			"Could not find bucket with id '%s, Authorizations %s '", bucketId, auths);
						
		// Audit the query
		List<HypothesisFeature> auditFeatures = new ArrayList<HypothesisFeature>();
		if(req.getHypotheses() != null){
            for(Hypothesis h : req.getHypotheses()){
                for(HypothesisFeature it : h.hypothesisFeatures){
					auditFeatures.add(it);
				}
			}
		}
		
		if(req.getHypotheses() != null ){
			Preconditions.checkArgument(auditFeatures.size() > 0, "Hypotheses provided but there were no features");
		}
		auditQuery(bucket.name, auditInfo, auths, auditFeatures, bucketValues);

		// Restrict the Hypotheses to check if the optional collection was passed in
		if(req.getHypotheses() != null){
			hypothesesToSearch = req.getHypotheses();
		} else {
			// Create potential Hypotheses if none were passed in
			hypothesesToSearch = new ArrayList<Hypothesis>();
			
			// Find the hypotheses that the user can see.  Restrict to features as that's all we care about
			final Scanner hypothesisFeaturesScanner = persistenceService.createScanner(metadataService.hypothesisTable, auths);
			hypothesisFeaturesScanner.setRange(new Range(auditInfo.getDn()));
			hypothesisFeaturesScanner.setScanIterators(30, RegExIterator.class.getCanonicalName(), "cqFilterIterator");
			hypothesisFeaturesScanner.setScanIteratorOption("cqFilterIterator", "colqRegex", "features");
						
			// Get all of the HypothesisFeatures of the Hypotheses we can see
            for(Map.Entry<Key, Value> hypothesisFeatureRow : hypothesisFeaturesScanner){
				// Check to see if we were interrupted and if so. give up.
				if(!keepWorking.get()) { return null; }

                HashSet<HypothesisFeature> features = new Gson().fromJson(hypothesisFeatureRow.getValue().toString(),
                        new TypeToken<HashSet<HypothesisFeature>>(){}.getType());
//				ArrayList<HypothesisFeature> features = new ArrayList<HypothesisFeature>();
//				// def serializedFeatureArray = new JsonSlurper().parseText(hypothesisFeatureRow.getValue().toString());
//				serializedFeatureArray.each{
//					features.add(new HypothesisFeature(it));
//				}
                final Hypothesis h = new Hypothesis();
                h.id = hypothesisFeatureRow.getKey().getColumnFamily().toString();
                h.hypothesisFeatures = features;
                h.queries = null;
				hypothesesToSearch.add(h);
			}
		}
			
		BatchScanner bucketValuesScanner = null;
		try{	
			bucketValuesScanner = persistenceService.createBatchScanner(byBucketTable, auths);
			
			// For each hypothesis, check it's features against the bucket values we are looking for
            for(Hypothesis hypothesis : hypothesesToSearch){
				// Check to see if we were interrupted and if so. give up.
				if(!keepWorking.get()) { return null; }

				boolean scanValues = configureByValueScanner(bucketValuesScanner, bucket.name, hypothesis.hypothesisFeatures, // ADDED <===========================
                        datasourceId + ":" + bucket.name, shardCount, auths, bucketValues);

				if(scanValues){
                    for(Map.Entry<Key,Value> bvRow : bucketValuesScanner){
                        final Hypothesis h = new Hypothesis();
                        h.id = hypothesis.id;
                        h.bucketValue = bvRow.getKey().getColumnFamily().toString();
                        h.hypothesisFeatures = null;
                        h.queries = null;
						matchedValues.add(h);
					}
				}
			} 
		} catch(Exception ex) {
			log.error(ex);
			throw ex;
		} finally {
            if(bucketValuesScanner != null){
			    bucketValuesScanner.close();
            }
		}
		return matchedValues;
	}

	private Collection<Hypothesis> createNonPersistedHypotheses(
		String datasourceid, String bucketid, Collection<String> bucketValues, String[] visibility, String userid, String justification, 
		AtomicBoolean keepRunning, List<String> featureIds) throws Exception {
		
		// Verify the parameters
		MorePreconditions.checkNotNullOrEmpty(datasourceid);
		MorePreconditions.checkNotNullOrEmpty(bucketid);
		MorePreconditions.checkNotNullOrEmpty(bucketValues);
		MorePreconditions.checkNotNullOrEmpty(userid);
		MorePreconditions.checkNotNullOrEmpty(justification);
		Preconditions.checkNotNull(visibility);
		
		final long startTime = System.currentTimeMillis();

		final BucketMetadata bucket = metadataService.getBucket(bucketid, visibility);
		final Integer  hashCount = metadataService.getHashCount();
		final Integer shardcount = metadataService.getShardCount();
		final DatasourceMetadata dataSource = metadataService.getDataSource(datasourceid, visibility);
		
		// Create the map which will hold all of the Hypothesis's that we create.  This is a lookup table of
		// bucketValues we are looking for to Hypothesis results. 
        HashMap<String, Hypothesis> hypothesisMap = new HashMap<String, Hypothesis>(bucketValues.size());
		
		// Every bucketvalue will have a hypothesis, regardless if there were any matching features
        for(String it : bucketValues){
		    // Create a Hypothesis for this bucket value
			Hypothesis hypothesis = new Hypothesis();
			hypothesis.bucketid = bucketid;
			hypothesis.bucketValue = it;
			hypothesis.datasourceid = datasourceid;
			hypothesis.justification = justification;
			hypothesis.name = "Generated for " + it;
			hypothesis.hypothesisFeatures = new HashSet<HypothesisFeature>();
			hypothesis.created = System.currentTimeMillis();
			hypothesisMap.put(it,  hypothesis);
		}

		// Audit the query		
		final Authorizations auths = new Authorizations(visibility);
		final AminoAuditRequest auditReq = new AminoAuditRequest();
		auditReq.setDn(userid);
		auditReq.setJustification(justification);
		auditQuery(bucket.name, auditReq, auths, Collections.EMPTY_LIST, bucketValues);
				
		// Create the BatchScanners for looking up all of the values that we will need
		BatchScanner byBucketBatchScanner = null;
		QueryStatisticsMap bucketStats = null;
		Set<Range> featureRanges = new HashSet<Range>();
		HashMap<Key, HashSet<String>>  featureFactToBucketValueMap;
		try{
			 byBucketBatchScanner =  persistenceService.createBatchScanner(byBucketTable, auths);
			
			// For each of the bucketValues that we are looking for, create a Range that will correspond to this bucketValue for lookup
			ArrayList<Range> bucketRanges = new ArrayList<Range>();
            for(String bv : bucketValues){
                bucketRanges.addAll(
                    persistenceService.generateRanges(
                        new AccumuloScanConfig().
                            setStartRow(dataSource.id + ":" + bucket.name).
                            setEndRow(dataSource.id + ":" + bucket.name).
                            setShardcount(shardcount).
                            setStartColumnQualifier("0").
                            setEndColumnQualifier(hashCount.toString()).
                            setStartColumnFamily(bv).
                            setEndColumnFamily(bv)
                    )
                );
			}
			
			// Now that we know what we are all looking for, set the Ranges on the scanner
			byBucketBatchScanner.setRanges(bucketRanges);
			
			bucketStats = new QueryStatisticsMap();
			
			// Scan through all of the bucket results and figure out which features we are interested in 
			featureFactToBucketValueMap = new HashMap<Key, HashSet<String>>();

            for(Map.Entry<Key, Value> it : byBucketBatchScanner){
				bucketStats.increment();
				final AminoBitmap featureFactValueIndexes = BitmapUtils.fromValue(it.getValue());
				final Iterator<Integer> bitmapIterator = featureFactValueIndexes.iterator();
				final String bucketValue = it.getKey().getColumnFamily().toString();
				final String salt = it.getKey().getColumnQualifier().toString();
				
				// Find all of the [featureID/salt/buckets] that we are interested in and make note of which bucketValues are interested in them
				while(bitmapIterator.hasNext()) {										
					// Abort if we were interrupted
					if(!keepRunning.get()){
						return null;
					}
					
					final String featureValueIndex = bitmapIterator.next().toString();
					final Key featureLookupKey = new Key(featureValueIndex, salt + "#" + bucket.name);
					featureRanges.add(new Range(featureLookupKey, featureLookupKey.followingKey(PartialKey.ROW_COLFAM)));
	
					// If there's not already an entry, add a new HashSet with this featureId
					if(!featureFactToBucketValueMap.containsKey(featureLookupKey)){
						HashSet<String> hs = new HashSet<String>();
						hs.add(bucketValue);
						featureFactToBucketValueMap.put(featureLookupKey, hs);
					}
					else {
						// Note that this bucket value is interested in this feature Fact
						featureFactToBucketValueMap.get(featureLookupKey).add(bucketValue);
					}
				}	
			}
		} catch (Exception ex){
			log.error(ex);
			throw ex;
		} finally {
            if(byBucketBatchScanner != null){
			    byBucketBatchScanner.close();
            }
			bucketStats.endTime();
		}
		
		if(featureRanges.size() == 0) {
			// There were no features to add, so no need to go any further
			return hypothesisMap.values();
		}


        final Map<String, Map<HypothesisFeature, Integer>> bucketValuesFeatureCounts = new HashMap<String, Map<HypothesisFeature, Integer>>();
        final Text featureIdText = new Text();
		final Map<String, HypothesisFeature> hypoFeatureCache = new HashMap<String, HypothesisFeature>();
		BatchScanner featureBatchScanner = null;
		QueryStatisticsMap featureStats = null;
		try{
            // Set the ranges for the feature scanner to efficiently look up the features we are interested in
            featureBatchScanner = persistenceService.createBatchScanner(featureLookupTable, auths);
            featureBatchScanner.setRanges(featureRanges);

            // Now that we know what feature facts we are interested in, go scan them and create our hypothesis features
            featureStats = new QueryStatisticsMap();
            for(Map.Entry<Key, Value> scanEntry : featureBatchScanner){
                // Abort if we were interrupted
                if(!keepRunning.get()){
                    return null;
                }

                featureStats.increment();
                scanEntry.getKey().getColumnQualifier(featureIdText);
                final String featureId = featureIdText.toString();
                final String featureValue = scanEntry.getValue().toString();

                // Check to see if we are filtering featureIds or if we are accepting them all (null)
                if(featureIds == null || (featureIds.contains(featureId))){
                    // TODO: We might be able to speed this part up
                    // Cache the FeatureMetadata as we are only changing its value
                    if(!hypoFeatureCache.containsKey(featureId)){
                        final FeatureMetadata metadataFeature = metadataService.getFeature(featureId, visibility);
                        if(metadataFeature == null){
                            log.warn("Could not find feature with featureId " + featureId);
                            continue;
                        }

                        final HypothesisFeature hf = new HypothesisFeature();
                        hf.featureMetadataId = featureId;
                        hf.include = true;
                        hf.btVisibility = metadataFeature.btVisibility;
                        hf.visibility = metadataFeature.visibility;
                        hf.type = metadataFeature.type;
                        hypoFeatureCache.put(featureId, hf);
                    }

                    // Create a new HypothesisFeature from the cached HypothesisFeature stub
                    final HypothesisFeature hypoFeature = new HypothesisFeature(hypoFeatureCache.get(featureId));

                    // Set the appropriate values
                    if(FeatureFactType.numericIntervalTypes.contains(hypoFeature.type)) {
                        log.debug("Config RATIO with value $featureValue");
                        Double convertedVal = translator.toRatio(featureValue);
                        hypoFeature.min = convertedVal;
                        hypoFeature.max = convertedVal;
                    } else if(hypoFeature.type.compareTo("NOMINAL") == 0){
                        log.debug("Config NOMINAL with $featureValue");
                        hypoFeature.value = featureValue;
                    } else if (FeatureFactType.dateIntervalTypes.contains(hypoFeature.type)) {
                        log.debug("Config DATE with $featureValue");
                        Long convertedVal = translator.toDate(featureValue);
                        hypoFeature.timestampFrom = convertedVal;
                        hypoFeature.timestampTo = convertedVal;

                    } else {
                        throw new RuntimeException("Don't know how to convert feature type ${hypoFeature.type}");
                    }

                    Key scanKey = scanEntry.getKey();
                    Key indexKey = new Key(scanKey.getRow(), scanKey.getColumnFamily());

                    // For every bucketValue that was interested in this featureFact, add the potential HypothesisFeature
                    // to the counting Map for that bucketValue (needed for filtering hash colosions)
                    for(String it : featureFactToBucketValueMap.get(indexKey)){
                        Map<HypothesisFeature, Integer> map = bucketValuesFeatureCounts.get(it);
                        if(map == null){
                            map = new HashMap<HypothesisFeature, Integer>();
                            bucketValuesFeatureCounts.put(it, map);
                        }

                        // TODO Make this more efficient
                        if(map.containsKey(hypoFeature)){
                            Integer i = map.get(hypoFeature);
                            map.put(hypoFeature, i + 1);
                        } else {
                            map.put(hypoFeature, 1);
                        }

                        //hypothesisMap.get(it).hypothesisFeatures.add(hypoFeature)
                    }
                }
            }

            // Now that we have all of potential HypothesisFeatures, go through and add them to the hypothesis, making
            // sure to eliminate any that might have been false positives
            for (Map.Entry<String, Map<HypothesisFeature, Integer>> entry : bucketValuesFeatureCounts.entrySet()) {
                String bucketValue = entry.getKey();
                Map<HypothesisFeature, Integer> featureCountMap = entry.getValue();
                for(Map.Entry<HypothesisFeature, Integer> featureCount : featureCountMap.entrySet()){
                    // If the count isn't exactly the same as the number of hashes then it's a false positive
                    if(featureCount.getValue().compareTo(hashCount) == 0){
                        hypothesisMap.get(bucketValue).hypothesisFeatures.add(featureCount.getKey());
                    }
                }
            }
		} catch (Exception ex){
			log.error(ex);
			throw ex;
		} finally {
            if(featureBatchScanner != null){
                featureBatchScanner.close();
            }
            if(featureStats != null){
                featureStats.endTime();
            }
		}
		
		// Now we need to create the visibility strings for all of the Hypothesis and HypothesisFeatures
		// TODO make this into some kind of class for doing this - It is nearly dup'd in the RestfulHypothesisController
        for(Hypothesis hypothesis : hypothesisMap.values()){
			// Abort if we were interrupted
			if(!keepRunning.get()){
				return null;
			}
			
			HashSet<String> btVisSet = new HashSet<String>();
			HashSet<String> hrVisSet = new HashSet<String>();
			for(HypothesisFeature hf : hypothesis.hypothesisFeatures){
				hrVisSet.add(hf.visibility);
				btVisSet.add(hf.btVisibility);
			}

			hypothesis.visibility = visibilityTranslator.combineHumanReadable(hrVisSet);
			// If there is only one value don't bother with (), otherwise combine everything together with () & () & ...
			hypothesis.btVisibility = (btVisSet.size() == 1) ? btVisSet.iterator().next() :  "(" + Joiner.on(")&(").join(btVisSet) + ")";
		}
		
		if (logQueryStats) {
			log.info ("createNonPersisted...() elapsed=[" +
				Math.round((System.currentTimeMillis() - startTime)/1000) +
				"], bucketCount=[" + String.valueOf(bucketValues.size()) +
				"], resultCount=[" + String.valueOf(hypothesisMap.values().size()) +
				"], byBucketScanner=[" + bucketStats +
				"], featureScanner=[" + featureStats +
				"].");
		}
		
		// Return all of the Hypothesis's that we created
		return hypothesisMap.values();
	}


    /**
     * Creates Hypotheses from the given bucket values.  Each of the hypotheses will be a Hypothesis that represents all
     * of the features that would match that bucket value for the given datasource and bucket.  The Hypotheses are only
     * created, they are not persisted to the database.
     *
     * @param datasourceid The datasource to check the bucket values against
     * @param bucketid The bucket to check the bucket values against
     * @param visibility The Accumulo visibilities
     * @param userid the DN of the person making the request
     * @param justification The justification string for auditing
     * @param featureIds (optional) The featureIds that we are interested in. If provided, all others will be excluded from the results
     * @return A Collection of Hypothesis, one for each bucket value
     */
    public Collection<Hypothesis> createNonPersistedHypothesisListForBucketValue(
            String datasourceid, String bucketid, Collection<String> bucketValues, String[] visibility, String userid, String justification,
            List<String> featureIds) throws InterruptedException, ExecutionException, TimeoutException {
        return createNonPersistedHypothesisListForBucketValue(datasourceid, bucketid, bucketValues, visibility, userid, justification, featureIds, 0, TimeUnit.SECONDS);
    }

    /**
     * Creates Hypotheses from the given bucket values.  Each of the hypotheses will be a Hypothesis that represents all
     * of the features that would match that bucket value for the given datasource and bucket.  The Hypotheses are only
     * created, they are not persisted to the database.
     *
     * @param datasourceid The datasource to check the bucket values against
     * @param bucketid The bucket to check the bucket values against
     * @param visibility The Accumulo visibilities
     * @param userid the DN of the person making the request
     * @param justification The justification string for auditing
     * @return A Collection of Hypothesis, one for each bucket value
     */
    public Collection<Hypothesis> createNonPersistedHypothesisListForBucketValue(
            String datasourceid, String bucketid, Collection<String> bucketValues, String[] visibility, String userid, String justification) throws InterruptedException, ExecutionException, TimeoutException {
        return createNonPersistedHypothesisListForBucketValue(datasourceid, bucketid, bucketValues, visibility, userid, justification, null, 0, TimeUnit.SECONDS);
    }

    /**
     * Creates Hypotheses from the given bucket values.  Each of the hypotheses will be a Hypothesis that represents all
     * of the features that would match that bucket value for the given datasource and bucket.  The Hypotheses are only
     * created, they are not persisted to the database.  This call takes optional paramters to abort afer a specified
     * timeout value.
     *
     * @param datasourceid The datasource to check the bucket values against
     * @param bucketid The bucket to check the bucket values against
     * @param visibility The Accumulo visibilities
     * @param userid the DN of the person making the request
     * @param justification The justification string for auditing
     * @param featureIds (optional) The featureIds that we are interested in. If provided, all others will be excluded from the results
     * @param timeout (optional) The amount of time to wait before timing out. If <= 0, then default will be used
     * @param units (optional) The TimeUnit of the timeout. Defaults to seconds
     * @return A Collection of Hypothesis, one for each bucket value
     */
	public Collection<Hypothesis> createNonPersistedHypothesisListForBucketValue(
		String datasourceid, String bucketid, Collection<String> bucketValues, String[] visibility, String userid, String justification, 
		 List<String> featureIds, long timeout, TimeUnit units) throws InterruptedException, ExecutionException, TimeoutException {
		 
		 MorePreconditions.checkNotNullOrEmpty(datasourceid, "Must provide datasourceid");
		 MorePreconditions.checkNotNullOrEmpty(bucketid, "Must provide bucketid");
		 MorePreconditions.checkNotNullOrEmpty(bucketValues, "Must provide bucketValues");
		 Preconditions.checkNotNull(visibility, "Must provide BT visibility strings");
		 MorePreconditions.checkNotNullOrEmpty(userid, "Must provide userid");
		 MorePreconditions.checkNotNullOrEmpty(justification, "Must provide justification");
		 
	   Callable<Collection<Hypothesis>> call = new CreateHypothesesCall(datasourceid, bucketid, bucketValues, visibility, userid, justification, featureIds);
	   return timedUserExecutionService.timedCall(call, userid, timeout, units, false);
	}

    /**
     * Fetches the number of times a particular feature is found in a bucket
     * @param featureMetadataId The ID of the feature to count
     * @param bucketName The name of the bucket
     * @param beginRange The start of the range (inclusive)
     * @param endRange   The end of the range (inclusive)
     * @param visibility The Accumulo visibilities
     * @return The number of instances of the feature
     * @throws Exception
     */
    public Integer getCountForHypothesisFeature(String featureMetadataId, String bucketName, String beginRange, String endRange, String[] visibility) throws Exception {
		MorePreconditions.checkNotNullOrEmpty(featureMetadataId);
		MorePreconditions.checkNotNullOrEmpty(bucketName);
		MorePreconditions.checkNotNullOrEmpty(beginRange);
		MorePreconditions.checkNotNullOrEmpty(endRange);
		Preconditions.checkNotNull(visibility);
		
		final FeatureMetadata feature = metadataService.getFeature(featureMetadataId, visibility);
		final String columnQualifier = bucketName + ":COUNT";
		String convertedBeginRange;
		String convertedEndRange;
        AccumuloScanConfig config = new AccumuloScanConfig();
		
		// Convert beginning and end range if need be
		// Should probably be using FeatureFactType.RATIO, etc
		if(feature.type.compareTo("RATIO") == 0) {
			 convertedBeginRange =(Double.parseDouble(beginRange) == Double.MIN_VALUE) ?  translator.fromRatio(Collections.min(feature.min.values())).toString() : translator.fromRatio(Double.parseDouble(beginRange)).toString();
			 convertedEndRange =(Double.parseDouble(endRange) == Double.MAX_VALUE) ?  translator.fromRatio(Collections.max(feature.max.values())).toString() : translator.fromRatio(Double.parseDouble(endRange)).toString();
		} else if (feature.type.compareTo("INTERVAL") ==0) {
			convertedBeginRange =(Double.parseDouble(beginRange) == Double.MIN_VALUE) ?  translator.fromRatio(Collections.min(feature.min.values())).toString() : translator.fromInterval(Double.parseDouble(beginRange)).toString();
			convertedEndRange = (Double.parseDouble(endRange) == Double.MAX_VALUE) ? translator.fromRatio(Collections.max(feature.max.values())).toString() : translator.fromInterval(Double.parseDouble(endRange)).toString();
		} else if (FeatureFactType.dateIntervalTypes.contains(feature.type)) {
                        convertedBeginRange = translator.fromDate(Long.parseLong(beginRange)).toString();
                        convertedEndRange = translator.fromDate(Long.parseLong(endRange)).toString();
		} else {
			convertedBeginRange = beginRange;
			convertedEndRange =  endRange;
		}
			
		if(feature.type.compareTo("NOMINAL") == 0) {
			config.setRow(featureMetadataId);
            config.setColumnFamily(convertedBeginRange);
            config.setColumnQualifier(columnQualifier);
		}
		else {
			config.setStartRow(featureMetadataId);
			config.setEndRow(featureMetadataId);
            config.setStartColumnFamily(convertedBeginRange);
            config.setEndColumnFamily(convertedEndRange);
            config.setStartInclusive(true);
            config.setEndInclusive(true);
            config.setStartColumnQualifier(columnQualifier);
            config.setEndColumnQualifier(columnQualifier);
            config.setColumnQualifierRegex(columnQualifier);
		}

		// TODO Investigate using a Scanner and a ColumnQualifierFilter instead
		BatchScanner bitLookupScanner = null;
		Integer count = 0;
		try{
	        bitLookupScanner = createBitmapOutputBitLookupScanner(config, new Authorizations(visibility));
            for(Map.Entry<Key, Value> it : bitLookupScanner){
                count += Integer.parseInt(it.getValue().toString());
            }
		}  catch (Exception ex){
			log.error(ex);
			throw ex;
		} finally {
            if(bitLookupScanner != null){
                bitLookupScanner.close();
            }
		}
		
        return count;
    }

    /**
     * Determines the uniqueness score of the given feature for a particular bucket
     * @param featureId The feature to look up
     * @param bucketName The bucket to look in
     * @param count The number of times that the feature exists for that bucket
     * @param visibility The Accumulo visibility strings
     * @return The uniqueness score
     * @throws TableNotFoundException
     */
	public double getUniqueness(String featureId, String bucketName, Integer count, String[] visibility) throws IOException {
		if (count <= 0) {
			return 0;
		}
		
		// Verify parameters
		MorePreconditions.checkNotNullOrEmpty(featureId);
		MorePreconditions.checkNotNullOrEmpty(bucketName);
		Preconditions.checkNotNull(visibility);
		
		final FeatureMetadata metadata = metadataService.getFeature(featureId, visibility);
		
		Long total = metadata.bucketValueCount.get(bucketName);
		if (total == 0) {
			return 0;
		}
		
		BigDecimal uniqueness = new BigDecimal(count.doubleValue() / total.doubleValue());
		String[] check = uniqueness.toPlainString().split("\\.");
		
		if( uniqueness.compareTo(BigDecimal.ONE) >= 0 || uniqueness.compareTo(BigDecimal.ZERO) <= 0 || check.length <= 1 ) {
			return 0;
		} else {			
			Integer zeroes = 0;
            final StringBuilder digits = new StringBuilder("");
			for(char decimal : check[1].toCharArray()) {
				if(decimal == '0') {
					zeroes++;
				}
				else if(digits.length() < 2){
					digits.append(decimal);
				} else {
					break;
				}
			}
			int integers = 100 - Integer.parseInt(digits.toString());
			return Double.parseDouble(zeroes.toString() + "." + String.valueOf(integers));
		}
	}

    /**
     * Determine is a string is comprised of completely numeric characters
     * @param str The string to check
     * @return true if numeric, false otherwise
     */
	public static boolean isNumeric(String str) {
		for (char c : str.toCharArray()) {
			if (!Character.isDigit(c)) 
				return false;
		}
		return true;
	}
	
	/**
	 * Determines what the AminoBitmap arrays should be for fetching values based on the HypothesisFeatures, bucket and authorizations 
	 * @param hypothesisFeatures The HypothesisFeatures to look for values for
	 * @param bucketName The name of the bucket to search against
	 * @param auths Accumulo authorizations
	 * @return Map containing [maskArray, first, last] where maskArray is an array 
	 * of AminoBitmaps, indexed by salt, representing the FeatureFacts, and first/last
	 * represent the first and last seen values for reducing the breath of a byBuckey scan 
	 */
	public BitMaskScanConfig getBitmaskScanInformationForQuery(Collection<HypothesisFeature> hypothesisFeatures, String bucketName,
                                                 Authorizations auths) throws Exception {
		final FirstLastTracker tracker = new FirstLastTracker();
		final Integer hashCount = metadataService.getHashCount();
        final ArrayList<HashMap<String, AminoBitmap>> rangeBitmaps = new ArrayList<HashMap<String, AminoBitmap>>(hashCount);
        final List<BitmapANDIterator.CompareBits> bits = new ArrayList<BitmapANDIterator.CompareBits>(hashCount);
        for(int i = 0; i < hashCount; i++){
            bits.add(new BitmapANDIterator.CompareBits());
            rangeBitmaps.add(new HashMap<String, AminoBitmap>());
        }

        // Check to see if there are any special "Range" features that need to be treated differently
        final HashSet<String> RANGE_IDS = new HashSet<String>();
        for(HypothesisFeature feature : hypothesisFeatures){
            if (FeatureFactType.intervalTypes.contains(feature.type)) {
            //if (RANGE_FEATUREFACT_TYPES.contains(feature.type)) {
                RANGE_IDS.add(feature.featureMetadataId);
            }
        }

        // TODO Test this and also make sure optimized
        final AccumuloScanConfig config = new AccumuloScanConfig();
        config.ranges = new ArrayList<Range>(hypothesisFeatures.size());
        for(HypothesisFeature hf : hypothesisFeatures){
            AccumuloScanConfig conf = createScanConfigForFeature(hf, auths, bucketName);
            config.ranges.add(persistenceService.createRangeForConfig(conf));
        }
		
		BatchScanner maskScan = null;
		try{
			maskScan = createBitmapOutputBitLookupScanner(config, auths);
            for(Map.Entry<Key, Value> it : maskScan){
				final String cq = it.getKey().getColumnQualifier().toString();
				final Value value = it.getValue();
				
				// If the cq is numeric, then we just found the salt and the FeatureFactValueIndex Bitmaps.  Or together
				// masks of similar salts to create the final mask for that salt
				if (isNumeric(cq)) {
                    int hashNumber = Integer.parseInt(cq);
                    String featureId = it.getKey().getRow().toString();

                    // If this feature is a speacial range/ratio feature, then keep it seperate from the other features
                    if(RANGE_IDS.contains(featureId)){
                        final HashMap<String, AminoBitmap> rangeFeatureBits = rangeBitmaps.get(hashNumber);

                        if(!rangeFeatureBits.containsKey(featureId)){
                            rangeFeatureBits.put(featureId, BitmapUtils.fromValue(value));
                        } else {
                            rangeFeatureBits.get(featureId).OR(BitmapUtils.fromValue(value));
                        }
                    } else {
                        // Combine with the rest of the non-range/ratio features
                        final BitmapANDIterator.CompareBits b = bits.get(hashNumber);
                        b.getNonRangeBitmap().OR(BitmapUtils.fromValue(value));
                        b.incrementNonRangeCardinality();
                    }
					continue;
				}
				
				// If the cq doesn't start with the bucketName then we don't know what we have and we move on
				final String[] splitCQ = cq.split(":");
				if (splitCQ[0].compareTo(bucketName) != 0){
					continue;
                }
				
				// Check to see if we have a FIRST or LAST and update the tracker to help narrow down ranges
				final String cqType = splitCQ[1];
				if (cqType.compareTo("FIRST") == 0 || cqType.compareTo("LAST") == 0){
	                tracker.updateStore(it.getKey().getRow().toString(), new Text(value.toString()), FirstLastTracker.StoreGoal.valueOf(cqType));
				}
	        }
		}  catch (Exception ex){
			log.error(ex);
			throw ex;
		} finally {
            if(maskScan != null){
                maskScan.close();
            }
		}

        // Convert the Range bits to arraylists
        for(int i = 0; i < hashCount; i++){
            final BitmapANDIterator.CompareBits compareBits = bits.get(i);
            final HashMap<String, AminoBitmap> rangeBitmap = rangeBitmaps.get(i);
            compareBits.setRangeBitmaps(new ArrayList<AminoBitmap>(rangeBitmap.values()));
        }

        return new BitMaskScanConfig(bits, tracker.getLatestFirst(), tracker.getEarliestLast());
    }


	///////////////////////////////////////////////////////////////////////////
    // Private methods below.
	///////////////////////////////////////////////////////////////////////////

	// public static final UserThreadPoolExecutor THREAD_POOL = new UserThreadPoolExecutor();
	
    private void checkState() {
        if (persistenceService == null) {
            throw new IllegalStateException("You must set a persistence service before calling methods on this service.");
        }
        if (metadataService == null) {
            throw new IllegalStateException("You must set a metadata service before calling methods on this service.");
        }
    }

	private void auditQuery(String bucketName, AminoAuditRequest auditRequest, Authorizations auths, 
		Collection<HypothesisFeature> hypothesisFeatures, Collection<String> bucketValues) throws IOException, TableNotFoundException {
		if (auditorService == null) {
			return;
		}
		Preconditions.checkNotNull(auditRequest);
		MorePreconditions.checkNotNullOrEmpty(auditRequest.getJustification(), "Justification can not be null or empty");
		MorePreconditions.checkNotNullOrEmpty(auditRequest.getDn(), "User DN can not be null or empty ");
		
		final StringBuilder criteria = new StringBuilder("bucketName=[" + bucketName + "],");
		
		// Keep track of which metadataId's we've already fetched
		final Set<String> fetchedIds = new HashSet<String>();
		
		if(bucketValues == null){
            for(HypothesisFeature hf : hypothesisFeatures){
				if(!fetchedIds.contains(hf.featureMetadataId)){
					final String featureName = metadataService.getFeature(hf.featureMetadataId, auths).name;
				    fetchedIds.add(hf.featureMetadataId);
				    criteria.append("featureName=[").append(featureName).append(":").append(hf.toAuditString()).append("],");
				}				
			}
		} else {
			auditRequest.setSelectors(bucketValues);
		}
		
		// Configure the auditRequest
		auditRequest.setCriteria(criteria.toString());
		auditRequest.setLogOnly(true);
		auditRequest.setSystemTo(auditSystemTo);
		auditRequest.setVisibility(auditVisibility);
		
		auditorService.doAudit(auditRequest, true);
	}

    /**
     * Creates the appropriate QueryEntry's by inspecting the amino_bitmap_byBucket table.
     * @return QueryResult with results for the Hypothesis
     */
    private ArrayList<QueryEntry> resultsViaByBucket(String datasourceid, String bucketName, Authorizations auths,
                                                     Set<HypothesisFeature> restrictions, Set<HypothesisFeature> featuresSansRestrictions,
                                                     QueryStatisticsMap resultStats, AtomicBoolean keepWorking, long maxResults,
                                                     AtomicBoolean hitCap) throws Exception {
        ArrayList<QueryEntry> results = new ArrayList<QueryEntry>();

        BatchScanner resultScan = null;
        try{
            final String resultScanRowId = datasourceid + ":" + bucketName;
            final Integer shardCount = metadataService.getShardCount();

            resultScan = persistenceService.createBatchScanner(byBucketTable, auths);

            // TODO this might be a bad conversion
            final HashSet<String> restrictionValues = new HashSet<String>(restrictions.size());
            for(HypothesisFeature hf : restrictions){
                // TODO - FIXME HACK AGGGHHH The GUI is sending the values in as ["a,b,c"] instead of ["a", "b", "c"]
                String hackValue = hf.value;
                hackValue = hackValue.replaceAll("\\[\"","");
                hackValue = hackValue.replaceAll("\"]","");
                for(String v : hackValue.split(",")){
                    restrictionValues.add(v.trim());
                }
            }
            boolean resultsToScan = configureByValueScanner(resultScan, bucketName, featuresSansRestrictions, resultScanRowId, shardCount, auths,
                    restrictionValues);

            if(resultsToScan){
                long resultsCount = 0;

                for(Map.Entry<Key, Value> entry : resultScan){
                    if(!keepWorking.get()){
                        log.warn("createQueryResult told to stop working");
                        return null;
                    }

                    resultStats.increment();

                    if (resultsCount == maxResults) {
                        hitCap.set(true);
                        break;
                    }
                    final QueryEntry qe = new QueryEntry(entry.getKey().getColumnFamily().toString());
                    results.add(qe);
                    resultsCount++;
                }
            }
        } catch (Exception ex) {
            log.error(ex);
            throw ex;
        } finally {
            if(resultScan != null){
                resultScan.close();
            }
            if(resultStats != null){
                resultStats.endTime();
            }
        }

        return results;
    }

    /**
     * Creates the appropriate QueryEntry's by inspecting the amino_reverse_bitmap_byBucket and
     * amino_reverse_feature_lookup tables.
     * @return QueryResult with results for the Hypothesis
     */
    private ArrayList<QueryEntry> resultsViaReverseByBucket(String datasource, String bucketName, Set<HypothesisFeature> features,
                                                            Authorizations auths, long maxResults, AtomicBoolean hitCap) throws Exception {
        ArrayList<QueryEntry> results = new ArrayList<QueryEntry>();

        final String DS_BN = datasource + "#" + bucketName + "#";
        final String revByBucketItr = "reverseByBucketIterator";
        final String revLookupItr = "reverseFeatureLookupIterator";

        // We need to tell the iterator which features should be OR'd when looking up values (RATIOS, etc) and which
        // ones simply needed to be AND'd (pretty much everything else)
        final Set<AbstractMap.SimpleImmutableEntry<String, String>> andIds = new HashSet<AbstractMap.SimpleImmutableEntry<String, String>>();
        final Set<String> orIds = new HashSet<String>();

        final BatchScanner revByBucketScanner = persistenceService.createBatchScanner(reverseByBucketTable, auths);

        final List<Range> ranges = new ArrayList<Range>(features.size()); // The Ranges to look for in the reverseByBucketTable
        final Set<Range> lookupRanges = new HashSet<Range>(); // The Ranges to use when looking up the reverseFeatureLookupTable

        // For each shard:salt pair, create the Ranges needed to fetch the feature values and configure the iterator options
        for(int salt = 0; salt < metadataService.getHashCount(); salt++){
            for(int shard = 0; shard < metadataService.getShardCount(); shard++){
                String rowid = shard + ":" + salt;

                for(HypothesisFeature feature : features){
                    if(FeatureFactType.intervalTypes.contains(feature.type)){
                        orIds.add(DS_BN + feature.featureMetadataId);
                        // TODO - HACK - Need to do this a more flexible way
                        if(FeatureFactType.dateIntervalTypes.contains(feature.type)){
                            ranges.add(new Range(new Key(rowid, DS_BN + feature.featureMetadataId, translator.fromDate(feature.timestampFrom).toString()),
                                    new Key(rowid,DS_BN + feature.featureMetadataId, translator.fromDate(feature.timestampTo).toString()).followingKey(PartialKey.ROW_COLFAM_COLQUAL)));
                        } else {
                            ranges.add(new Range(new Key(rowid, DS_BN + feature.featureMetadataId, translator.fromRatio(feature.min).toString()),
                                    new Key(rowid, DS_BN + feature.featureMetadataId, translator.fromRatio(feature.max).toString()).followingKey(PartialKey.ROW_COLFAM_COLQUAL)));
                        }
                    } else {
                        andIds.add(new AbstractMap.SimpleImmutableEntry<String, String>(DS_BN + feature.featureMetadataId, feature.value));
                        ranges.add(IteratorUtils.exactRow(rowid, DS_BN + feature.featureMetadataId, feature.value));
                    }
                }
            }
        }

        // Configure the options on the iterator for the BatchScanner on the reverseByBucketTable
        revByBucketScanner.setScanIterators(30, ReverseByBucketCombiner.class.getCanonicalName(), revByBucketItr);
        revByBucketScanner.setScanIteratorOption(revByBucketItr, ReverseByBucketCombiner.OPTION_NUM_RANGES, String.valueOf(features.size()));
        if(andIds.size() > 0){
            revByBucketScanner.setScanIteratorOption(revByBucketItr, ReverseByBucketCombiner.OPTION_AND_IDS, new Gson().toJson(andIds));
        }
        if(orIds.size() > 0){
            revByBucketScanner.setScanIteratorOption(revByBucketItr, ReverseByBucketCombiner.OPTION_OR_IDS, new Gson().toJson(orIds));
        }

        // Set up the ranges and get ready to scan the amino_reverse_bitmap_byBucket table
        revByBucketScanner.setRanges(ranges);

        try{
            // Scan the table.  If there were any hits, create a Range for each bit to look up in the amino_reverse_feature_lookup table
            for(Map.Entry<Key, Value> e : revByBucketScanner){
                if(ReverseByBucketCombiner.INVALID_KEY.compareTo(e.getKey()) != 0){
                    String shard = e.getKey().getRow().toString().split(":")[0];
                    String salt = e.getKey().getRow().toString().split(":")[1];
                    AminoBitmap b = BitmapUtils.fromValue(e.getValue());

                    for(Integer i : b){
                        Range r = IteratorUtils.exactRow(shard, i + "#" + DS_BN + salt);
                        lookupRanges.add(r);
                    }
                }
            }
        } finally {
            revByBucketScanner.close();
        }

        // Check to see if there is anything to lookup
        if(lookupRanges.size() ==0){
            return results;
        }

        // Create the Scanner and set the iterator to de-conflict hash collisions
        final BatchScanner lookupScanner = persistenceService.createBatchScanner(reverseFeatureLookupTable, auths);
        lookupScanner.setRanges(lookupRanges);
        lookupScanner.setScanIterators(30, ReverseFeatureCombiner.class.getCanonicalName(), revLookupItr);
        lookupScanner.setScanIteratorOption(revLookupItr, ReverseFeatureCombiner.OPTION_SALTS, metadataService.getHashCount().toString());
        lookupScanner.setScanIteratorOption(revLookupItr, ReverseFeatureCombiner.OPTION_MAX_RESULTS, String.valueOf(maxResults));

        // Fetch the results
        try {
            int resultCount = 0;
            for(Map.Entry<Key, Value> entry : lookupScanner){
                resultCount++;
                results.add(new QueryEntry(entry.getValue().toString()));
                if(resultCount > maxResults){
                    hitCap.set(true);
                    return results;
                }
            }
        } finally {
            lookupScanner.close();
        }

        return results;
    }


    private QueryResult createQueryResult(String owner, String id, long maxResults, String justification, String userId,
                                          Authorizations auths, AtomicBoolean keepWorking) throws Exception {
        long startTime = System.currentTimeMillis();

        // Parameter verification
        MorePreconditions.checkNotNullOrEmpty(justification,"You cannot query without a justification." );
        MorePreconditions.checkNotNullOrEmpty(owner);
        MorePreconditions.checkNotNullOrEmpty(id);

        QueryResult returnQR;
        final ArrayList<QueryEntry> results;

        // Fetch the Hypothesis to run
        final Hypothesis hypothesis = metadataService.getHypothesis(userId, owner, id, auths);
        Preconditions.checkNotNull(hypothesis, "No hypothesis with owner '%s' and id '%s' could be found", owner, id);
        Preconditions.checkArgument(0 != hypothesis.hypothesisFeatures.size(), "There were no features for the Hypothesis");

        // Separate the regular features and the RESTRICTION features
        final Set<HypothesisFeature> restrictions = new HashSet<HypothesisFeature>();
        final Set<HypothesisFeature> featuresSansRestrictions = Sets.newHashSet(hypothesis.hypothesisFeatures);
        for(Iterator<HypothesisFeature> itr = featuresSansRestrictions.iterator(); itr.hasNext();){
            HypothesisFeature feature = itr.next();
            if(feature.type.compareTo("RESTRICTION") == 0){
                restrictions.add(feature);
                itr.remove();
            }
        }

        // Audit the query
        final String bucketName = metadataService.getBucket(hypothesis.bucketid, auths).name;
        final AminoAuditRequest auditReq = new AminoAuditRequest();
        auditReq.setDn(userId);
        auditReq.setJustification(justification);
        auditQuery(bucketName, auditReq, auths, hypothesis.hypothesisFeatures, null);

        // Can't just just a regular boolean to pass into the functions. That'd be too easy.  Stupid Java.
        AtomicBoolean hitCap = new AtomicBoolean(false);
        QueryStatisticsMap resultStats = new QueryStatisticsMap();

        if(restrictions.size() > 0){
            // Use the old way
            results = resultsViaByBucket(hypothesis.datasourceid, bucketName, auths, restrictions, featuresSansRestrictions,
                    resultStats, keepWorking, maxResults, hitCap);
        } else {
            // Use the new way
            results = resultsViaReverseByBucket(hypothesis.datasourceid, bucketName, featuresSansRestrictions, auths, maxResults, hitCap);
        }

        final String errorString = (hitCap.get()) ? "Query exceeded " + maxResults + " results." : null;

        if (logQueryStats) {
            log.info ("createQueryResult() elapsed=[" +
                    Math.round((System.currentTimeMillis() - startTime)/1000) +
                    "], resultCount=[" + String.valueOf(results.size()) +
                    "], resultScanner=[" + resultStats +
                    "].");
        }

        // Update the Hypothesis to reflect the new timestamps
        hypothesis.executed = System.currentTimeMillis();
        returnQR =	persistQueryResults(userId, hypothesis, results, errorString);
        hypothesis.queries.add(returnQR.id);
        metadataService.updateHypothesis(hypothesis, userId, auths);
        return returnQR;
    }

    private static boolean isByBucketScanNecessaryForQuery(BitMaskScanConfig bitmaskScanInformation) {
		if (bitmaskScanInformation.first == null ||
			 bitmaskScanInformation.last == null ||  
			bitmaskScanInformation.first.compareTo(bitmaskScanInformation.last) == 1) 
		{
			return false;
		}
			
		// Check to see if any of the masks cardinality is 0.  If it is, there's no need to do scan
        for(BitmapANDIterator.CompareBits compareBits : bitmaskScanInformation.maskArray){
            if(compareBits.getNonRangeCardinality() == 0 && compareBits.getRangeBitmaps().size() == 0){
                return false;
            }
        }

        return true;
    }

    private QueryResult persistQueryResults(String ownerId, Hypothesis hypothesis, List<QueryEntry>results, String error) throws Exception {
        final Gson gson = new Gson();
        final Long reverseTimestamp = Long.MAX_VALUE - new Date().getTime();
		final String cf = reverseTimestamp.toString();

        ArrayList<Mutation> mutations = new ArrayList<Mutation>(5);

        mutations.add(persistenceService.createInsertMutation(ownerId, cf, "result_count", hypothesis.btVisibility, String.valueOf(results.size())));
        mutations.add(persistenceService.createInsertMutation(ownerId, cf, "result_set", hypothesis.btVisibility, gson.toJson(results)));
        mutations.add(persistenceService.createInsertMutation(ownerId, cf, "id", hypothesis.btVisibility, hypothesis.id));
        mutations.add(persistenceService.createInsertMutation(ownerId, cf, "name", hypothesis.btVisibility, hypothesis.name));
        mutations.add(persistenceService.createInsertMutation(ownerId, cf, "hypothesis_at_runtime", hypothesis.btVisibility, hypothesis.toJson()));

        persistenceService.insertRows(mutations, resultsTable);
        final QueryResult result = new QueryResult();
        result.id = cf;
        result.timestamp = Long.MAX_VALUE - reverseTimestamp;
        result.result_count = Long.valueOf(results.size());
        result.result_set = results;
        result.hypothesisid = hypothesis.id;
        result.hypothesisname = hypothesis.name;
        result.hypothesis_at_runtime = hypothesis;
        result.error = error;
        return result;
    }

    // TODO This should be cleaned up - Jeremy
    private static void  addResultComponent(QueryResult result, String cq, String value) {
        if (cq.compareTo("result_set") == 0) {
            result.result_set = new Gson().fromJson(value, new TypeToken<List<QueryEntry>>(){}.getType());
        } else if (cq.compareTo("result_count") == 0) {
            result.result_count = Long.parseLong(value);
        } else if (cq.compareTo("name") == 0) {
		    result.hypothesisname = value;
		} else if (cq.compareTo("id") == 0) {
			result.hypothesisid = value;
		} else if (cq.compareTo("hypothesis_at_runtime") == 0) {
			result.hypothesis_at_runtime = new Gson().fromJson(value, Hypothesis.class);
		}
    }
		
	/**
	 * Creates a configuration for scanning the amino_bitmap_bitLookup table
	 * @param feature The feature to look for
	 * @param auths Accumulo Authorizations
     * @param bucketName The name of the bucket the feature is related to (useful to find min/max)
	 * @return Map containing params 
	 */
    private AccumuloScanConfig createScanConfigForFeature(HypothesisFeature feature, Authorizations auths, String bucketName) throws IOException {
        final AccumuloScanConfig config = new AccumuloScanConfig();

        if(feature.type.compareTo("NOMINAL") == 0 || feature.type.compareTo("BOOLEAN") == 0 || feature.type.compareTo("inList") == 0){
            config.setStartRow(feature.featureMetadataId);
            config.setStartColumnFamily(feature.value);
            config.setStartColumnQualifier("0");
            config.setEndRow(feature.featureMetadataId);
            config.setEndColumnFamily(feature.value);
            config.setEndColumnQualifier(TableConstants.ROW_TERMINATOR);
        } else if (FeatureFactType.numericIntervalTypes.contains(feature.type)) {
            FeatureMetadata f = null; // Declare here so we only have to fetch once
            double fMin = feature.min;
            double fMax = feature.max;

            // If min or max are set to the special values, go fetch what the current min and max are
            if(feature.min == Double.MIN_VALUE) {
                f = metadataService.getFeature(feature.featureMetadataId, auths);
                fMin = f.min.get(bucketName);
            }
            if(feature.max == Double.MAX_VALUE){
                if(f == null){
                    f = metadataService.getFeature(feature.featureMetadataId, auths);
                }
                fMax = f.max.get(bucketName);
            }

            config.setStartRow(feature.featureMetadataId);
            config.setStartColumnFamily(translator.fromRatio(fMin).toString());
            config.setStartColumnQualifier("0");
            config.setEndRow(feature.featureMetadataId);
            config.setEndColumnFamily(translator.fromRatio(fMax).toString());
            config.setEndColumnQualifier(TableConstants.ROW_TERMINATOR);
        } else if (feature.type.compareTo("RESTRICTION") == 0){ // RESTRICTION features are valid, but are unscannable ATT
            // RESTRICTION features are valid, but are unscannable ATT
        } else if (feature.type.compareTo("ORDINAL") == 0){
            throw new UnsupportedOperationException("Ordinal features not currently supported for query");
        } else if (FeatureFactType.dateIntervalTypes.contains(feature.type)) {
            // FIXME: make sure we've implemented intervals for dates properly...
            long fMin = feature.timestampFrom;
            long fMax = feature.timestampTo;

            config.setStartRow(feature.featureMetadataId);
            config.setStartColumnFamily(translator.fromDate(fMin).toString());
            config.setStartColumnQualifier("0");
            config.setEndRow(feature.featureMetadataId);
            config.setEndColumnFamily(translator.fromDate(fMax).toString());
            config.setEndColumnQualifier("~~~~~~~~~~~");
        } else {
            throw new UnsupportedOperationException("An unknown feature type was submitted for query: " + feature.type);
        }

        return config;
    }

    private BatchScanner createBitmapOutputBitLookupScanner(AccumuloScanConfig config, Authorizations auths) throws IOException, TableNotFoundException {
        return persistenceService.createConfiguredBatchScanner(bitLookupTable, auths, config);
    }

	/**
	 * Configures a BatchScanner for the byBucket table based on the HypothesisFeatures you are looking for. If
	 * you pass in a list of restriction values it will optimize the search to only look at rows that would hit on
	 * the values that you are looking for.  Returns true if a scan is necessary, false if no values would be found
	 *
	 * @param scanner The BatchScanner to configure
	 * @param bucketName The bucket to look in
	 * @param features The HypothesisFeatures to scan against
	 * @param resultScanRowId String in the form of  hypothesis.datasourceid + ":" + bucketName
	 * @param shardCount The number of shards in the byBucket table
	 * @param auths Accumulo Authorizations
	 * @return  true if a scan is necessary, false if no values would be found
	 */
	private boolean configureByValueScanner(BatchScanner scanner, String bucketName,
		Set<HypothesisFeature> features, String resultScanRowId, Integer shardCount, Authorizations auths, Set<String> restrictions) throws Exception {
		Preconditions.checkNotNull(scanner, "BatchScanner can not be null");
		
		// Special case - we want to know if certain values are in the the byBucket table but we don't need to combine
		// any of the features
		if(features.size() == 0) {
			if(restrictions == null || restrictions.size() == 0){
				log.warn("No features or restrictions were passed in");
				return false;
			}
            // TODO TEST THIS
			scanner.setRanges(persistenceService.generateRanges(new AccumuloScanConfig().setRow(resultScanRowId).setShardcount(shardCount)));
			for(String restriction : restrictions){
				// We only grab the first hash because normally the iterator would collapse this to one row.
				scanner.fetchColumn(new Text(restriction), new Text("0"));
			}
			log.debug("Configured for just restrictions");
			return true;
		}
		
		final BitMaskScanConfig bitmaskScanInformation = getBitmaskScanInformationForQuery(features, bucketName, auths);
		
		if (!isByBucketScanNecessaryForQuery(bitmaskScanInformation)){
			log.debug("ByBucketScan is not necessary for Query");
			return false;
		}
		
		// Clear out the old iterator configs so that we can put new values in
		scanner.clearScanIterators();
		
		// Create the configuration for the scanner iterator
        final ScanIteratorConfig scanIteratorConfig = new ScanIteratorConfig();

        scanIteratorConfig.priority = 30;
        scanIteratorConfig.iteratorClass = BitmapANDIterator.class.getCanonicalName();
        scanIteratorConfig.name = "queryScanIterator";

        // Set the iterator options
        scanIteratorConfig.options = new HashSet<ScanIteratorOption>(1);

        final String optionString = new Gson().toJson(bitmaskScanInformation.maskArray);
        scanIteratorConfig.options.add(new ScanIteratorOption(BitmapANDIterator.OPTION_BITS, optionString));

        final AccumuloScanConfig config = new AccumuloScanConfig();
        config.setStartRow(resultScanRowId);
        config.setStartColumnFamily(bitmaskScanInformation.first);
        config.setStartColumnQualifier("0");
        config.setEndRow(resultScanRowId);
        config.setEndColumnFamily(bitmaskScanInformation.last);
        config.setEndColumnQualifier(TableConstants.ROW_TERMINATOR);
        config.setShardcount(shardCount);
        config.setCustomIterator(scanIteratorConfig);

		persistenceService.configureBatchScanner(scanner,config);
		
		// If there are restrictions, use them to optimize the scan
        for(String it : restrictions){
			scanner.fetchColumnFamily(new Text(it));
		}

		return true;
	}

	///////////////////////////////////////////////////////////////////////////
	// Inner Classes
	///////////////////////////////////////////////////////////////////////////

    private class BitMaskScanConfig {
        public final String first;
        public final String last;
        public final List<BitmapANDIterator.CompareBits> maskArray; // One CompareBit per hash

        public BitMaskScanConfig(List<BitmapANDIterator.CompareBits> maskArray, String first, String last){
            this.maskArray = maskArray;
            this.first = first;
            this.last = last;
        }
    }

    ///////////////////////////////////////////////////////////////////////////
	// Inner classes for doing timed calls.  Accumulo has the abilitiy to do timed calls, but Accumulo was lacking this
    // feature so we had to bolt on our own solution.
    ///////////////////////////////////////////////////////////////////////////
		
	private class CreateHypothesesCall extends FlaggableCallable<Collection<Hypothesis>> {
		final String datasourceid;
		final String bucketid;
		final Collection<String> bucketValues;
		final String[] visibility;
		final String userid;
		final String justification;
		final List<String> featureIds;
		
		public CreateHypothesesCall(String datasourceid, String bucketid, Collection<String> bucketValues, String[] visibility, String userid, String justification, List<String> featureIds){
			this.datasourceid = datasourceid;
			this.bucketid = bucketid;
			this.bucketValues = bucketValues;
			this.visibility = visibility;
			this.userid = userid;
			this.justification = justification;
			this.featureIds = featureIds;
			this.threadName = "CreateHypothesesCall";
		}

		@Override
		protected Collection<Hypothesis> flaggableCall() throws Exception {
			return createNonPersistedHypotheses(datasourceid, bucketid, bucketValues, visibility, userid, justification, keepWorking, featureIds);
		}
	}
	
	private class FindHypothesesByBucketValuesCall extends FlaggableCallable<List<Hypothesis>> {
		final BtaByValuesRequest bvRequest;
		
		public FindHypothesesByBucketValuesCall(BtaByValuesRequest req){
			// super();
			this.bvRequest = req;
			this.threadName = "FindHypothesesByBucketValuesCall";
		}
				
		@Override
		protected List<Hypothesis> flaggableCall() throws Exception {
			return findHypothesesByBucketValues(bvRequest, keepWorking);
		}
	}
	
	private class CreateQueryResultCall extends FlaggableCallable<QueryResult> {
		final String owner;
		final String hypothesisId;
		final int maxResults;
		final String justification;
		final String userId;
		final Authorizations auths;
		
		public CreateQueryResultCall(String owner, String hypothesisId, int maxResults, String justification, String userId, String[] visibility) {
			this.owner = owner;
			this.hypothesisId = hypothesisId;
			this.maxResults = maxResults;
			this.justification = justification;
			this.userId = userId;
			this.auths = new Authorizations(visibility);
			this.threadName = "CreateQueryResultCall";				
		}
		
		@Override
		protected QueryResult flaggableCall() throws Exception {
			return createQueryResult(owner, hypothesisId, maxResults, justification, userId, auths, keepWorking);
		}
	}

}

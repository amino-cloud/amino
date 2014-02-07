package com._42six.amino.query.services;

import com._42six.amino.common.BucketMetadata;
import com._42six.amino.common.DatasourceMetadata;
import com._42six.amino.common.FeatureMetadata;
import com._42six.amino.common.entity.Hypothesis;
import com._42six.amino.query.exception.BigTableException;

import java.io.IOException;
import java.util.List;

/**
 * CRUD operations for {@link com._42six.amino.common.entity entities}
 */
public interface AminoMetadataService {

    /**
     * Adds the suffix to all of the tables
     * @param suffix The suffix to append to the tables
     */
    public void addTableSuffix(String suffix);

    /**
     * Returns the DatasourceMetadata corresponding to the id
     * @param dataSourceId The ID of the DatasourceMetadata to fetch
     * @param visibility   A list of strings corresponding to allowed visibilities for the user.
     * @return The appropriate DatasourceMetadata
     * @throws IOException
     */
    public DatasourceMetadata getDataSource(String dataSourceId, String[] visibility) throws IOException;

	/**
	 * List all DataSources in the system.
	 *
	 * @param visibility A list of strings corresponding to allowed visibilities for the user.
	 * @return The DataSources
	 */
	public List<DatasourceMetadata> listDataSources(String[] visibility) throws IOException;

	/**
	 * For a given source, find all available FeatureMetadata
	 *
	 * @param dataSourceId The id of the DatasourceMetadata to search against.
	 * @param visibility   A list of strings corresponding to allowed visibilities for the user.
	 * @return The FeatureMetadatas
	 */
	public List<FeatureMetadata> listFeatures(String dataSourceId, String[] visibility) throws IOException;

	/**
	 * For a given source, find all available BucketMetadata
	 *
	 * @param dataSourceId The id of the DatasourceMetadata to search against.
	 * @param visibility   A list of strings corresponding to the allowed visibilities for the user.
	 * @return The BucketMetadatas
	 */
	public List<BucketMetadata> listBuckets(String dataSourceId, String[] visibility) throws IOException;

	/**
	 * Get a given FeatureMetadata based on its id.
	 *
	 * @param id         The id to search with.
	 * @param visibility A list of strings corresponding to the allowed visibilities for the user.
	 * @return The FeatureMetadata
	 */
	public FeatureMetadata getFeature(String id, String[] visibility) throws IOException;

	/**
	 * Get a given BucketMatadata based on its id.
	 *
	 * @param id         The id to search with.
	 * @param visibility A list of strings corresponding to the allowed visibilities for the user.
	 * @return The BucketMetadata
	 */
	public BucketMetadata getBucket(String id, String[] visibility) throws IOException;

	/**
	 * Fetches the Hypothesis
	 *
	 * @param userId       The ID of the user making the request
	 * @param owner        The owner field of the hypthesis to fetch
	 * @param hypothesisId The ID of the hypothesis to fetch
	 * @param visibility   The security visibilities for the database
	 */
	public Hypothesis getHypothesis(String userId, String owner, String hypothesisId, String[] visibility) throws IOException;

	/**
	 * Lists existing hypotheses
	 *
	 * @param userId        The ID of the person's hypotheses to search
	 * @param visibility    A list of strings corresponding to the allowed visibilities for the user.
	 * @return The Hypotheses that the userId can see
	 */
	public List<Hypothesis> listHypotheses(String userId, String[] visibility) throws IOException;

	/**
	 * Persists a new hypothesis object to the database, setting its id property to an id chosen by the service.
	 *
	 * @param hypothesis The hypothesis to persist.
	 * @param userId     The user ID of the owner of the Hypothesis
	 * @param visibility A list of strings corresponding to the allowed visibilities for the user.
	 * @return The Hypothesis that was created
	 */
	public Hypothesis createHypothesis(Hypothesis hypothesis, String userId, String[] visibility) throws Exception;

	/**
	 * Updates a given hypothesis object.
	 *
	 * @param hypothesis The hypothesis to update
	 * @param userId     The user ID of the owner of the Hypothesis
	 * @param visibility A list of strings corresponding to the allowed visibilities for the user.
	 * @return The Hypothesis that was updated
	 */
	public Hypothesis updateHypothesis(Hypothesis hypothesis, String userId, String[] visibility) throws Exception;

	/**
	 * Deletes a hypothesis from the database
	 *
	 * @param owner      The owner string of the Hypothesis to delete
	 * @param id         The id of the hypothesis to delete
	 * @param visibility A list of strings corresponding to the allowed visibilities for the user.
	 */
	public void deleteHypothesis(String owner, String id, String[] visibility);

    /**
     * Returns the number of Amino shards that are being used for the bitmaps
     * @return the number of Amino shards that are being used for the bitmaps
     * @throws BigTableException
     */
    public Integer getShardCount() throws BigTableException;

    /**
     * Returns the number of salts that are being used
     * @return the number of salts that are being used
     * @throws BigTableException
     */
    public Integer getHashCount() throws BigTableException;
}

/*
 * This generates the Thrift service for the Amino MetadataService.  You must have Thrift 0.6.1 installed.
 *
 * Run the generate.sh script to generate all of the Thrift classes and place them in the appropriate spot in the src tree
 */

namespace java com._42six.amino.query.thrift.services

include "Common.thrift"

service ThriftMetadataService {
    list<Common.TDatasourceMetadata> listDataSources(1: set<string> visibilities),

    list<Common.TFeatureMetadata> listFeatures(1: string datasourceId, 2: set<string> visibilities),

    list<Common.TBucketMetadata> listBuckets(1: string datasourceId, 2: set<string> visibilities),

    Common.TDatasourceMetadata getDataSource(1: string dataSourceId, 2: set<string> visibilities),

    Common.TFeatureMetadata getFeature(1: string id, 2: set<string> visibilities),

    Common.TBucketMetadata getBucket(1: string id, 2: set<string> visibilities),

    /**
     * Fetches the Hypothesis
     *
     * @param userId       The ID of the user making the request
     * @param owner        The owner field of the hypthesis to fetch
     * @param hypothesisId The ID of the hypothesis to fetch
     * @param visibility   The security visibilities for the database
     */
    Common.THypothesis getHypothesis(1: string userId, 2: string owner, 3: string hypothesisId, 4: set<string> visibilities),

    list<Common.THypothesis> listHypotheses(1: string userId, 2: set<string> visibilities),

    Common.THypothesis createHypothesis(1: Common.THypothesis hypothesis, 2: string userId, 3: set<string> visibilities),

    Common.THypothesis updateHypothesis(1: Common.THypothesis hypothesis, 2: string requester, 3: set<string> visibilities),

    void deleteHypothesis(1: string owner, 2: string id, 3: set<string> visibilities),

    i32 getShardCount(),

    i32 getHashCount(),
}
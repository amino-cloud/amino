namespace java com._42six.amino.common.thrift

enum TGroupRole {
	ADMIN,          // Can Add & Delete users; Can delete group
    CONTRIBUTOR,    // Can share their Inquiries for the group to view
	VIEWER          // Can view Inquiries shared by the group.
}

struct TGroupMember {
    1: string name;
    2: set<TGroupRole> roles;
}

struct TGroup {
    1: string groupName;
    2: string createdBy;
    3: i64 dateCreated;
    4: set<TGroupMember> members;
}

struct TCreateGroupRequest {
    1: TGroup group,
    2: string requester,
    3: set<string> visibilities
}

struct TAddUsersRequest {
    1: string groupName,
    2: set<TGroupMember> usersToAdd,
    3: string requester,
    4: set<string> visibilities
}

struct THypothesisFeature {
	1: string id;
	2: string featureMetadataId;
	3: string type;
	4: string operator;
	5: string value;
	6: double min;
	7: double max;
	8: string dateTimeType;
	9: string relativeDateTimeRange;
	10: i64 timestampFrom;
	11: i64 timestampTo;

	/**
	 * The visibility of the Hypothesis
	 */
	12: string visibility;

	/**
	 * The number of bucket matches for this feature
	 */
	13: i32 count;

	/**
	 * The measure of uniqueness for this feature in the bucket
	 */
	14: double uniqueness;
	15: bool toInclude;
}

struct THypothesis {
	/**
	 * The person or group that has ownership of this hypothesis
	 */
	1: string owner;

	/**
	 * The UUID representing the hypothesis
	 */
	2: string id;

	/**
	 * The human readable name of the hypothesis
	 */
	3: string name;

	/**
	 * Which bucket the Hypothesis relates to
	 */
	4: string bucketid;

	/**
	 * A list of groups that can edit this hypothesis
	 */
	5: list<string> canEdit;

	/**
	 * A list of groups that can view this hypothesis
	 */
	6: list<string> canView;

	/**
	 * The datasource that the data comes from
	 */
	7: string datasourceid;

	/**
	 * The justification as to why this Hypothesis was created
	 */
	8: string justification;

	/**
	 * The value that was used to generate the Hypothesis
	 */
	9: string bucketValue;

	/**
	 * The Big Table ColumnVisibility string
	 */
	10: string btVisibility;

	/**
	 * The human readable visibility
	 */
	11: string visibility;

	/**
	 * All of the features that relate to this Hypothesis
	 */
	12: set<THypothesisFeature> hypothesisFeatures;

	/**
	 * The time that the Hypothesis was created
	 */
	13: i64 created;

	/**
	 * The time that the Hypothesis was updated
	 */
	14: i64 updated;

	/**
	 * The time that the Hypothesis was executed
	 */
	15: i64 executed;

	/**
	 * A sorted set of QueryResult ID's, sorted by time
	 */
	16: set<string> queries;
}
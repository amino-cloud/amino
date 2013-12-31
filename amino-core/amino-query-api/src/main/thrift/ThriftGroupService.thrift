/*
 * This generates the Thrift service for the Amino GroupService.  You must have Thrift 0.6.1 installed.
 *
 * Run the generate.sh script to generate all of the Thrift classes and place them in the appropriate spot in the src tree
 */

namespace java com._42six.amino.query.thrift.services

include "Common.thrift"

service ThriftGroupService {
    bool verifyGroupExists(1: string group, 2: set<string> visibilities),

    bool verifyUserExists(1: string user, 2: set<string> visibilities),

    void addToGroup(1: Common.TAddUsersRequest request),

    void createGroup(1: Common.TCreateGroupRequest request),

    set<string> listGroups(1: string userId, 2: set<string> visibilities),

    set<string> getGroupsForUser(1: string userId, 2: set<string> visibilities),

    void removeUserFromGroups(1: string requester, 2: string userId, 3: set<string> groups, 4: set<string> visibilities),

    void removeUsersFromGroup(1: string requester, 2: string group, 3: set<string> users, 4: set<string> visibilities),

    /* userOwned is optional.  Use false if you're unsure */
    list<Common.THypothesis> getGroupHypothesesForUser(1: string userId, 2: set<string> visibilities, 3: bool userOwned),

    Common.TGroup getGroup(1: string requester, 2: string group, 3: set<string> visibilities),
}


package com._42six.amino.query.services;

import com._42six.amino.common.Group;
import com._42six.amino.common.entity.Hypothesis;
import com._42six.amino.common.query.requests.AddUsersRequest;
import com._42six.amino.common.query.requests.CreateGroupRequest;

import java.io.IOException;
import java.util.List;
import java.util.Set;


public interface AminoGroupService {

    String setGroupMetadataTable(String table);

    String getGroupHypothesisLUT();

    String setGroupHypothesisLUT(String lut);

    String setGroupMembershipTable(String memberTable);

    String setHypothesisTable(String hypothesisTable);

    /**
     * Verify that the group exists in the group_metadata table
     * @param group The group name to search for
     * @param visibilities The database visibilities
     * @return true if the group exists, false otherwise
     * @throws java.io.IOException if the group_metadata table does not exist
     */
    boolean verifyGroupExists(String group, Set<String> visibilities) throws IOException;

    /**
     * Verify that the user belongs to any groups
     * @param user The user name to search for
     * @param visibilities The database visibilities
     * @return true if the user belongs to any groups, false otherwise
     * @throws java.io.IOException if the group_membership table does not exist
     */
    boolean verifyUserExists(String user, Set<String> visibilities) throws IOException;

    /**
     * Adds the members to the group.
     *
     * @param request The request object representing all of the values needed for adding users
     */
    void addToGroup(AddUsersRequest request) throws Exception;

    /**
     * Creates a group in the database.  If the group already exists, an exception will be thrown
     *
     * @param request The incoming request containing the Group to add to the database
     */
    void createGroup(CreateGroupRequest request) throws Exception;

    /**
     * List all of the groups that a particular user can see
     *
     * @param userId     The person for which groups can be seen
     * @param visibilities the db visibility strings
     */
    Set<String> listGroups(String userId, Set<String> visibilities) throws IOException;

    /**
     * Remove the members from the group
     *
     * @param requester The person requesting the users to be removed.  Must be an admin for the group
     * @param group   The group to remove from
     * @param members The members to remove from the group
     * @param visibilities   The database authorizations
     */
    public void removeUsersFromGroup(String requester, String group, Set<String> members, Set<String> visibilities) throws Exception;

    /**
     * Removes the user from a group.  If no group is provided, they are removed from all of the groups
     *
     * @param requester  The ID of the person requesting that the user be removed
     * @param userId     The ID of the user to remove
     * @param groups     The group to remove from
     * @param visibilities The db visibility strings
     */
    void removeUserFromGroups(String requester, String userId, Set<String> groups, Set<String> visibilities) throws Exception;

    /**
     * Returns the Group object that corresponds with the underlying group in the tables.  The requester must be a member
     * in order to get the Group.
     * @param requester   The person trying to fetch the Group
     * @param group       The Group to lookup in the tables
     * @param visibilities  The BigTable visibility strings
     * @return Group representing all of of the members of the group
     * @throws java.io.IOException
     */
    Group getGroup(String requester, String group, Set<String> visibilities) throws IOException;

    /**
     * Fetches the groups that a particular id belongs to
     *
     * @param userId     The userId to fetch the groups for or empty to fetch them all
     * @param visibilities The database visibilities
     * @return Set<String> of groups for the particular userId
     */
    Set<String> getGroupsForUser(String userId, Set<String> visibilities) throws IOException;

    /**
     * Returns the Hypotheses for the groups that the userId belongs to
     *
     * @param userId     The userId to fetch the group Hypotheses for
     * @param visibilities database visibilities
     * @param userOwned  Set to true to return the Hypotheses owned by the user in addition to the group Hypotheses
     * @return All the Hypothesis for each of the given groups
     */
    List<Hypothesis> getGroupHypothesesForUser(String userId, Set<String> visibilities, boolean userOwned) throws IOException;

    /**
     * Returns the Hypotheses for the groups that the userId belongs to
     *
     * @param userId     The userId to fetch the group Hypotheses for
     * @param visibilities database visibilities
     * @return All the Hypothesis for each of the given groups
     */
    List<Hypothesis> getGroupHypothesesForUser(String userId, Set<String> visibilities) throws IOException;
}

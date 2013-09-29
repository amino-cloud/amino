package com._42six.amino.query.services;

import com._42six.amino.common.entity.Hypothesis;
import com._42six.amino.common.query.requests.AddUsersRequest;
import com._42six.amino.common.query.requests.CreateGroupRequest;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * Interface for handling group management and querying.
 */
public interface AminoGroupService{
	/**
	 * Adds the members to the group.
	 *
	 * @param request The group to add to plus the users and their roles in the group
	 */
	public void addToGroup (AddUsersRequest request) throws Exception;

	/**
	 * Creates a group in the database.  If the group already exists, an exception will be thrown
	 *
	 * @param request Information about the group to create
	 */
	public void createGroup(CreateGroupRequest request) throws Exception;

	/**
	 * Remove the members from the group
	 *
	 * @param group   The group to remove from
	 * @param members The members to remove from the group
	 */
	public void removeUsersFromGroup(final String group, List<String> members) throws Exception;

	/**
	 * Removes the user from a group.  If no group is provided, they are removed from all of the groups
	 *
     * @param requester  The ID or the person requesting that the user be removed
	 * @param userId     The ID of the user to remove
	 * @param groups     The group to remove from
	 * @param visibility The db visibility strings
	 */
	public void removeUserFromGroups(String requester, String userId, Set<String> groups, String[] visibility) throws Exception;

	/**
	 * Fetches the groups that a particular id belongs to
	 *
	 * @param userId     The userId to fetch the groups for or empty to fetch them all
	 * @param visibility The Accumulo authorizations
	 * @return Set<String> of groups for the particular userId
	 */
	public Set<String> getGroups(String userId, String[] visibility) throws IOException;

	/**
	 * Returns the Hypotheses for the groups that the userId belongs to
	 *
	 * @param userId     The userId to fetch the group Hypotheses for
	 * @param visibility Accumulo authorizations
	 * @param userOwned  Set to true to return the Hypotheses owned by the user in addition to the group Hypotheses
	 * @return All the Hypothesis for each of the given groups
	 */
	public List<Hypothesis> getGroupHypothesesForUser(String userId, String[] visibility, boolean userOwned) throws IOException;

	/**
	 * Returns the Hypotheses for the groups that the userId belongs to
	 *
	 * @param userId     The userId to fetch the group Hypotheses for
	 * @param visibility Accumulo authorizations
	 * @return All the Hypothesis for each of the given groups
	 */
	public List<Hypothesis> getGroupHypothesesForUser(String userId, String[] visibility) throws IOException;
}

package com._42six.amino.query.services.accumulo;

import com._42six.amino.common.Group;
import com._42six.amino.common.GroupMember;
import com._42six.amino.common.MorePreconditions;
import com._42six.amino.common.bigtable.TableConstants;
import com._42six.amino.common.entity.Hypothesis;
import com._42six.amino.common.query.requests.AddUsersRequest;
import com._42six.amino.common.query.requests.CreateGroupRequest;
import com._42six.amino.query.services.AminoGroupService;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.*;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

/**
 * Service for handling everything having to do with groups
 */
public class AccumuloGroupService implements AminoGroupService {

	public static final Logger log = Logger.getLogger(AccumuloGroupService.class);

    /** The groups to Hypothesis Look Up Table */
	private String groupHypothesisLUT;

    /** The table containing whom belongs to which groups */
	private String groupMembershipTable;

    /** The table that contains the metadata about the groups */
    private String groupMetadataTable;

    /** The table all of the Hypotheses are stored in */
	private String hypothesisTable;

    /** Service for persisting (and fetching) things from Accumulo */
	private AccumuloPersistenceService persistenceService;

	public AccumuloGroupService() {
		// EMPTY
	}

	public AccumuloGroupService(AccumuloPersistenceService service) {
		this.persistenceService = service;
	}

	public AccumuloPersistenceService setPersistenceService(AccumuloPersistenceService service) {
		return this.persistenceService = service;
	}

	public String setGroupMetadataTable(String table) {
		return this.groupMetadataTable = table;
	}

	public String getGroupHypothesisLUT() {
		return this.groupHypothesisLUT;
	}

	public String setGroupHypothesisLUT(String lut) {
		return this.groupHypothesisLUT = lut;
	}

	public String setGroupMembershipTable(String memberTable) {
		return this.groupMembershipTable = memberTable;
	}

	public String setHypothesisTable(String hypothesisTable) {
		return this.hypothesisTable = hypothesisTable;
	}

    /**
     * Verify that the group exists in the group_metadata table
     * @param group The group name to search for
     * @param vis The Accumulo visibilities
     * @return true if the group exists, false otherwise
     * @throws IOException if the group_metadata table does not exist
     */
    public boolean verifyGroupExists(String group, String[] vis) throws IOException {
        MorePreconditions.checkNotNullOrEmpty(group);
        final Authorizations auths = new Authorizations(Preconditions.checkNotNull(vis));
        final String groupName = (group.startsWith(TableConstants.GROUP_PREFIX)) ? group : TableConstants.GROUP_PREFIX + group;
        final Scanner groupMetaScanner;
        try {
            groupMetaScanner = persistenceService.createScanner(groupMetadataTable, auths);
        } catch (TableNotFoundException e) {
            throw new IOException(e);
        }
        groupMetaScanner.setRange(new Range(new Text(groupName)));
        return groupMetaScanner.iterator().hasNext();
    }

    /**
     * Verify that the user belongs to any groups
     * @param user The user name to search for
     * @param vis The Accumulo visibilities
     * @return true if the user belongs to any groups, false otherwise
     * @throws IOException if the group_membership table does not exist
     */
    public boolean verifyUserExists(String user, String[] vis) throws IOException {
        MorePreconditions.checkNotNullOrEmpty(user);
        final Authorizations auths = new Authorizations(Preconditions.checkNotNull(vis));
        final String userName = (user.startsWith(TableConstants.USER_PREFIX)) ? user : TableConstants.USER_PREFIX + user;
        final Scanner groupMembershipScanner;
        try {
            groupMembershipScanner = persistenceService.createScanner(groupMembershipTable, auths);
        } catch (TableNotFoundException e) {
            throw new IOException(e);
        }
        groupMembershipScanner.setRange(new Range(new Text(userName)));
        return groupMembershipScanner.iterator().hasNext();
    }

	/**
	 * Adds the members to the group.
     *
     * @param request The request object representing all of the values needed for adding users
	 */
	public void addToGroup(AddUsersRequest request) throws Exception {
		Preconditions.checkNotNull(request, "Request object was null");
        final String[] tokens = Preconditions.checkNotNull(request.getSecurityTokens(), "Security tokens were null");
        final Authorizations auths = Preconditions.checkNotNull(new Authorizations(tokens), "Could not create Authorizations");
        final Group group = Preconditions.checkNotNull(request.getGroup(), "Group was missing");
        final Set<GroupMember> members = Preconditions.checkNotNull(group.getMembers(), "Missing members");
        String groupName = MorePreconditions.checkNotNullOrEmpty(group.getGroupName(), "Missing group name");
        String requestor = MorePreconditions.checkNotNullOrEmpty(request.getRequestor(), "Missing requestor");

        // Make sure prefixed properly
        groupName = groupName.startsWith(TableConstants.GROUP_PREFIX) ? groupName : TableConstants.GROUP_PREFIX + groupName;
        requestor = requestor.startsWith(TableConstants.USER_PREFIX) ? requestor : TableConstants.USER_PREFIX + requestor;

		final ArrayList<Mutation> memberEntries = new ArrayList<Mutation>(members.size());
        final ArrayList<Mutation> metaEntries = new ArrayList<Mutation>();

        // Make sure that the requestor is an admin for the group and can perform these admin tasks
        final Scanner scanner = persistenceService.createScanner(groupMetadataTable, auths);
        scanner.setRange(new Range(groupName));
        scanner.fetchColumn(new Text("admin"), new Text(requestor));

        if(!scanner.iterator().hasNext()){
            throw new Exception("User " + requestor + " does not have admin rights to the group " + groupName);
        }

        // Create the mutations for the group_metadata and group_membership tables and insert them
        for(GroupMember member : members){
            String memberName = MorePreconditions.checkNotNullOrEmpty(member.getName(), "Group member missing name");
            if(!memberName.startsWith(TableConstants.USER_PREFIX)) { memberName = TableConstants.USER_PREFIX + memberName; }
            final Set<Group.GroupRole> roles = Preconditions.checkNotNull(member.getRoles(), "Member " + memberName + " missing roles");

            // group_membership
            memberEntries.add(persistenceService.createInsertMutation(memberName, groupName, "", "", ""));

            // group_metadata
            for(Group.GroupRole role : roles){
                metaEntries.add(persistenceService.createInsertMutation(groupName, role.toString(), memberName, "", ""));
            }
        }
        persistenceService.insertRows(memberEntries, groupMembershipTable);
		persistenceService.insertRows(metaEntries, groupMetadataTable);
	}

	/**
	 * Creates a group in the database.  If the group already exists, an exception will be thrown
	 *
	 * @param request The incoming request containing the Group to add to the database
	 */
	public void createGroup(CreateGroupRequest request) throws Exception {
        Preconditions.checkNotNull(request);
        final Group group = Preconditions.checkNotNull(request.getGroup());
        String groupName = MorePreconditions.checkNotNullOrEmpty(group.getGroupName(), "Group Name not set");
        if(!groupName.startsWith(TableConstants.GROUP_PREFIX)){ groupName = TableConstants.GROUP_PREFIX + groupName; }
        final String[] tokens = Preconditions.checkNotNull(request.getSecurityTokens(), "Security tokens were null");
        final Authorizations auths = Preconditions.checkNotNull(new Authorizations(tokens), "Could not create Authorizations");
        final Collection<GroupMember> members = MorePreconditions.checkNotNullOrEmpty(group.getMembers(), "Must provide at least one group member");
        String createdBy = MorePreconditions.checkNotNullOrEmpty(group.getCreatedBy(), "Created by was empty");
        if(!createdBy.startsWith(TableConstants.USER_PREFIX)){ createdBy = TableConstants.USER_PREFIX + createdBy; }

        final long createdDate = System.currentTimeMillis() / 1000L;

        boolean adminProvided = false;
        final ArrayList<Mutation> metadataRows = new ArrayList<Mutation>();
        final ArrayList<Mutation> membershipRows = new ArrayList<Mutation>();

        // Check to see if the group already exists
        final Scanner groupScanner = persistenceService.createScanner(groupMetadataTable, auths);
        groupScanner.setRange(new Range(groupName));
        if (groupScanner.iterator().hasNext()) {
            throw new IllegalStateException("Group '" + groupName + "' already exists");
        }

        // Create membership entries
        for(GroupMember gm : members){
            for(Group.GroupRole role : gm.getRoles()){
                final String memberName = TableConstants.USER_PREFIX + MorePreconditions.checkNotNullOrEmpty(gm.getName(), "Group member name was empty");
                metadataRows.add(persistenceService.createInsertMutation(groupName, MorePreconditions.checkNotNullOrEmpty(role.toString()),
                        memberName, "", ""));

                membershipRows.add(persistenceService.createInsertMutation(memberName, groupName, "", "", ""));

                // Note that an admin was provided for the group
                if (role.equals(Group.GroupRole.ADMIN)){
                    adminProvided = true;
                }
            }
        }

        // Add the entries if at least one admin was provided
        Preconditions.checkArgument(adminProvided, "There must be at least one Admin user per group");
        persistenceService.insertRow(groupName, "created_by", createdBy, "", "", groupMetadataTable);
        persistenceService.insertRow(groupName, "created_date", String.valueOf(createdDate), "", "", groupMetadataTable);
        persistenceService.insertRows(metadataRows, groupMetadataTable);
        persistenceService.insertRows(membershipRows, groupMembershipTable);
        log.info("Created group " + groupName);
    }

//    /**
//     * Remove the members from the group
//     *
//     * @param group   The group to remove from
//     * @param members The members to remove from the group
//     */
//    public void removeUsersFromGroup(final String group, List<String> members) throws Exception {
//        MorePreconditions.checkNotNullOrEmpty(group);
//        MorePreconditions.checkNotNullOrEmpty(members);
//        final Collection<Mutation> entries = new ArrayList<Mutation>();
//
//        for(String it : members){
//            entries.add(persistenceService.createDeleteMutation(it, group, "", ""));
//        }
//
//        persistenceService.insertRows(entries, groupMembershipTable);
//    }

    /**
     * Removes the user from a group.  If no group is provided, they are removed from all of the groups
     *
     * @param requester  The ID of the person requesting that the user be removed
     * @param userId     The ID of the user to remove
     * @param groups     The group to remove from
     * @param visibility The db visibility strings
     */
    public void removeUserFromGroups(String requester, String userId, Set<String> groups, String[] visibility) throws Exception {
        Preconditions.checkNotNull(visibility);
        removeUserFromGroups(requester, userId, groups, new Authorizations(visibility));
    }

    /**
     * Removes the user from a group.  If no group is provided, they are removed from all of the groups
     *
     * @param requester The ID of the person requesting that the user be removed
     * @param userId    The ID of the user to remove
     * @param groups    The group to remove from
     * @param auths     The authorization strings
     */
    public void removeUserFromGroups(String requester, final String userId, Set<String> groups, Authorizations auths) throws Exception {
        MorePreconditions.checkNotNullOrEmpty(userId);
        MorePreconditions.checkNotNullOrEmpty(requester);
        Preconditions.checkNotNull(auths);

        final Collection<Mutation> groupMembershipMutations = new ArrayList<Mutation>();
        final Collection<Mutation> groupMetadataMutations = new ArrayList<Mutation>();
        final Collection<Range> metaGroupsToPurgeFrom = new HashSet<Range>();

        // If no groups provided, remove from any group they might be a part of
        if(groups == null || groups.size() == 0){
            groups = getGroups(userId, auths);
        }

        for(String group : groups){
            if(checkCanRemove(requester, userId, group, auths)){
                metaGroupsToPurgeFrom.add(new Range(group));
                groupMembershipMutations.add(persistenceService.createDeleteMutation(userId, group, "", ""));
            } else {
                log.warn(String.format("'%s' does not have permission to remove '%s' from group '%s'", requester, userId, group));
                throw new Exception(String.format("'%s' does not have permission to remove '%s' from group '%s'", requester, userId, group));
            }
        }

        // Find all of the places in the group_metadata table where the user appears and we have permission to remove them
        final BatchScanner metaGroupScanner = persistenceService.createBatchScanner(groupMetadataTable, auths);
        metaGroupScanner.setRanges(metaGroupsToPurgeFrom);
        for(Map.Entry<Key, Value> entry : metaGroupScanner){
            Text user = entry.getKey().getColumnQualifier();
            if(user.toString().compareTo(userId) == 0){
                groupMetadataMutations.add(persistenceService.createDeleteMutation(entry.getKey().getRow().toString(),
                        entry.getKey().getColumnFamily().toString(), user.toString(), entry.getKey().getColumnVisibility().toString()));
            }
        }

        // Do the deletions
        persistenceService.insertRows(groupMembershipMutations, groupMembershipTable);
        persistenceService.insertRows(groupMetadataMutations, groupMetadataTable);
    }

    /**
     * Checks to see if the the requester has permission to remove the user from the given table
     *
     * @param requester The ID asking that the userId be removed
     * @param userId    The ID to be removed
     * @param group     The group to be removed from
     * @param auths     The BigTable authorizations
     * @return          true if there are sufficient permissions to remove the userId, false otherwise
     * @throws TableNotFoundException
     */
    private boolean checkCanRemove(String requester, String userId, String group, Authorizations auths) throws TableNotFoundException {
        // See if the requester is an admin for the group and has permissions to remove the user
        final Scanner groupMetaScanner = persistenceService.createScanner(groupMetadataTable, auths);
        groupMetaScanner.setRange(new Range(group));
        groupMetaScanner.fetchColumnFamily(new Text("admin"));

        boolean requesterIsAdmin = false;
        int adminsFound = 0;
        for(Map.Entry<Key, Value> entry : groupMetaScanner){
            adminsFound++;
            if(entry.getKey().getColumnQualifier().toString().compareTo(requester) == 0){
                requesterIsAdmin = true;
            }
        }

        if(requesterIsAdmin){
            // If the requester is the only admin, can't remove self.
            return (adminsFound > 1) || (requester.compareTo(userId) != 0);
        } else {
            // If not an admin, can only remove self
            return requester.compareTo(userId) == 0;
        }
    }

    /**
     * Returns the Group object that corresponds with the underlying group in the tables.  The requestor must be a member
     * in order to get the Group.
     * @param requestor   The person trying to fetch the Group
     * @param group       The Group to lookup in the tables
     * @param visibility  The BigTable visibility strings
     * @return Group representing all of of the members of the group
     * @throws IOException
     */
    public Group getGroup(String requestor, String group, String[] visibility) throws IOException {
        return getGroup(requestor, group, new Authorizations( Preconditions.checkNotNull(visibility)));
    }

    /**
     * Returns the Group object that corresponds with the underlying group in the tables.  The requestor must be a member
     * in order to get the Group.  If the group doesn't exist, null is returned
     * @param requestor   The person trying to fetch the Group
     * @param group       The Group to lookup in the tables
     * @param auths       The BigTable authorizations
     * @return Group representing all of of the members of the group
     * @throws IOException
     */
    public Group getGroup(String requestor, String group, Authorizations auths) throws IOException {
        MorePreconditions.checkNotNullOrEmpty(requestor);
        MorePreconditions.checkNotNullOrEmpty(group);
        Preconditions.checkNotNull(auths);

        final Group returnGroup = new Group(group);
        boolean requestorPartOfGroup = false;

        // Make sure prefixes are present
        if(!requestor.startsWith(TableConstants.USER_PREFIX)){ requestor = TableConstants.USER_PREFIX + requestor; }
        if(!group.startsWith(TableConstants.GROUP_PREFIX)){ group = TableConstants.GROUP_PREFIX + group; }

        try{
            final Scanner metadataScanner = persistenceService.createScanner(groupMetadataTable, auths);
            metadataScanner.setRange(new Range(group));

            if(!metadataScanner.iterator().hasNext()){
                throw new IOException("Group was not found");
            }

            final Set<GroupMember> members = returnGroup.getMembers();

            Text roleText = new Text();
            Text memberText = new Text();

            // Serialize the group members
            for(Map.Entry<Key, Value> entry : metadataScanner){
                entry.getKey().getColumnFamily(roleText);
                entry.getKey().getColumnQualifier(memberText);
                String role = roleText.toString();
                String member = memberText.toString();

                if(member.equals(requestor)){
                    requestorPartOfGroup = true;
                }

                if(role.equals("created_by")){
                    returnGroup.setCreatedBy(member);
                } else if(role.equals("created_date")){
                    returnGroup.setDateCreated(Long.parseLong(member));
                } else {
                    // See if the member already exists, and if so, add the role, otherwise create a new member
                    // TODO - Could make this faster if need be
                    boolean found = false;
                    for(GroupMember m : members){
                        if(m.getName().equals(member)){
                            Set<Group.GroupRole> roles = m.getRoles();
                            roles.add(Group.GroupRole.fromString(role));
                            found = true;
                            break;
                        }
                    }
                    if(!found){
                        members.add(new GroupMember(member, Sets.newHashSet(Group.GroupRole.fromString(role))));
                    }
                }
            }
        } catch (TableNotFoundException ex) {
            throw new IOException(ex);
        }

        if(!requestorPartOfGroup){
            throw new IOException(String.format("User <%s> is not part of group <%s> and does not have permission to list members", requestor, group));
        }

        return returnGroup;
    }

	/**
	 * Fetches the groups that a particular id belongs to
	 *
	 * @param userId     The userId to fetch the groups for or empty to fetch them all
	 * @param visibility The Accumulo authorizations
	 * @return Set<String> of groups for the particular userId
	 */
	public Set<String> getGroups(String userId, String[] visibility) throws IOException {
		Preconditions.checkNotNull(visibility);
		return getGroups(userId, new Authorizations(visibility));
	}

	/**
	 * Fetches the groups that a particular id belongs to
	 *
	 * @param userId The userId to fetch the groups for or empty to fetch them all
	 * @param auths  The Accumulo authorizations
	 * @return Set<String> of groups for the particular userId
	 */
	public Set<String> getGroups(String userId, Authorizations auths) throws IOException {
		Preconditions.checkNotNull(auths);

		// Everybody is part of the public group
		Set<String> groups = new HashSet<String>();
        groups.add(TableConstants.PUBLIC_GROUP);

        Scanner scan;
        try{
            scan = persistenceService.createScanner(groupMetadataTable, auths);
        }  catch (TableNotFoundException ex){
            log.error("Table '" + groupHypothesisLUT + "' was not found");
            throw new IOException(ex);
        }
		scan.setRange(new Range(userId));

        // TODO FIX THIS ======================================================================


		for(Map.Entry<Key, Value> entry : scan){
			groups.add(entry.getKey().getColumnFamily().toString());
		}

		return groups;
	}

	/**
	 * Returns the Hypotheses for the groups that the userId belongs to
	 *
	 * @param userId     The userId to fetch the group Hypotheses for
	 * @param visibility Accumulo authorizations
	 * @param userOwned  Set to true to return the Hypotheses owned by the user in addition to the group Hypotheses
	 * @return All the Hypothesis for each of the given groups
	 */
	public List<Hypothesis> getGroupHypothesesForUser(String userId, String[] visibility, boolean userOwned) throws IOException {
		Preconditions.checkNotNull(userId);
		Preconditions.checkNotNull(visibility);

		final Authorizations auths = new Authorizations(visibility);
		List<Range> hypothesesToFind = new ArrayList<Range>();

		// Fetch which groups the userId belongs to
		Set<String> groups = getGroups(userId, auths);
		List<Range> ranges = new ArrayList<Range>(groups.size());

		// Find which Hypotheses are visible for each group
		if (groups != null && groups.size() > 0) {
			BatchScanner groupsLutScanner = null;
			try {
				groupsLutScanner = persistenceService.createBatchScanner(groupHypothesisLUT, auths);
				for(String group : groups){
					ranges.add(new Range(group));
				}
				groupsLutScanner.setRanges(ranges);

				for(Map.Entry<Key, Value> entry : groupsLutScanner){
					Key startKey = new Key(entry.getKey().getColumnFamily().toString(), entry.getKey().getColumnQualifier().toString());
					Key endKey = startKey.followingKey(PartialKey.ROW_COLFAM);
					hypothesesToFind.add(new Range(startKey, true, endKey, false));
				}
			} catch (TableNotFoundException ex){
                log.error("Table '" + groupHypothesisLUT + "' was not found");
                throw new IOException(ex);
            }
            finally {
				if (groupsLutScanner != null) {
					groupsLutScanner.close();
				}
			}
		}

		// To avoid two separate calls, add the user's Hypotheses if requested
		if (userOwned) {
			hypothesesToFind.add(new Range(userId));
		}

		final List<Hypothesis> foundHypotheses = new ArrayList<Hypothesis>(hypothesesToFind.size());
		if (hypothesesToFind.size() > 0) {
			// Now that we know which Hypothesis that we need, go fetch them
			BatchScanner hypothesesScanner = null;
			Hypothesis activeEntity = null;
			try {
				hypothesesScanner = persistenceService.createBatchScanner(hypothesisTable, auths);
				hypothesesScanner.setRanges(hypothesesToFind);
				final ArrayList<String> fieldsToPopulate = new ArrayList<String>(Arrays.asList("name", "created", "updated", "executed", "queries"));
				for(Map.Entry<Key, Value> entry : hypothesesScanner){
					final String id = entry.getKey().getColumnFamily().toString();

					// Hypothesis are made up of multiple rows.  Since the results are sorted, if we
					// come across a new row, then need to create a new Hypothesis
					if (activeEntity == null || id.compareTo(activeEntity.id) != 0) {
						//activeEntity = new Hypothesis([id:id, hypothesisFeatures:new ArrayList<HypothesisFeature>()])
						activeEntity = new Hypothesis();
						activeEntity.id = id;
						activeEntity.owner = entry.getKey().getRow().toString();
						foundHypotheses.add(activeEntity);
					}

					AccumuloMetadataService.addHypothesisComponent(activeEntity, entry, fieldsToPopulate);
				}
			} catch (TableNotFoundException ex){
                log.error("Table '" + groupHypothesisLUT + "' was not found");
                throw new IOException(ex);
            } finally {
				if (hypothesesScanner != null) {
					hypothesesScanner.close();
				}
			}
		}

		return foundHypotheses;
	}

	/**
	 * Returns the Hypotheses for the groups that the userId belongs to
	 *
	 * @param userId     The userId to fetch the group Hypotheses for
	 * @param visibility Accumulo authorizations
	 * @return All the Hypothesis for each of the given groups
	 */
	public List<Hypothesis> getGroupHypothesesForUser(String userId, String[] visibility) throws IOException {
		return getGroupHypothesesForUser(userId, visibility, false);
	}

}

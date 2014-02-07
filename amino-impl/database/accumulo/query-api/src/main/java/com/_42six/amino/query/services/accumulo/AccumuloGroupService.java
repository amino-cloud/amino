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
	private String groupHypothesisLUT = "amino_group_hypothesis_lookup";

    /** The table containing whom belongs to which groups */
	private String groupMembershipTable = "amino_group_membership";

    /** The table that contains the metadata about the groups */
    private String groupMetadataTable = "amino_group_metadata";

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

    /**
     * Adds the suffix to all of the tables
     * @param suffix The suffix to append to the tables
     */
    @Override
    public void addTableSuffix(String suffix){
        groupHypothesisLUT = groupHypothesisLUT + suffix;
        groupMembershipTable = groupMetadataTable + suffix;
        groupMetadataTable = groupMetadataTable + suffix;
        hypothesisTable = hypothesisTable + suffix;
    }

    @Override
    public String setGroupMetadataTable(String table) {
		return this.groupMetadataTable = table;
	}

	@Override
    public String getGroupHypothesisLUT() {
		return this.groupHypothesisLUT;
	}

	@Override
    public String setGroupHypothesisLUT(String lut) {
		return this.groupHypothesisLUT = lut;
	}

	@Override
    public String setGroupMembershipTable(String memberTable) {
		return this.groupMembershipTable = memberTable;
	}

	@Override
    public String setHypothesisTable(String hypothesisTable) {
		return this.hypothesisTable = hypothesisTable;
	}

    /**
     * Verify that the group exists in the group_metadata table
     * @param group The group name to search for
     * @param visibilities The Accumulo visibilities
     * @return true if the group exists, false otherwise
     * @throws IOException if the group_metadata table does not exist
     */
    @Override
    public boolean verifyGroupExists(String group, Set<String> visibilities) throws IOException {
        MorePreconditions.checkNotNullOrEmpty(group);
        Preconditions.checkNotNull(visibilities);
        final Authorizations auths = new Authorizations(visibilities.toArray(new String[visibilities.size()]));
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
     * @param visibilities The Accumulo visibilities
     * @return true if the user belongs to any groups, false otherwise
     * @throws IOException if the group_membership table does not exist
     */
    @Override
    public boolean verifyUserExists(String user, Set<String> visibilities) throws IOException {
        MorePreconditions.checkNotNullOrEmpty(user);
        Preconditions.checkNotNull(visibilities);
        final Authorizations auths = new Authorizations(visibilities.toArray(new String[visibilities.size()]));
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
	@Override
    public void addToGroup(AddUsersRequest request) throws Exception {
		Preconditions.checkNotNull(request, "Request object was null");
        final String[] tokens = Preconditions.checkNotNull(request.getSecurityTokens(), "Security tokens were null");
        final Authorizations auths = Preconditions.checkNotNull(new Authorizations(tokens), "Could not create Authorizations");       
        final Set<GroupMember> members = Preconditions.checkNotNull(request.getUsers(), "Missing members");
        String groupName = MorePreconditions.checkNotNullOrEmpty(request.getGroupName(), "Missing group name");
        String requester = MorePreconditions.checkNotNullOrEmpty(request.getRequester(), "Missing requester");

        // Make sure prefixed properly
        groupName = groupName.startsWith(TableConstants.GROUP_PREFIX) ? groupName : TableConstants.GROUP_PREFIX + groupName;
        requester = requester.startsWith(TableConstants.USER_PREFIX) ? requester : TableConstants.USER_PREFIX + requester;

		final ArrayList<Mutation> memberEntries = new ArrayList<Mutation>(members.size());
        final ArrayList<Mutation> metaEntries = new ArrayList<Mutation>();

        // Make sure that the requester is an admin for the group and can perform these admin tasks
        final Scanner scanner = persistenceService.createScanner(groupMetadataTable, auths);
        scanner.setRange(new Range(groupName));
        scanner.fetchColumn(new Text("admin"), new Text(requester));

        if(!scanner.iterator().hasNext()){
            throw new Exception("User " + requester + " does not have admin rights to the group " + groupName);
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
	@Override
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


    /**
     * List all of the groups that a particular user can see
     *
     * @param userId     The person for which groups can be seen
     * @param visibilities the db visibility strings
     */
    @Override
    public Set<String> listGroups(String userId, Set<String> visibilities) throws IOException {
        Preconditions.checkNotNull(visibilities);
        return listGroups(userId, new Authorizations(visibilities.toArray(new String[visibilities.size()])));
    }

    /**
     * List all of the groups that a particular user can see
     *
     * @param userId     The person for which groups can be seen
     * @param auths      the db authorizations
     */
    public Set<String> listGroups(String userId, Authorizations auths) throws IOException {
        MorePreconditions.checkNotNullOrEmpty(userId);
        Preconditions.checkNotNull(auths);

        final Set<String> groups = getGroupsForUser(userId, auths);
        groups.addAll(getGroupsForUser("public",auths));

        return groups;
    }

    /**
     * Remove the members from the group
     *
     * @param requester    The person requesting the users to be removed.  Must be an admin for the group
     * @param group        The group to remove from
     * @param members      The members to remove from the group
     * @param visibilities The database authorizations
     */
    @Override
    public void removeUsersFromGroup(String requester, String group, Set<String> members, Set<String> visibilities) throws Exception {
        Preconditions.checkNotNull(visibilities);
        removeUsersFromGroup(requester, group, members, new Authorizations(visibilities.toArray(new String[visibilities.size()])));
    }

    /**
     * Remove the members from the group
     *
     * @param requester The person requesting the users to be removed.  Must be an admin for the group
     * @param group   The group to remove from
     * @param members The members to remove from the group
     * @param auths   The database authorizations
     */
    public void removeUsersFromGroup(String requester, String group, Set<String> members, Authorizations auths) throws Exception {
        MorePreconditions.checkNotNullOrEmpty(requester);
        MorePreconditions.checkNotNullOrEmpty(group);
        MorePreconditions.checkNotNullOrEmpty(members);

        // Make sure prefixes are in place
        if(!requester.startsWith(TableConstants.USER_PREFIX)) { requester = TableConstants.USER_PREFIX + requester; }
        if(!group.startsWith(TableConstants.GROUP_PREFIX)) { group = TableConstants.GROUP_PREFIX + group; }
        final HashSet<String> internalMembers = new HashSet<String>(members.size());
        for(String member : members){
            internalMembers.add(member.startsWith(TableConstants.USER_PREFIX) ? member : TableConstants.USER_PREFIX + member);
        }

        // Fetch admins for the group
        final Scanner adminScanner = persistenceService.createScanner(groupMetadataTable, auths);
        adminScanner.setRange(new Range(group));
        adminScanner.fetchColumnFamily(new Text("admin"));
        HashSet<String> admins = new HashSet<String>();
        for(Map.Entry<Key, Value> entry: adminScanner){
            admins.add(entry.getKey().getColumnQualifier().toString());
        }

        // Make sure requester is an admin for the group
        if(!admins.contains(requester)){
            throw new IllegalArgumentException("requester was not an admin for the group");
        }

        // Make sure that we aren't removing all of the admins
        if(internalMembers.containsAll(admins)){
            throw new IllegalArgumentException("Can not remove all of the admins for the group");
        }

        // Remove the entries from the membership table
        final List<Mutation> membershipEntries = new ArrayList<Mutation>();
        for(String member : internalMembers){
            membershipEntries.add(persistenceService.createDeleteMutation(member, group, "", ""));
        }
        persistenceService.insertRows(membershipEntries, groupMembershipTable);

        // Remove the entries from the metadata table
        final List<Mutation> metaEntries = new ArrayList<Mutation>();
        final Scanner metaScanner = persistenceService.createScanner(groupMetadataTable, auths);
        metaScanner.setRange(new Range(group));
        for(Map.Entry<Key, Value> entry : metaScanner){
            if(internalMembers.contains(entry.getKey().getColumnQualifier().toString())){
                Key k = entry.getKey();
                metaEntries.add(persistenceService.createDeleteMutation(group, k.getColumnFamily().toString(),
                        k.getColumnQualifier().toString(), k.getColumnVisibility().toString()));
            }
        }
        persistenceService.insertRows(metaEntries, groupMetadataTable);
    }

    /**
     * Removes the user from a group.  If no group is provided, they are removed from all of the groups
     *
     * @param requester  The ID of the person requesting that the user be removed
     * @param userId     The ID of the user to remove
     * @param groups     The group to remove from
     * @param visibilities The db visibility strings
     */
    @Override
    public void removeUserFromGroups(String requester, String userId, Set<String> groups, Set<String> visibilities) throws Exception {
        Preconditions.checkNotNull(visibilities);
        removeUserFromGroups(requester, userId, groups, new Authorizations(visibilities.toArray(new String[visibilities.size()])));
    }

    /**
     * Removes the user from a group.  If no group is provided, they are removed from all of the groups
     *
     * @param requester The ID of the person requesting that the user be removed
     * @param userId    The ID of the user to remove
     * @param groups    The group to remove from
     * @param auths     The authorization strings
     */
    public void removeUserFromGroups(String requester, String userId, Set<String> groups, Authorizations auths) throws Exception {
        MorePreconditions.checkNotNullOrEmpty(userId);
        MorePreconditions.checkNotNullOrEmpty(requester);
        Preconditions.checkNotNull(auths);

        final Collection<Mutation> groupMembershipMutations = new ArrayList<Mutation>();
        final Collection<Mutation> groupMetadataMutations = new ArrayList<Mutation>();
        final Collection<Range> metaGroupsToPurgeFrom = new HashSet<Range>();

        // Make sure prefixes in place
        if(!requester.startsWith(TableConstants.USER_PREFIX)) { requester = TableConstants.USER_PREFIX + requester; }
        if(!userId.startsWith(TableConstants.USER_PREFIX)) { userId = TableConstants.USER_PREFIX + userId; }
        Set<String> internalGroups = new HashSet<String>(groups.size());
        for(String group : groups){
            internalGroups.add(group.startsWith(TableConstants.GROUP_PREFIX) ? group : TableConstants.GROUP_PREFIX + group);
        }

        // If no groups provided, remove from any group they might be a part of
        if(internalGroups == null || internalGroups.size() == 0){
            internalGroups = getGroupsForUser(userId, auths);
        }

        for(String group : internalGroups){
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
        if(!requester.startsWith(TableConstants.USER_PREFIX)){requester = TableConstants.USER_PREFIX + requester;}
        if(!userId.startsWith(TableConstants.USER_PREFIX)){userId = TableConstants.USER_PREFIX + userId;}
        if(!group.startsWith(TableConstants.GROUP_PREFIX)){group = TableConstants.GROUP_PREFIX + group;}

        // See if the requester is an admin for the group and has permissions to remove the user
        final Scanner groupMetaScanner = persistenceService.createScanner(groupMetadataTable, auths);
        groupMetaScanner.setRange(new Range(group));
        groupMetaScanner.fetchColumnFamily(new Text("admin"));

        boolean requesterIsAdmin = false;
        int adminsFound = 0;
        for(Map.Entry<Key, Value> entry : groupMetaScanner){
            adminsFound++;
            if(entry.getKey().getColumnQualifier().toString().equals(requester)){
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
     * Returns the Group object that corresponds with the underlying group in the tables.  The requester must be a member
     * in order to get the Group.
     * @param requester   The person trying to fetch the Group
     * @param group       The Group to lookup in the tables
     * @param visibilities  The BigTable visibility strings
     * @return Group representing all of of the members of the group
     * @throws IOException
     */
    @Override
    public Group getGroup(String requester, String group, Set<String> visibilities) throws IOException {
        Preconditions.checkNotNull(visibilities);
        return getGroup(requester, group, new Authorizations(visibilities.toArray(new String[visibilities.size()])));
    }

    /**
     * Returns the Group object that corresponds with the underlying group in the tables.  The requester must be a member
     * in order to get the Group.  If the group doesn't exist, null is returned
     * @param requester   The person trying to fetch the Group
     * @param group       The Group to lookup in the tables
     * @param auths       The BigTable authorizations
     * @return Group representing all of of the members of the group
     * @throws IOException
     */
    public Group getGroup(String requester, String group, Authorizations auths) throws IOException {
        MorePreconditions.checkNotNullOrEmpty(requester);
        MorePreconditions.checkNotNullOrEmpty(group);
        Preconditions.checkNotNull(auths);

        Group returnGroup;
        boolean requesterPartOfGroup = false;

        // Make sure prefixes are present
        if(!requester.startsWith(TableConstants.USER_PREFIX)){ requester = TableConstants.USER_PREFIX + requester; }
        if(!group.startsWith(TableConstants.GROUP_PREFIX)){
            returnGroup = new Group(group);
            group = TableConstants.GROUP_PREFIX + group;
        } else {
            returnGroup = new Group(group.substring(TableConstants.GROUP_PREFIX.length()));
        }

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

                if(member.equals(requester)){
                    requesterPartOfGroup = true;
                }

                member = member.startsWith(TableConstants.USER_PREFIX) ? member.substring(TableConstants.USER_PREFIX.length()) : member;

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

        if(!requesterPartOfGroup){
            throw new IOException(String.format("User <%s> is not part of group <%s> and does not have permission to list members", requester, group));
        }

        return returnGroup;
    }

	/**
	 * Fetches the groups that a particular id belongs to
	 *
	 * @param userId     The userId to fetch the groups for or empty to fetch them all
	 * @param visibilities The Accumulo authorizations
	 * @return Set<String> of groups for the particular userId
	 */
	@Override
    public Set<String> getGroupsForUser(String userId, Set<String> visibilities) throws IOException {
		Preconditions.checkNotNull(visibilities);
		return getGroupsForUser(userId, new Authorizations(visibilities.toArray(new String[visibilities.size()])));
	}

    /**
     * Fetches the groups that a particular id belongs to
     *
     * @param userId The userId to fetch the groups for or empty to fetch them all
     * @param auths  The Accumulo authorizations
     * @return Set<String> of groups for the particular userId
     */
    public Set<String> getGroupsForUser(String userId, Authorizations auths) throws IOException {
        MorePreconditions.checkNotNullOrEmpty(userId);
        Preconditions.checkNotNull(auths);

        // Make sure we have the internal USER prefix appended if it's not already there
        if(!userId.startsWith(TableConstants.USER_PREFIX)){
            userId = TableConstants.USER_PREFIX + userId;
        }

        // Everybody is part of the public group
        Set<String> groups = new HashSet<String>();
        groups.add(TableConstants.PUBLIC_GROUP.substring(TableConstants.GROUP_PREFIX.length()));

        Scanner scan;
        try{
            scan = persistenceService.createScanner(groupMembershipTable, auths);
        }  catch (TableNotFoundException ex){
            log.error("Table '" + groupMembershipTable + "' was not found");
            throw new IOException(ex);
        }
        scan.setRange(new Range(userId));

        for(Map.Entry<Key, Value> entry : scan){
            groups.add(entry.getKey().getColumnFamily().toString().substring(TableConstants.GROUP_PREFIX.length()));
        }

        return groups;
    }

	/**
	 * Returns the Hypotheses for the groups that the userId belongs to
	 *
	 * @param userId     The userId to fetch the group Hypotheses for
	 * @param visibilities Accumulo authorizations
	 * @param userOwned  Set to true to return the Hypotheses owned by the user in addition to the group Hypotheses
	 * @return All the Hypothesis for each of the given groups
	 */
	@Override
    public List<Hypothesis> getGroupHypothesesForUser(String userId, Set<String> visibilities, boolean userOwned) throws IOException {
		Preconditions.checkNotNull(userId);
		Preconditions.checkNotNull(visibilities);

		final Authorizations auths = new Authorizations(visibilities.toArray(new String[visibilities.size()]));
		List<Range> hypothesesToFind = new ArrayList<Range>();

		// Fetch which groups the userId belongs to
		Set<String> groups = getGroupsForUser(userId, auths);
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
	 * @param visibilities Accumulo authorizations
	 * @return All the Hypothesis for each of the given groups
	 */
	@Override
    public List<Hypothesis> getGroupHypothesesForUser(String userId, Set<String> visibilities) throws IOException {
		return getGroupHypothesesForUser(userId, visibilities, false);
	}

}

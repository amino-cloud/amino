package com._42six.amino.query.services.accumulo;

import com._42six.amino.common.Group;
import com._42six.amino.common.GroupMember;
import com._42six.amino.common.query.requests.AddUsersRequest;
import com._42six.amino.common.query.requests.CreateGroupRequest;
import com.google.common.collect.Sets;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.io.Text;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

public class AccumuloGroupServiceTest {

    /***********************
     * Setup Methods
     **********************/
    static TableOperations tableOps;
    static Connector connector;
    static Set<String> perms = Sets.newHashSet("U");
    static Authorizations auths = new Authorizations("U");

    private static final String METADATA_TABLE = "amino_group_metadata";
    private static final String MEMBERSHIP_TABLE = "amino_group_membership";
    private static final String HYPOTHESIS_LOOKUP = "amino_group_hypothesis_lookup";

    static AccumuloGroupService groupService;
    static AccumuloPersistenceService persistenceService;

    @BeforeClass
    public static void setupMock(){

        try{
            connector = new MockInstance("mock").getConnector("username", "password");
            tableOps = connector.tableOperations();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        persistenceService = new AccumuloPersistenceService(connector);
        groupService = new AccumuloGroupService(persistenceService);
        groupService.setGroupHypothesisLUT(HYPOTHESIS_LOOKUP);
        groupService.setGroupMembershipTable(MEMBERSHIP_TABLE);
        groupService.setGroupMetadataTable(METADATA_TABLE);
    }

    @Test
    public void createGroup() throws Exception {
        if(tableOps.exists(MEMBERSHIP_TABLE)){
            tableOps.delete(MEMBERSHIP_TABLE);
        }
        tableOps.create(MEMBERSHIP_TABLE);

        if(tableOps.exists(METADATA_TABLE)){
            tableOps.delete(METADATA_TABLE);
        }
        tableOps.create(METADATA_TABLE);

        final Set<GroupMember> members = new HashSet<GroupMember>();
        members.add(new GroupMember("member1", Sets.newHashSet(Group.GroupRole.ADMIN, Group.GroupRole.CONTRIBUTOR, Group.GroupRole.VIEWER)));
        members.add(new GroupMember("member2", Sets.newHashSet(Group.GroupRole.CONTRIBUTOR, Group.GroupRole.VIEWER)));
        members.add(new GroupMember("member3", Sets.newHashSet(Group.GroupRole.VIEWER)));

        final Group g = new Group();
        g.setDateCreated(1234);
        g.setCreatedBy("TestUser1");
        g.setMembers(members);
        g.setGroupName("TestGroup1");

        final CreateGroupRequest request = new CreateGroupRequest();
        request.setGroup(g);
        request.setRequester("USER|requester");
        request.setSecurityTokens(perms.toArray(new String[0]));

        groupService.createGroup(request);

        final Scanner metaScanner = connector.createScanner(METADATA_TABLE, auths);
        metaScanner.setRange(new Range("GROUP|TestGroup1"));
        metaScanner.fetchColumn(new Text("admin"), new Text("USER|member1"));
        metaScanner.fetchColumn(new Text("contributor"), new Text("USER|member1"));
        metaScanner.fetchColumn(new Text("contributor"), new Text("USER|member2"));
        metaScanner.fetchColumn(new Text("viewer"), new Text("USER|member1"));
        metaScanner.fetchColumn(new Text("viewer"), new Text("USER|member2"));
        metaScanner.fetchColumn(new Text("viewer"), new Text("USER|member3"));
        metaScanner.fetchColumn(new Text("created_by"), new Text("USER|TestUser1"));
        metaScanner.fetchColumnFamily(new Text("created_date"));

        int i = 0;
        for(Map.Entry<Key, Value> entry: metaScanner){
            i++;
        }
        assertTrue(i == 8);

        final BatchScanner memberScanner = connector.createBatchScanner(MEMBERSHIP_TABLE, auths, 8);
        memberScanner.setRanges(Sets.newHashSet(new Range("USER|member1"), new Range("USER|member2"), new Range("USER|member3")));
        i = 0;
        for(Map.Entry<Key, Value> e : memberScanner){
            i++;
        }
        assertTrue(i==3);
    }

    private void initalizeTables() throws Exception {
        if(tableOps.exists(MEMBERSHIP_TABLE)){
            tableOps.delete(MEMBERSHIP_TABLE);
        }
        tableOps.create(MEMBERSHIP_TABLE);

        if(tableOps.exists(METADATA_TABLE)){
            tableOps.delete(METADATA_TABLE);
        }
        tableOps.create(METADATA_TABLE);

        // Initialize the membership table
        persistenceService.insertRow("USER|member1", "GROUP|group1" , "", "", "", MEMBERSHIP_TABLE);
        persistenceService.insertRow("USER|member1", "GROUP|group2" , "", "", "", MEMBERSHIP_TABLE);
        persistenceService.insertRow("USER|member1", "GROUP|group3" , "", "", "", MEMBERSHIP_TABLE);

        persistenceService.insertRow("USER|member2", "GROUP|group1" , "", "", "", MEMBERSHIP_TABLE);
        persistenceService.insertRow("USER|member2", "GROUP|group2" , "", "", "", MEMBERSHIP_TABLE);
        persistenceService.insertRow("USER|member2", "GROUP|group3" , "", "", "", MEMBERSHIP_TABLE);

        persistenceService.insertRow("USER|member3", "GROUP|group1" , "", "", "", MEMBERSHIP_TABLE);
        persistenceService.insertRow("USER|member3", "GROUP|group2" , "", "", "", MEMBERSHIP_TABLE);
        persistenceService.insertRow("USER|member3", "GROUP|group3" , "", "", "", MEMBERSHIP_TABLE);
        persistenceService.insertRow("USER|public", "GROUP|group3", "", "", "", MEMBERSHIP_TABLE);

        // Initialize the metadata table
        persistenceService.insertRow("GROUP|group1", "admin" , "USER|member1", "", "", METADATA_TABLE);
        persistenceService.insertRow("GROUP|group1", "contributor" , "USER|member1", "", "", METADATA_TABLE);
        persistenceService.insertRow("GROUP|group1", "contributor" , "USER|member2", "", "", METADATA_TABLE);
        persistenceService.insertRow("GROUP|group1", "viewer" , "USER|member1", "", "", METADATA_TABLE);
        persistenceService.insertRow("GROUP|group1", "viewer" , "USER|member2", "", "", METADATA_TABLE);
        persistenceService.insertRow("GROUP|group1", "viewer" , "USER|member3", "", "", METADATA_TABLE);
        persistenceService.insertRow("GROUP|group1", "created_by" , "USER|creator", "", "", METADATA_TABLE);
        persistenceService.insertRow("GROUP|group1", "created_date" , "12345", "", "", METADATA_TABLE);

        persistenceService.insertRow("GROUP|group2", "admin" , "USER|member1", "", "", METADATA_TABLE);
        persistenceService.insertRow("GROUP|group2", "admin" , "USER|member2", "", "", METADATA_TABLE);
        persistenceService.insertRow("GROUP|group2", "contributor" , "USER|member1", "", "", METADATA_TABLE);
        persistenceService.insertRow("GROUP|group2", "contributor" , "USER|member2", "", "", METADATA_TABLE);
        persistenceService.insertRow("GROUP|group2", "viewer" , "USER|member1", "", "", METADATA_TABLE);
        persistenceService.insertRow("GROUP|group2", "viewer" , "USER|member2", "", "", METADATA_TABLE);
        persistenceService.insertRow("GROUP|group2", "viewer" , "USER|member3", "", "", METADATA_TABLE);
        persistenceService.insertRow("GROUP|group2", "created_by" , "USER|creator", "", "", METADATA_TABLE);
        persistenceService.insertRow("GROUP|group2", "created_date" , "12345", "", "", METADATA_TABLE);

        persistenceService.insertRow("GROUP|group3", "admin" , "USER|member1", "", "", METADATA_TABLE);
        persistenceService.insertRow("GROUP|group3", "contributor" , "USER|member1", "", "", METADATA_TABLE);
        persistenceService.insertRow("GROUP|group3", "contributor" , "USER|member2", "", "", METADATA_TABLE);
        persistenceService.insertRow("GROUP|group3", "viewer" , "USER|member1", "", "", METADATA_TABLE);
        persistenceService.insertRow("GROUP|group3", "viewer" , "USER|member2", "", "", METADATA_TABLE);
        persistenceService.insertRow("GROUP|group3", "viewer" , "USER|member3", "", "", METADATA_TABLE);
        persistenceService.insertRow("GROUP|group3", "viewer" , "USER|public", "", "", METADATA_TABLE);
        persistenceService.insertRow("GROUP|group3", "created_by" , "USER|creator", "", "", METADATA_TABLE);
        persistenceService.insertRow("GROUP|group3", "created_date" , "12345", "", "", METADATA_TABLE);
    }

    @Test
    /**
     * Test that an admin user can remove users from a group
     */
    public void removeUserFromSpecificGroups() throws Exception {
        initalizeTables();
        final HashSet<String> groups = Sets.newHashSet("GROUP|group2", "GROUP|group3");

        // Remove the user from the groups
        groupService.removeUserFromGroups("USER|member1", "USER|member2", groups, auths);

        // Check the membership table
        final Scanner memberScanner = persistenceService.createScanner(MEMBERSHIP_TABLE, auths);
        memberScanner.setRange(new Range());
        int entries = 0;
        for(Map.Entry<Key, Value> entry : memberScanner){
            entries++;
            if(entry.getKey().getRow().toString().compareTo("USER|member2") == 0){
                String group = entry.getKey().getColumnFamily().toString();
                assertTrue(!groups.contains(group));
            }
        }
        assertEquals(8, entries);

        // Check the metadata table
        final Scanner metadataScanner = persistenceService.createScanner(METADATA_TABLE, auths);
        metadataScanner.setRange(new Range());
        entries = 0;
        for(Map.Entry<Key, Value> entry : metadataScanner){
            entries++;
            // Found a group that they shouldn't be in.  Make sure they aren't
            if(groups.contains(entry.getKey().getRow().toString())){
                // Don't care if they are still listed as the creator of the group
                if(entry.getKey().getColumnFamily().toString().compareTo("created_by") != 0){
                    assertTrue(entry.getKey().getColumnQualifier().toString().compareTo("USER|member2") != 0);
                }
            }
        }
        assertEquals(21, entries);
    }

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    /**
     * Tests that users that don't have admin privs on a group shouldn't be alowed to remove anyone but themselves
     */
    public void removeUserNotAdminNotSelf() throws Exception {
        initalizeTables();
        final HashSet<String> groups = Sets.newHashSet("GROUP|group2", "GROUP|group3");

        // We expect there to be an exception
        exception.expect(Exception.class);
        exception.expectMessage(String.format("'USER|member2' does not have permission to remove 'USER|member3' from group 'GROUP|group3'"));

        // Remove the user from the groups
        groupService.removeUserFromGroups("USER|member2", "USER|member3", groups, auths);
    }

    @Test
    /**
     * Tests that a non-admin user can remove themselves from a group
     */
    public void removeUserSelfNotAdmin() throws Exception {
        initalizeTables();
        final HashSet<String> groups = Sets.newHashSet("GROUP|group2", "GROUP|group3");

        // Remove the user from the groups
        groupService.removeUserFromGroups("USER|member2", "USER|member2", groups, auths);

        // Check the membership table
        final Scanner memberScanner = persistenceService.createScanner(MEMBERSHIP_TABLE, auths);
        memberScanner.setRange(new Range());
        int entries = 0;
        for(Map.Entry<Key, Value> entry : memberScanner){
            entries++;
            if(entry.getKey().getRow().toString().compareTo("USER|member2") == 0){
                String group = entry.getKey().getColumnFamily().toString();
                assertTrue(!groups.contains(group));
            }
        }
        assertEquals(8, entries);

        // Check the metadata table
        final Scanner metadataScanner = persistenceService.createScanner(METADATA_TABLE, auths);
        metadataScanner.setRange(new Range());
        entries = 0;
        for(Map.Entry<Key, Value> entry : metadataScanner){
            entries++;
            // Found a group that they shouldn't be in.  Make sure they aren't
            if(groups.contains(entry.getKey().getRow().toString())){
                // Don't care if they are still listed as the creator of the group
                if(entry.getKey().getColumnFamily().toString().compareTo("created_by") != 0){
                    assertTrue(entry.getKey().getColumnQualifier().toString().compareTo("USER|member2") != 0);
                }
            }
        }
        assertEquals(21, entries);
    }

    @Test
    /**
     * Tests to make sure that we can't remove the last admin from a group
     */
    public void removeLastAdmin_admin() throws Exception{
        initalizeTables();

        // We expect there to be an exception as the admin can't remove the last admin
        exception.expect(Exception.class);
        exception.expectMessage(String.format("'USER|member1' does not have permission to remove 'USER|member1' from group 'GROUP|group1'"));

        groupService.removeUserFromGroups("USER|member1", "USER|member1", Sets.newHashSet("GROUP|group1"), auths);
    }

    @Test
    public void verifyUserExists() throws Exception {
        initalizeTables();

        assertTrue(groupService.verifyUserExists("USER|member1", perms));
        assertTrue(!groupService.verifyUserExists("USER|BogusUser", perms));
    }

    @Test
    public void verifyGroupExists() throws Exception {
        initalizeTables();

        assertTrue(groupService.verifyGroupExists("GROUP|group1", perms));
        assertTrue(!groupService.verifyGroupExists("GROUP|BogusGroup", perms));
    }

    @Test
    /**
     * Tests that an admin for the group can add a user to the group, and that a non-admin can not
     */
    public void addToGroup() throws Exception {
        initalizeTables();

        final Set<GroupMember> members = new HashSet<GroupMember>();
        members.add(new GroupMember("newMemberAll", Sets.newHashSet(Group.GroupRole.VIEWER, Group.GroupRole.ADMIN, Group.GroupRole.CONTRIBUTOR)));

        final AddUsersRequest request = new AddUsersRequest();
        request.setGroupName("group1");
        request.setSecurityTokens(perms.toArray(new String[0]));
        request.setRequester("member1");
        request.setUsers(members);

        groupService.addToGroup(request);

        // Check the membership table
        final Scanner memberScanner = persistenceService.createScanner(MEMBERSHIP_TABLE, auths);
        memberScanner.setRange(new Range());
        int entries = 0;
        for(Map.Entry<Key, Value> entry : memberScanner){
            entries++;
            if(entry.getKey().getRow().toString().compareTo("USER|newMemberAll") == 0){
                String group = entry.getKey().getColumnFamily().toString();
                assertTrue(group.compareTo("GROUP|group1") == 0);
            }
        }
        // Make sure that only the one record was added
        assertEquals(11, entries);

        // Check the metadata table
        final Scanner metadataScanner = persistenceService.createScanner(METADATA_TABLE, auths);
        metadataScanner.setRange(new Range("GROUP|group1"));
        metadataScanner.fetchColumn(new Text("admin"), new Text("USER|newMemberAll"));
        metadataScanner.fetchColumn(new Text("contributor"), new Text("USER|newMemberAll"));
        metadataScanner.fetchColumn(new Text("viewer"), new Text("USER|newMemberAll"));
        entries = 0;
        for(Map.Entry<Key, Value> entry : metadataScanner){
            entries++;
        }
        // Make sure all the rows were added
        assertEquals(3, entries);

        // Verify other rows
        metadataScanner.clearColumns();
        metadataScanner.setRange(new Range());
        entries = 0;
        for(Map.Entry<Key, Value> entry : metadataScanner){
            entries++;
        }
        // Make sure those were the only rows added
        assertEquals(29, entries);
        assertTrue("", true);


        //
        // Now try to add a new user as a non-admin
        //
        exception.expect(Exception.class);
        exception.expectMessage(String.format("USER|member2 does not have admin rights to the group GROUP|group1"));
        request.setRequester("member2");
        groupService.addToGroup(request);
    }

    @Test
    /**
     * Tests that we can retrieve a Group from the DB and rehydrate the object
     * @throws Exception
     */
    public void getGroup() throws Exception {
        initalizeTables();

        final Group actual = groupService.getGroup("member1", "group1", auths);
        assertNotNull("Fetched null group", actual);

        final Set<GroupMember> members = new HashSet<GroupMember>();
        members.add(new GroupMember("member1", Sets.newHashSet(Group.GroupRole.ADMIN, Group.GroupRole.CONTRIBUTOR, Group.GroupRole.VIEWER)));
        members.add(new GroupMember("member2", Sets.newHashSet(Group.GroupRole.CONTRIBUTOR, Group.GroupRole.VIEWER)));
        members.add(new GroupMember("member3", Sets.newHashSet(Group.GroupRole.VIEWER)));

        final Group expected = new Group();
        expected.setDateCreated(12345);
        expected.setCreatedBy("creator");
        expected.setMembers(members);
        expected.setGroupName("group1");

        assertEquals(expected, actual);
    }

    @Test
    /**
     * Test that we can get the groups that a user belongs to
     */
    public void getGroupsForUser() throws Exception{
        initalizeTables();

        final Set<String> expected = Sets.newHashSet("public", "group1", "group2", "group3");

        // Check to see that it works with the prefix
        assertEquals(expected, groupService.getGroupsForUser("USER|member1", auths));

        // Check to see that it works without the prefix
        assertEquals(expected, groupService.getGroupsForUser("member1", auths));

        // Check for a user that doesn't exist
        assertEquals(Sets.newHashSet("public"), groupService.getGroupsForUser("memberFake", auths));
    }

    @Test
    /**
     * Test that we can get all of the groups that a user can see, which includes the groups they are in plus the public groups
     */
    public void listGroups() throws Exception {
        initalizeTables();
        Set<String> expected = Sets.newHashSet("group1", "group2", "group3", "public");

        // Remove member2 from group3 so that we can see that we still get group3 because of the public member
        groupService.removeUserFromGroups("USER|member2", "USER|member2", Sets.newHashSet("GROUP|group3"), auths);

        assertEquals(expected, groupService.listGroups("member2", auths));

    }

    @Test @Ignore
    /**
     * This tests that we can use the ReverseFeatureCombiner when using a restriction feature
     */
    public void useReverseWithRestriction() throws Exception {
        throw new NotImplementedException();
        // TODO - Reverse the order of doing the lookups if there is a restriction involved
    }

    @Test
    public void removeUsersFromGroup_notAdmin() throws Exception {
        initalizeTables();
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("requester was not an admin for the group");
        groupService.removeUsersFromGroup("USER|NotAdmin", "GROUP|group1", Sets.newHashSet("USER|member1"), auths);
    }

    @Test
    public void removeUsersFromGroup_noMoreAdmins() throws Exception {
        initalizeTables();
        Set<String> users = Sets.newHashSet("USER|member1", "USER|member3");
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("Can not remove all of the admins for the group");
        groupService.removeUsersFromGroup("USER|member1", "GROUP|group3", users, auths);
    }

    @Test
    public void removeUsersFromGroup_invalidUsers() throws Exception {
        initalizeTables();
        Set<String> users = Sets.newHashSet("USER|member2", "USER|fakeUser");
        groupService.removeUsersFromGroup("USER|member1", "GROUP|group3", users, auths);

        // Check the membership table
        final Scanner memberScanner = persistenceService.createScanner(MEMBERSHIP_TABLE, auths);
        memberScanner.setRange(new Range());
        int entries = 0;
        for(Map.Entry<Key, Value> entry : memberScanner){
            entries++;
            if(users.contains(entry.getKey().getRow().toString())){
                String group = entry.getKey().getColumnFamily().toString();
                assertTrue(group.compareTo("GROUP|group3") != 0);
            }
        }
        assertEquals(9, entries);

        // Check the metadata table
        final Scanner metadataScanner = persistenceService.createScanner(METADATA_TABLE, auths);
        metadataScanner.setRange(new Range());
        entries = 0;
        for(Map.Entry<Key, Value> entry : metadataScanner){
            entries++;
            // Found a group that they shouldn't be in.  Make sure they aren't
            if(entry.getKey().getRow().toString().equals("GROUP|group3")){
                // Don't care if they are still listed as the creator of the group
                if(entry.getKey().getColumnFamily().toString().compareTo("created_by") != 0){
                    assertTrue(!users.contains(entry.getKey().getColumnQualifier().toString()));
                }
            }
        }
        assertEquals(24, entries);
    }

    @Test
    public void removeUsersFromGroup() throws Exception {
        initalizeTables();
        Set<String> users = Sets.newHashSet("USER|member2", "USER|member3");
        groupService.removeUsersFromGroup("USER|member1", "GROUP|group3", users, auths);

        // Check the membership table
        final Scanner memberScanner = persistenceService.createScanner(MEMBERSHIP_TABLE, auths);
        memberScanner.setRange(new Range());
        int entries = 0;
        for(Map.Entry<Key, Value> entry : memberScanner){
            entries++;
            if(users.contains(entry.getKey().getRow().toString())){
                String group = entry.getKey().getColumnFamily().toString();
                assertTrue(group.compareTo("GROUP|group3") != 0);
            }
        }
        assertEquals(8, entries);

        // Check the metadata table
        final Scanner metadataScanner = persistenceService.createScanner(METADATA_TABLE, auths);
        metadataScanner.setRange(new Range());
        entries = 0;
        for(Map.Entry<Key, Value> entry : metadataScanner){
            entries++;
            // Found a group that they shouldn't be in.  Make sure they aren't
            if(entry.getKey().getRow().toString().equals("GROUP|group3")){
                // Don't care if they are still listed as the creator of the group
                if(entry.getKey().getColumnFamily().toString().compareTo("created_by") != 0){
                    assertTrue(!users.contains(entry.getKey().getColumnQualifier().toString()));
                }
            }
        }
        assertEquals(23, entries);
    }
}

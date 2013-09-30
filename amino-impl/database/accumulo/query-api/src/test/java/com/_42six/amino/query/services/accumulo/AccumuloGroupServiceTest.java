package com._42six.amino.query.services.accumulo;

import com._42six.amino.common.Group;
import com._42six.amino.common.GroupMember;
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
import org.apache.hadoop.io.Text;
import org.eclipse.jdt.internal.core.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class AccumuloGroupServiceTest {

    /***********************
     * Setup Methods
     **********************/
    static TableOperations tableOps;
    static Connector connector;
    static String[] perms = { "U" };
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
        tableOps.create(MEMBERSHIP_TABLE);
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
        request.setRequestor("USER|requestor");
        request.setSecurityTokens(perms);

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
        Assert.isTrue(i == 8);

        final BatchScanner memberScanner = connector.createBatchScanner(MEMBERSHIP_TABLE, auths, 8);
        memberScanner.setRanges(Sets.newHashSet(new Range("USER|member1"), new Range("USER|member2"), new Range("USER|member3")));
        i = 0;
        for(Map.Entry<Key, Value> e : memberScanner){
            i++;
        }
        Assert.isTrue(i == 3);
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
                Assert.isTrue(!groups.contains(group));
            }
        }
        Assert.isTrue(entries == 7);

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
                    Assert.isTrue(entry.getKey().getColumnQualifier().toString().compareTo("USER|member2") != 0);
                }
            }
        }
        Assert.isTrue(entries == 20);
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
                Assert.isTrue(!groups.contains(group));
            }
        }
        Assert.isTrue(entries == 7);

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
                    Assert.isTrue(entry.getKey().getColumnQualifier().toString().compareTo("USER|member2") != 0);
                }
            }
        }
        Assert.isTrue(entries == 20);
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

}

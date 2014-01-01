package com._42six.amino.query.services.accumulo.thrift;

import com._42six.amino.common.thrift.*;
import com._42six.amino.query.services.accumulo.AccumuloGroupService;
import com._42six.amino.query.services.accumulo.AccumuloPersistenceService;
import com._42six.amino.query.thrift.services.ThriftGroupServiceHandler;
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
import org.apache.thrift.TException;
import org.junit.*;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class AccumuloGroupServiceHandlerTest {
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
    static ThriftGroupServiceHandler serviceHandler;

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
        serviceHandler = new ThriftGroupServiceHandler(groupService);
    }

    @Before
    public void initalizeTables() throws Exception {
        wipeTables();

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

    private static void wipeTables() throws Exception {
        if(tableOps.exists(MEMBERSHIP_TABLE)){
            tableOps.delete(MEMBERSHIP_TABLE);
        }
        tableOps.create(MEMBERSHIP_TABLE);

        if(tableOps.exists(METADATA_TABLE)){
            tableOps.delete(METADATA_TABLE);
        }
        tableOps.create(METADATA_TABLE);

        if(tableOps.exists(HYPOTHESIS_LOOKUP)){
            tableOps.delete(HYPOTHESIS_LOOKUP);
        }
        tableOps.create(HYPOTHESIS_LOOKUP);
    }

    @Test
    public void createGroupTest() throws Exception {
        wipeTables();

        final Set<TGroupMember> members = new HashSet<TGroupMember>(2);
        members.add(new TGroupMember("testUser1", Sets.newHashSet(TGroupRole.ADMIN, TGroupRole.CONTRIBUTOR, TGroupRole.VIEWER)));
        members.add(new TGroupMember("testUser2", Sets.newHashSet(TGroupRole.VIEWER)));
        final TGroup group = new TGroup("testGroup", "testUser1", 0, members);
        final TCreateGroupRequest request = new TCreateGroupRequest(group, "testUser1", Sets.newHashSet("U"));
        serviceHandler.createGroup(request);

        // Make sure that there are no extra rows
        int actual = 0;
        int expected = 6;
        final Scanner metaScanner = persistenceService.createScanner(METADATA_TABLE, auths);
        metaScanner.setRange(new Range());
        for(Map.Entry<Key, Value> entry : metaScanner){
            actual++;
        }
        Assert.assertEquals(expected, actual);

        // Make sure that the rows that we think should be there are there
        actual = 0;
        metaScanner.setRange(new Range("GROUP|testGroup"));
        metaScanner.fetchColumn(new Text("admin"), new Text("USER|testUser1"));
        metaScanner.fetchColumn(new Text("contributor"), new Text("USER|testUser1"));
        metaScanner.fetchColumn(new Text("viewer"), new Text("USER|testUser1"));
        metaScanner.fetchColumn(new Text("viewer"), new Text("USER|testUser2"));
        metaScanner.fetchColumn(new Text("created_by"), new Text("USER|testUser1"));
        metaScanner.fetchColumnFamily(new Text("created_date"));
        //metaScanner.fetchColumn(new Text("created_date"), new Text("0"));

        // Count the rows
        for(Map.Entry<Key, Value> entry : metaScanner){
            actual++;
        }

        Assert.assertEquals(expected, actual);

        // Make sure only those rows created
        final Scanner memberScanner = persistenceService.createScanner(MEMBERSHIP_TABLE, auths);
        actual = 0;
        expected = 2;
        for(Map.Entry<Key, Value> entry : memberScanner){
            actual++;
        }
        Assert.assertEquals(expected, actual);

        // Make sure right rows created
        actual = 0;
        final BatchScanner memberBatchScanner = persistenceService.createBatchScanner(MEMBERSHIP_TABLE, auths);
        memberBatchScanner.setRanges(Sets.newHashSet(new Range("USER|testUser1"), new Range("USER|testUser2")));
        for(Map.Entry<Key, Value> entry : memberScanner){
            actual++;
        }
        Assert.assertEquals(expected, actual);
        memberBatchScanner.close();
    }

    @Test
    public void verifyGroupExistsTest() throws Exception {
        Assert.assertTrue(serviceHandler.verifyGroupExists("group1", perms));
        Assert.assertTrue(serviceHandler.verifyGroupExists("group2", perms));
        Assert.assertTrue(serviceHandler.verifyGroupExists("group3", perms));
        Assert.assertTrue(serviceHandler.verifyGroupExists("GROUP|group1", perms));
        Assert.assertFalse(serviceHandler.verifyGroupExists("BogusGroup", perms));
        Assert.assertFalse(serviceHandler.verifyGroupExists("GROUP|BogusGroup", perms));
    }

    @Test
    public void verifyUserExistsTest() throws TException {
        Assert.assertTrue(serviceHandler.verifyUserExists("member1", perms));
        Assert.assertTrue(serviceHandler.verifyUserExists("member2", perms));
        Assert.assertTrue(serviceHandler.verifyUserExists("member3", perms));
        Assert.assertTrue(serviceHandler.verifyUserExists("USER|member2", perms));
        Assert.assertFalse(serviceHandler.verifyUserExists("BogusMember", perms));
        Assert.assertFalse(serviceHandler.verifyUserExists("USER|BogusMember", perms));
    }

    @Test
    public void listGroupsTest() throws TException {
        final Set<String> expected = Sets.newHashSet("public", "group1", "group2", "group3");
        final Set<String> actual = serviceHandler.listGroups("member1", perms);
        Assert.assertEquals(expected, actual);
        Assert.assertEquals(Sets.newHashSet("group3", "public"), serviceHandler.listGroups("bogusUser", perms));
    }

    @Test
    public void removeUserFromGroupsTest() throws TException {
        Assert.assertEquals(Sets.newHashSet("public", "group1", "group2", "group3"), serviceHandler.listGroups("member2", perms));
        serviceHandler.removeUserFromGroups("member1", "member2", Sets.newHashSet("group1"), perms);
        Assert.assertEquals(Sets.newHashSet("public", "group2", "group3"), serviceHandler.listGroups("member2", perms));
    }

    @Test
    public void getGroupsForUserTest() throws TException {
        Set<String> expected = Sets.newHashSet("group1", "group2", "group3", "public");
        Assert.assertEquals(expected, serviceHandler.getGroupsForUser("member2", perms));
        Assert.assertEquals(Sets.newHashSet("public"), serviceHandler.getGroupsForUser("bogus", perms));
    }

    @Test
    public void addToGroupTest() throws Exception {
        final Set<TGroupMember> users = new HashSet<TGroupMember>(2);
        final TGroupMember member4 = new TGroupMember("member4", Sets.newHashSet(TGroupRole.VIEWER, TGroupRole.CONTRIBUTOR));
        final TGroupMember member5 = new TGroupMember("member5", Sets.newHashSet(TGroupRole.VIEWER));
        users.add(member4);
        users.add(member5);

        final TAddUsersRequest request = new TAddUsersRequest("group2", users, "member1", perms);
        serviceHandler.addToGroup(request);
        final TGroup actual = serviceHandler.getGroup("member1", "group2", perms);

        Assert.assertTrue(actual.getMembers().containsAll(Sets.newHashSet(member4, member5)));
    }

    @Test
    public void getGroupTest() throws Exception {
        final Set<TGroupMember> members = Sets.newHashSet(
                new TGroupMember("member1", Sets.newHashSet(TGroupRole.ADMIN, TGroupRole.CONTRIBUTOR, TGroupRole.VIEWER)),
                new TGroupMember("member2", Sets.newHashSet(TGroupRole.CONTRIBUTOR, TGroupRole.VIEWER)),
                new TGroupMember("member3", Sets.newHashSet(TGroupRole.VIEWER)),
                new TGroupMember("public", Sets.newHashSet(TGroupRole.VIEWER))
        );
        final TGroup expected = new TGroup("group3", "creator", 12345, members);
        final TGroup actual = serviceHandler.getGroup("member1", "group3", perms);
        Assert.assertEquals(expected, actual);
    }

    @Test @Ignore
    public void getGroupHypothesesForUserTest() throws Exception {
        throw new NotImplementedException();
    }

    @Test
    public void removeUsersFromGroupTest() throws Exception {
        serviceHandler.removeUsersFromGroup("member1", "group2", Sets.newHashSet("member2", "member3"), perms);
        Assert.assertFalse(serviceHandler.listGroups("member2", perms).contains("group2"));
        Assert.assertFalse(serviceHandler.listGroups("member3", perms).contains("group2"));
        Assert.assertTrue(serviceHandler.listGroups("member1", perms).contains("group2"));
        Assert.assertTrue(serviceHandler.listGroups("member2", perms).containsAll(Sets.newHashSet("group1", "group3")));
    }

}

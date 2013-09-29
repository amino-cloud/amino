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
import org.junit.Test;

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

    @BeforeClass
    public static void setupMock(){

        try{
            connector = new MockInstance("mock").getConnector("username", "password");
            tableOps = connector.tableOperations();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        groupService = new AccumuloGroupService(new AccumuloPersistenceService(connector));
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
}

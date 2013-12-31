package com._42six.amino.common.query.requests;

import com._42six.amino.common.Group;
import com._42six.amino.common.GroupMember;

import java.util.Set;

/**
 * Adds users to a group.  For each user, their individual roles should be provided.
 */
public class AddUsersRequest extends Request {

    /**
     * Represents the users to be added to the group and all of their individial Roles. Important to note that this is
     * what you want the add to the group, not what you want the group to look like when you are done.
     */
    private Set<GroupMember> users;

    public Set<GroupMember> getUsers() {
        return users;
    }

    public void setUsers(Set<GroupMember> users) {
        this.users = users;
    }

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    private String groupName;

    /** Default Constructor */
    public AddUsersRequest() {}

    public AddUsersRequest(Group group){
        users = group.getMembers();
        groupName = group.getGroupName();
    }

}

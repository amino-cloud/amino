package com._42six.amino.common.query.requests;

import com._42six.amino.common.Group;

/**
 * Adds users to a group.  For each user, their individual roles should be provided.
 */
public class AddUsersRequest extends Request {

    /**
     * Represents the users to be added to the group and all of their individial Roles. Important to note that this is
     * what you want the add to the group, not what you want the group to look like when you are done.
     */
    public Group group;

    /** Default Constructor */
    public AddUsersRequest() {}

    public AddUsersRequest(Group group){
        this.group = group;
    }

    public Group getGroup(){
        return this.group;
    }

    public void setGroup(Group group){
        this.group = group;
    }
}

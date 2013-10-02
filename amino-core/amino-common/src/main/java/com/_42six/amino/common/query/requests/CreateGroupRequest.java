package com._42six.amino.common.query.requests;

import com._42six.amino.common.Group;

public class CreateGroupRequest extends Request {
    public Group group;

    public Group getGroup(){
        return this.group;
    }

    public void setGroup(Group group){
        this.group = group;
    }
}

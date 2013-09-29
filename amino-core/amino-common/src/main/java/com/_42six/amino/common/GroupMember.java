package com._42six.amino.common;

import java.util.Set;

/**
 * A member of a com._42six.amino.common.Group
 */
public class GroupMember {
    private String name;
    private Set<Group.GroupRole> roles;

    public GroupMember() {
        // EMPTY  Constructor
    }

    /**
     * Creates a GroupMember for a Group
     * @param name  The name of the group member
     * @param roles Any roles that the member might have
     */
    public GroupMember(String name, Set<Group.GroupRole> roles) {
        this.name = name;
        this.roles = roles;
    }

    // Getters and Setters

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Set<Group.GroupRole> getRoles() {
        return this.roles;
    }

    public void setRole(Set<Group.GroupRole> roles) {
        this.roles = roles;
    }

}
package com._42six.amino.common;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import java.util.HashSet;
import java.util.Set;

/**
 * Used for managing sharing of Hypotheses
 */
public class Group {
	
	public enum GroupRole {
		ADMIN("admin"),              // Can Add & Delete users; Can delete group
		CONTRIBUTOR("contributor"),  // Can share their Inquiries for the group to view
		VIEWER("viewer");            // Can view Inquiries shared by the group.
		
	    /**
	     * @param text
	     */
	    private GroupRole(final String text) {
	        this.text = text;
	    }

	    private final String text;

	    /* (non-Javadoc)
	     * @see java.lang.Enum#toString()
	     */
	    @Override
	    public String toString() {
	        return text;
	    }

        public static GroupRole fromString(String roleString){
            try{
                return valueOf(roleString.toUpperCase());
            } catch (Exception ex){
                throw new IllegalArgumentException(ex);
            }
        }
	}
	
	private String groupName;
	private Set<GroupMember> members;
	private String createdBy;
    private long dateCreated;

    // Getters and Setters
    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    public long getDateCreated() {
        return dateCreated;
    }

    public void setDateCreated(long dateCreated) {
        this.dateCreated = dateCreated;
    }

	public String getGroupName(){ return this.groupName; }
	public void setGroupName(String name){ this.groupName = name; }
	
	public Set<GroupMember> getMembers(){ return this.members; }
	public void setMembers(Set<GroupMember> members){ this.members = members;}
	
	public Group() {
		this.members = new HashSet<GroupMember>();
	}

    public Group(String name){
        this.groupName = name;
        this.members = new HashSet<GroupMember>();
    }

    public Group(String name, String createdBy){
        this.groupName = name;
        this.createdBy = createdBy;
        this.members = new HashSet<GroupMember>();
    }

    public Group(String name, String createdBy, long dateCreated){
        this.groupName = name;
        this.createdBy = createdBy;
        this.dateCreated = dateCreated;
        this.members = new HashSet<GroupMember>();
    }

    public Group(String name, String createdBy, long dateCreated, Set<GroupMember> members){
        this.groupName = name;
        this.createdBy = createdBy;
        this.dateCreated = dateCreated;
        this.members = members;
    }

    @Override
    public boolean equals(final Object obj){
        if (obj == null) {
            return false;
        }

        if (!this.getClass().equals(obj.getClass())) {
            return false;
        }

        Group group = (Group) obj;

        return new EqualsBuilder().append(this.groupName, group.groupName)
            .append(this.createdBy, group.createdBy)
            .append(this.dateCreated, group.dateCreated)
            .append(this.members, group.members)
            .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37).append(this.groupName)
            .append(this.createdBy)
            .append(this.dateCreated)
            .append(this.members)
            .toHashCode();
    }

}

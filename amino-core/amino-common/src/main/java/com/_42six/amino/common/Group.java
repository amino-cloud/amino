package com._42six.amino.common;

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
		// EMPTY Constructor
	}

}

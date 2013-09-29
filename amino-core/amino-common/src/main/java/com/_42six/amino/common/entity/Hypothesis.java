package com._42six.amino.common.entity;

import com.google.gson.Gson;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * A hypothesis is an entity representing the necessary information required to make a query.
 *
 * @author Amino Team
 */
public class Hypothesis implements Comparable<Hypothesis> {
	/**
	 * Sort Hypothesis bases on their name
	 */
	public int compareTo(Hypothesis other) {
		return this.name.compareTo(other.name);
	}

	/**
	 * Serializes this class to a JSON representation.
	 *
	 * @return JSON string representing the class
	 */
	public String toJson() {
		Gson gson = new Gson();
		return gson.toJson(this);
	}

	/**
	 * Create a Hypothesis by deserializing JSON
	 *
	 * @param json JSON representing the Hypothesis
	 * @return Hypothesis representing JSON string
	 */
	public static Hypothesis fromJson(String json) {
		Gson gson = new Gson();
		return gson.fromJson(json, Hypothesis.class);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}

		if (!this.getClass().equals(obj.getClass())) {
			return false;
		}

		Hypothesis hypothesis = (Hypothesis) obj;

		return new EqualsBuilder().append(this.owner, hypothesis.owner)
				.append(this.name, hypothesis.name)
				.append(this.bucketid, hypothesis.bucketid)
				.append(this.datasourceid, hypothesis.datasourceid)
				.append(this.justification, hypothesis.justification)
				.append(this.bucketValue, hypothesis.bucketValue)
				.append(this.btVisibility, hypothesis.btVisibility)
				.append(this.visibility, hypothesis.visibility)
				.append(this.hypothesisFeatures, hypothesis.hypothesisFeatures)
				.isEquals();
	}

	@Override
	public int hashCode() {
		// TODO Not including the id, see if this is a problem...
		return new HashCodeBuilder(17, 37).append(this.owner)
				.append(this.name)
				.append(this.bucketid)
				.append(this.datasourceid)
				.append(this.justification)
				.append(this.bucketValue)
				.append(this.btVisibility)
				.append(this.visibility)
				.append(this.hypothesisFeatures)
				.toHashCode();
	}

	/**
	 * The person or group that has ownership of this hypothesis
	 */
	public String owner;

	/**
	 * The UUID representing the hypothesis
	 */
	public String id;

	/**
	 * The human readable name of the hypothesis
	 */
	public String name;

	/**
	 * Which bucket the Hypothesis relates to
	 */
	public String bucketid;

	/**
	 * A list of groups that can edit this hypothesis
	 */
	public List<String> canEdit;

	/**
	 * A list of groups that can view this hypothesis
	 */
	public List<String> canView;

	/**
	 * The datasource that the data comes from
	 */
	public String datasourceid;

	/**
	 * The justification as to why this Hypothesis was created
	 */
	public String justification;

	/**
	 * The value that was used to generate the Hypothesis
	 */
	public String bucketValue;

	/**
	 * The Big Table ColumnVisibility string
	 */
	public String btVisibility;

	/**
	 * The human readable visibility
	 */
	public String visibility;

	/**
	 * All of the features that relate to this Hypothesis
	 */
	public Set<HypothesisFeature> hypothesisFeatures;

	/**
	 * The time that the Hypothesis was created
	 */
	public Long created;

	/**
	 * The time that the Hypothesis was updated
	 */
	public Long updated;

	/**
	 * The time that the Hypothesis was executed
	 */
	public Long executed;

	/**
	 * A sorted set of QueryResult ID's, sorted by time
	 */
	public SortedSet<String> queries = new TreeSet<String>();
}

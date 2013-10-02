package com._42six.amino.common.query.requests;

import com._42six.amino.common.entity.Hypothesis;

import java.util.Collection;
import java.util.Set;

/***
 * Configuration object for intersecting bucket values with either existing or given Hypothesis objects
 */
public class ByValuesRequest extends Request {
    /** The datasource the bucket values are associated with  */
	private String datasourceId;

    /** The bucket the bucket values are associated with */
	private String bucketId;

    /** The values to intersect the Hypotheses with */
	private Set<String> bucketValues;

    /** OPTIONAL - If provided then only searches these hypotheses */
	private Collection<Hypothesis> hypotheses;

    /** Configuration object for intersecting bucket values with either existing or given Hypothesis objects */
	public ByValuesRequest() {
		// Empty
	}

	/**
	 * @return the datasourceId
	 */
	public String getDatasourceId() {
		return datasourceId;
	}

	/**
	 * @param datasourceId the datasourceId to set
	 */
	public void setDatasourceId(String datasourceId) {
		this.datasourceId = datasourceId;
	}

	/**
	 * @return the bucketId
	 */
	public String getBucketId() {
		return bucketId;
	}

	/**
	 * @param bucketId the bucketId to set
	 */
	public void setBucketId(String bucketId) {
		this.bucketId = bucketId;
	}

	/**
	 * @return the bucketValues
	 */
	public Set<String> getBucketValues() {
		return bucketValues;
	}

	/**
	 * @param bucketValues the bucketValues to set
	 */
	public void setBucketValues(Set<String> bucketValues) {
		this.bucketValues = bucketValues;
	}

	/**
	 * @return the hypotheses
	 */
	public Collection<Hypothesis> getHypotheses() {
		return hypotheses;
	}

	/**
	 * @param hypotheses the hypotheses to set
	 */
	public void setHypotheses(Collection<Hypothesis> hypotheses) {
		this.hypotheses = hypotheses;
	}

	@Override
	public void verify() throws IllegalStateException{
		super.verify();
		if (datasourceId == null || datasourceId.isEmpty()) { throw new IllegalStateException("datasourceId is not set properly"); }
		if (bucketId == null || bucketId.isEmpty()) { throw new IllegalStateException("bucketId is not set properly"); }
		if (bucketValues == null || bucketValues.isEmpty()) { throw new IllegalStateException("bucketValues is not set properly"); }
		// if (hypotheses != null || hypotheses.isEmpty()) { throw new IllegalStateException("hypotheses is not set properly"); }
	}

}

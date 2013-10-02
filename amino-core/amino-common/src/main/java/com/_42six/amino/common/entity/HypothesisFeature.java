package com._42six.amino.common.entity;

import com.google.gson.Gson;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * A HypothesisFeature represents a configured version of a FeatureMetadata. The
 * featureMetadataId field is a reference to a FeatureMetadata object, and the type
 * property of the HypothesisFeature should mirror that FeatureMetadata's type.
 * The rest of the fields represent possible configured values, although which values
 * are set will depend on the type of feature.
 *
 * @author Amino Team
 * @see com._42six.amino.common.FeatureMetadata
 */
public class HypothesisFeature {

	public HypothesisFeature() {
		// EMPTY
	}

	public HypothesisFeature(HypothesisFeature feature) {
		this.id = feature.id;
		this.featureMetadataId = feature.featureMetadataId;
		this.type = feature.type;
		this.operator = feature.operator;
		this.value = feature.value;
		this.min = feature.min;
		this.max = feature.max;
		this.dateTimeType = feature.dateTimeType;
		this.relativeDateTimeRange = feature.relativeDateTimeRange;
		this.timestampFrom = feature.timestampFrom;
		this.timestampTo = feature.timestampTo;
		this.visibility = feature.visibility;
		this.btVisibility = feature.btVisibility;
		this.count = feature.count;
		this.uniqueness = feature.uniqueness;
		this.include = feature.include;
	}

	public String toAuditString() {
		String auditString;
		if(type.compareTo("NOMINAL") == 0 || type.compareTo("RESTRICTION") == 0) {
			auditString = this.value;
		} else if(type.compareTo("RATIO") == 0 || type.compareTo("INTERVAL") == 0) { // inList
			auditString = "min: " + min + "max: " + max;
		} else if(type.compareTo("DATE") == 0 || type.compareTo("DATEHOUR") == 0) {
            auditString = "dateFrom: " + timestampFrom + " dateTo:" + timestampTo;    
        }else {
			throw new RuntimeException("Unsupported feature type while building audit string");
		}
		return auditString;
	}

	public String toJson() {
		Gson gson = new Gson();
		return gson.toJson(this);
	}

	public static HypothesisFeature fromJson(String json) {
		Gson gson = new Gson();
		return gson.fromJson(json, HypothesisFeature.class);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}

		if (!this.getClass().equals(obj.getClass())) {
			return false;
		}

		HypothesisFeature feature = (HypothesisFeature) obj;

		return new EqualsBuilder().append(this.featureMetadataId, feature.featureMetadataId)
				.append(this.type, feature.type)
				.append(this.operator, feature.operator)
				.append(this.value, feature.value)
				.append(this.min, feature.min)
				.append(this.max, feature.max)
				.append(this.dateTimeType, feature.dateTimeType)
				.append(this.relativeDateTimeRange, feature.relativeDateTimeRange)
				.append(this.timestampFrom, feature.timestampFrom)
				.append(this.timestampTo, feature.timestampTo)
				.append(this.include, feature.include)
				.append(this.visibility, feature.visibility)
				.append(this.btVisibility, feature.btVisibility)
				.append(this.count, feature.count)
				.append(this.uniqueness, feature.uniqueness)
				.isEquals();
	}

	@Override
	public int hashCode() {
		// TODO Not including the id, see if this is a problem...
		return new HashCodeBuilder(17, 37)
				.append(this.featureMetadataId)
				.append(this.type)
				.append(this.operator)
				.append(this.value)
				.append(this.min)
				.append(this.max)
				.append(this.dateTimeType)
				.append(this.relativeDateTimeRange)
				.append(this.timestampFrom)
				.append(this.timestampTo)
				.append(this.include)
				.append(this.visibility)
				.append(this.btVisibility)
				.append(this.count)
				.append(this.uniqueness)
				.toHashCode();
	}

	public String id;
	public String featureMetadataId;
	public String type;
	public String operator;
	public String value;
	public double min;
	public double max;
	public String dateTimeType;
	public String relativeDateTimeRange;
	public long timestampFrom;
	public long timestampTo;

	/**
	 * The visibility of the Hypothesis
	 */
	public String visibility;

	/**
	 * The Accumulo visibility
	 */
	public transient String btVisibility = null;

	/**
	 * The number of bucket matches for this feature
	 */
	public int count;

	/**
	 * The measure of uniqueness for this feature in the bucket
	 */
	public double uniqueness;
	public boolean include;
}

package com._42six.amino.bitmap;

import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BitLookupKey implements WritableComparable {

    private int featureIndex;
    private String featureFact;
    private String visibility;
    private int salt;

    public int getFeatureIndex() {
        return featureIndex;
    }

    public void setFeatureIndex(int featureIndex) {
        this.featureIndex = featureIndex;
    }

    public String getFeatureFact() {
        return featureFact;
    }

    public void setFeatureFact(String featureFact) {
        this.featureFact = featureFact;
    }

    public int getSalt(){
        return salt;
    }

    public void setSalt(int salt){
        this.salt = salt;
    }

    public String getVisibility() {
        return visibility;
    }

    public void setVisibility(String visibility) {
        this.visibility = visibility;
    }

    public BitLookupKey() {
        // EMPTY
    }

    public BitLookupKey(int featureIndex, String featureFact, String visibility) {
        this.featureIndex = featureIndex;
        this.featureFact = featureFact;
        this.visibility = visibility;
    }

    public BitLookupKey(int featureIndex, String featureFact, String visibility, int salt) {
        this(featureIndex, featureFact, visibility);
        this.salt = salt;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) { return false; }
        if (o == this) { return true; }
        if (o.getClass() != getClass()) { return false; }

        BitLookupKey other = (BitLookupKey) o;
        return new EqualsBuilder()
                .append(featureFact, other.featureFact)
                .append(featureIndex, other.featureIndex)
                .append(salt, other.salt)
                .append(visibility, other.visibility)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .append(featureFact)
                .append(featureIndex)
                .append(salt)
                .append(visibility)
                .toHashCode();
    }

    public int compareTo(Object o) {
        BitLookupKey other = (BitLookupKey) o;
        return new CompareToBuilder()
                .append(Integer.toString(featureIndex), Integer.toString(other.featureIndex))
                .append(featureFact, other.featureFact)
                .append(salt, other.salt)
                .append(visibility, other.visibility)
                .toComparison();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("featureFact", featureFact)
                .append("featureIndex", featureIndex)
                .append("salt", salt)
                .append("visibility", visibility)
                .toString();
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(featureFact);
        dataOutput.writeInt(featureIndex);
        dataOutput.writeInt(salt);
        dataOutput.writeUTF(visibility);
    }

    public void readFields(DataInput dataInput) throws IOException {
        featureFact = dataInput.readUTF();
        featureIndex = dataInput.readInt();
        salt = dataInput.readInt();
        visibility = dataInput.readUTF();
    }
}

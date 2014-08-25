package com._42six.amino.common;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.hash.Hash;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Feature implements WritableComparable<Feature> {

    private static final String DEFAULT_NAMESPACE = "Public";
    private static final String DEFAULT_DESCRIPTION = "No description provided by developer.";
    private String name;
    private String description;
    private int hashCode = -1;
    private String namespace;

    public Feature(String name, String description, String namespace, int hashCode) {
        this.namespace = namespace;
        this.name = name;
        this.description = description;
        this.hashCode = hashCode;
    }
    
    public Feature(String name, String description, String namespace) {
        this.namespace = namespace;
        this.name = name;
        this.description = description;
        //this.hashCode = BitmapIndex.getFeatureNameIndex(name);
        Hash hasher = Hash.getInstance(Hash.MURMUR_HASH);
        this.hashCode = Math.abs(hasher.hash(name.getBytes()));
    }

    public Feature(String name, String description) {
        this(name, description, DEFAULT_NAMESPACE);
    }

    public Feature(String name) {
        this(name, DEFAULT_DESCRIPTION, DEFAULT_NAMESPACE);
    }

    public Feature(int hashCode) {
        this.hashCode = hashCode;
    }

    public Feature(Feature other) {
        this.namespace = other.namespace;
        this.description = other.description;
        this.name = other.name;
        this.hashCode = other.hashCode;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, Integer.toString(hashCode));
        Text.writeString(out, name);
        Text.writeString(out, description);
        Text.writeString(out, namespace);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        hashCode = Integer.parseInt(Text.readString(in));
        name = Text.readString(in);
        description = Text.readString(in);
        namespace = Text.readString(in);
    }

    @Override
    public int compareTo(Feature other) {
        return Integer.valueOf(this.hashCode()).compareTo(other.hashCode());
    }

    @Override
    public int hashCode() {
        return this.hashCode;
    }

    public String getName() {
        return this.name;
    }

    public String getDescription() {
        return this.description;
    }

    public String getNamespace() {
        return namespace;
    }
    
    public void setHashCode(int hashCode) {
    	this.hashCode = hashCode;
    }
}

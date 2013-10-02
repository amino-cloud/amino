package com._42six.amino.common;

import com.google.gson.Gson;

import java.io.IOException;
import java.util.Set;

public class DomainMetadata extends Metadata{

	public Set<String> datasources;
	public String name;
	public String description;

	public DomainMetadata(){
		// EMPTY
	}

	public DomainMetadata(String id, String name, String description, Set<String> ds){
        this.id = id;
		this.name = name;
		this.description = description;
		this.datasources = ds;
	}

	public static DomainMetadata fromJson(String json) {
		Gson gson = new Gson();
		return gson.fromJson(json, DomainMetadata.class);
	}

	public String toJson() {
		Gson gson = new Gson();
		return gson.toJson(this);
	}

    @Override
    /**
     * Combines with another DomainMetadata
     * @param with The DomainMetadata to combine with
     */
    public void combine(Metadata with) throws IOException {

        // Make sure with is of the right type
        if(with == null || with == this || !(with instanceof DomainMetadata)){
            return;
        }
        DomainMetadata that = (DomainMetadata) with;

        if(this.id.compareTo(that.id) != 0){
            throw new IOException("Can not combine domains with different IDs.  Actual: " + that.id + " Expected: " + this.id);
        }

        this.datasources.addAll(that.datasources);

        // TODO Combine visibility labels ??

    }

}

package com._42six.amino.common.accumulo;

import com._42six.amino.common.FeatureMetadata;
import com._42six.amino.common.bigtable.TableConstants;
import com.google.gson.Gson;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;


public class BtFeatureMetadata extends FeatureMetadata implements BtMetadata {
    private static final Gson gson = new Gson();

    private static void putIfNotNull(Mutation mutation, Text cf, ColumnVisibility cv, String value){
        if(value != null){
            mutation.put(cf, TableConstants.EMPTY_FIELD, cv, new Value(value.getBytes()));
        }
    }

    /**
     * Creates a Accumulo Mutaion
     *
     * @return {@link org.apache.accumulo.core.data.Mutation} for inserting an object into the metadata table
     */
    @Override
    public Mutation createMutation() {
        final Mutation mutation = new Mutation(TableConstants.FEATURE_PREFIX + this.id);
        final ColumnVisibility cv = new ColumnVisibility(this.btVisibility.getBytes());

        putIfNotNull(mutation, TableConstants.API_FIELD, cv, this.api_version);
        putIfNotNull(mutation, TableConstants.DATASOURCEIDS_FIELD, cv, gson.toJson(this.datasources));
        putIfNotNull(mutation, TableConstants.DESCRIPTION_FIELD, cv, this.description);
        putIfNotNull(mutation, TableConstants.JOB_FIELD, cv, this.job_version);
        putIfNotNull(mutation, TableConstants.NAME_FIELD, cv, this.name);
        putIfNotNull(mutation, TableConstants.NAMESPACE_FIELD, cv, this.namespace);
        putIfNotNull(mutation, TableConstants.TYPE_FIELD, cv, this.type);
        putIfNotNull(mutation, TableConstants.VISIBILITY_FIELD, cv, this.visibility);
        mutation.put(TableConstants.JSON_FIELD, TableConstants.EMPTY_FIELD, cv, new Value(this.toJson().getBytes()));

        return mutation;
    }
}

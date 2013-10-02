package com._42six.amino.common.accumulo;

import com._42six.amino.common.DatasourceMetadata;
import com._42six.amino.common.bigtable.TableConstants;
import com.google.gson.Gson;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;


public class BtDatasourceMetadata  extends DatasourceMetadata implements BtMetadata {
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
        final Mutation mutation = new Mutation(TableConstants.DATASOURCE_PREFIX + this.id);

        mutation.put(TableConstants.NAME_FIELD, TableConstants.EMPTY_FIELD, new Value(this.name.getBytes()));
        mutation.put(TableConstants.DESCRIPTION_FIELD, TableConstants.EMPTY_FIELD, new Value(this.description.getBytes()));
        mutation.put(TableConstants.JSON_FIELD, TableConstants.EMPTY_FIELD, new Value(this.toJson().getBytes()));
        mutation.put(TableConstants.BUCKETID_FIELD, TableConstants.EMPTY_FIELD, new Value(gson.toJson(this.bucketIds).getBytes()));
        mutation.put(TableConstants.FEATUREIDS_FIELD, TableConstants.EMPTY_FIELD, new Value(gson.toJson(this.featureIds).getBytes()));

        return mutation;
    }
}

package com._42six.amino.common.accumulo;

import com._42six.amino.common.DomainMetadata;
import com._42six.amino.common.bigtable.TableConstants;
import com.google.gson.Gson;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;


public class BtDomainMetadata extends DomainMetadata implements BtMetadata {
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
        final Mutation mutation = new Mutation(TableConstants.DOMAIN_PREFIX + this.id);
        mutation.put(TableConstants.DESCRIPTION_FIELD, TableConstants.EMPTY_FIELD, new Value(this.description.getBytes()));
        mutation.put(TableConstants.NAME_FIELD, TableConstants.EMPTY_FIELD, new Value(this.name.getBytes()));
        mutation.put(TableConstants.DATASOURCEIDS_FIELD, TableConstants.EMPTY_FIELD, new Value(gson.toJson(this.datasources).getBytes()));
        mutation.put(TableConstants.JSON_FIELD, TableConstants.EMPTY_FIELD, new Value(this.toJson().getBytes()));

        return mutation;
    }
}

package com._42six.amino.common.accumulo;

import com._42six.amino.common.BucketMetadata;
import com._42six.amino.common.bigtable.TableConstants;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;


public class BtBucketMetadata extends BucketMetadata implements BtMetadata {

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
        final Mutation mutation = new Mutation(TableConstants.BUCKET_PREFIX + this.id);
        final ColumnVisibility cv = new ColumnVisibility(this.btVisibility.getBytes());

        mutation.put(TableConstants.NAME_FIELD, TableConstants.EMPTY_FIELD, cv, new Value(this.name.getBytes()));
        mutation.put(TableConstants.DISPLAYNAME_FIELD, TableConstants.EMPTY_FIELD,  cv, new Value(this.displayName.getBytes()));
        mutation.put(TableConstants.VISIBILITY_FIELD, TableConstants.EMPTY_FIELD, cv, new Value(this.visibility.getBytes()));
        mutation.put(TableConstants.TIMESTAMP_FIELD, TableConstants.EMPTY_FIELD, cv, new Value(Long.toString(this.timestamp).getBytes()));
        mutation.put(TableConstants.JSON_FIELD, TableConstants.EMPTY_FIELD, cv, new Value(this.toJson().getBytes()));

        return mutation;
    }
}

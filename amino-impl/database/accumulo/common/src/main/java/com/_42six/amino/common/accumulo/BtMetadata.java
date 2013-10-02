package com._42six.amino.common.accumulo;

import com._42six.amino.common.Metadata;
import org.apache.accumulo.core.data.Mutation;

import java.io.IOException;

public interface BtMetadata {

    /**
     * Creates a Accumulo Mutaion
     * @return {@link org.apache.accumulo.core.data.Mutation} for inserting an object into the metadata table
     */
    public Mutation createMutation();

    public void combine(Metadata with) throws IOException;
}

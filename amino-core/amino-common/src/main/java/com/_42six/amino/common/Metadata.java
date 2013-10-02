package com._42six.amino.common;

import java.io.IOException;

/**
 * Base class for all of the types of metadata for the metadata table
 */
public abstract class Metadata {
    public String id;

    /**
     * Combines two Metadata objects together.  They must be of the same type
     * @param with The metadata object to combine with
     */
    public abstract void combine(Metadata with) throws IOException;
}

package com._42six.amino.common;

import com.google.gson.Gson;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;

/**
 * Metadata about features to be serialized/deserialized from Accumulo.
 */
public class DateFeatureMetadata extends FeatureMetadata {

    public DateFeatureMetadata(FeatureMetadata that) {
        super(that);
    }

    /** Minimum value (for interval features) */
    public Hashtable<String,Long> minDate;
    
    /** Maximum value (for interval features) */
    public Hashtable<String,Long> maxDate;
    
    @Override
    public void combine(Metadata with) throws IOException {
        super.combine(with);
        
        if (with == null || with == this || !(with instanceof DateFeatureMetadata)) {
            return;
        }
        DateFeatureMetadata that = (DateFeatureMetadata)with;

        if (that.minDate != null) {
            if (this.minDate != null) {
                this.minDate.putAll(that.minDate);
            } else {
                this.minDate = new Hashtable<String, Long>(that.minDate);
            }
        }

        if (that.maxDate != null) {
            if (this.maxDate != null) {
                this.maxDate.putAll(that.maxDate);
            } else {
                this.maxDate = new Hashtable<String, Long>(that.maxDate);
            }
        }

    }
}

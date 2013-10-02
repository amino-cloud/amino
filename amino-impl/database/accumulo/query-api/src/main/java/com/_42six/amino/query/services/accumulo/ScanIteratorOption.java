package com._42six.amino.query.services.accumulo;

import java.util.AbstractMap;

/**
 * A simple Key/Value pair for setting Iterator options
 */
public class ScanIteratorOption extends AbstractMap.SimpleEntry<String, String>{
	String key;
	String value;

    public ScanIteratorOption(String key, String value) {
        super(key, value);
    }
}

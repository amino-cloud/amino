package com._42six.amino.common;

public class FeatureFactFactory {

	public static FeatureFact createInstance(FeatureFactType type) {
        switch (type) {
            case NOMINAL:
                return new NominalFeatureFact();
            case ORDINAL:
                return new OrdinalFeatureFact();
            case RATIO:
                return new RatioFeatureFact();
            case INTERVAL:
                return new IntervalFeatureFact();
            case DATE:
                return new DateFeatureFact();
            case DATEHOUR:
                return new DateHourFeatureFact();
            case POINT:
                return new PointFeatureFact();
            case POLYGON:
                return new PolygonFeatureFact();
            default:
                return null;
        }
    }
    
}

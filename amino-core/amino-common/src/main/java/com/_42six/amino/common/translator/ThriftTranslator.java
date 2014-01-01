package com._42six.amino.common.translator;

import com._42six.amino.common.*;
import com._42six.amino.common.entity.Hypothesis;
import com._42six.amino.common.entity.HypothesisFeature;
import com._42six.amino.common.query.requests.AddUsersRequest;
import com._42six.amino.common.query.requests.CreateGroupRequest;
import com._42six.amino.common.thrift.*;

import java.util.*;

/**
 * Class for translating internal objects to/from thrift
 */
public class ThriftTranslator {
    public static THypothesisFeature toTHypothesisFeature(HypothesisFeature feature){
        return new THypothesisFeature()
                .setId(feature.id)
                .setFeatureMetadataId(feature.featureMetadataId)
                .setType(feature.type)
                .setOperator(feature.operator)
                .setValue(feature.value)
                .setMin(feature.min)
                .setMax(feature.max)
                .setDateTimeType(feature.dateTimeType)
                .setRelativeDateTimeRange(feature.relativeDateTimeRange)
                .setTimestampFrom(feature.timestampFrom)
                .setTimestampTo(feature.timestampTo)
                .setVisibility(feature.visibility)
                .setCount(feature.count)
                .setUniqueness(feature.uniqueness)
                .setToInclude(feature.include);
    }

    public static HypothesisFeature fromTHypothesisFeature(THypothesisFeature feature){
        final HypothesisFeature retVal = new HypothesisFeature();
        retVal.id = feature.id;
        retVal.featureMetadataId = feature.featureMetadataId;
        retVal.type = feature.type;
        retVal.operator = feature.operator;
        retVal.value = feature.value;
        retVal.min = feature.min;
        retVal.max = feature.max;
        retVal.dateTimeType = feature.dateTimeType;
        retVal.relativeDateTimeRange = feature.relativeDateTimeRange;
        retVal.timestampFrom = feature.timestampFrom;
        retVal.timestampTo = feature.timestampTo;
        retVal.visibility = feature.visibility;
        retVal.count = feature.count;
        retVal.uniqueness = feature.uniqueness;
        retVal.include = feature.toInclude;

        return  retVal;
    }


    public static THypothesis toTHypothesis(Hypothesis h){
        final Set<THypothesisFeature> features = new HashSet<THypothesisFeature>(h.hypothesisFeatures.size());
        for(HypothesisFeature feature : h.hypothesisFeatures){
            features.add(toTHypothesisFeature(feature));
        }

        return new THypothesis()
                .setOwner(h.owner)
                .setId(h.id)
                .setName(h.name)
                .setBucketid(h.bucketid)
                .setCanEdit(h.canEdit)
                .setCanView(h.canView)
                .setDatasourceid(h.datasourceid)
                .setJustification(h.justification)
                .setBucketValue(h.bucketValue)
                .setBtVisibility(h.btVisibility)
                .setHypothesisFeatures(features)
                .setCreated(h.created)
                .setUpdated(h.updated)
                .setExecuted(h.executed)
                .setQueries(h.queries);
    }

    public static Hypothesis fromTHypothesis(THypothesis hypothesis){
        final Set<HypothesisFeature> features = new HashSet<HypothesisFeature>(hypothesis.getHypothesisFeaturesSize());
        for(THypothesisFeature feature : hypothesis.hypothesisFeatures){
            features.add(fromTHypothesisFeature(feature));
        }

        final Hypothesis retVal = new Hypothesis();
        retVal.owner = hypothesis.owner;
        retVal.id = hypothesis.id;
        retVal.name = hypothesis.name;
        retVal.bucketid = hypothesis.bucketid;
        retVal.canEdit = hypothesis.canEdit;
        retVal.canView = hypothesis.canView;
        retVal.datasourceid = hypothesis.datasourceid;
        retVal.justification = hypothesis.justification;
        retVal.bucketValue = hypothesis.bucketValue;
        retVal.btVisibility = hypothesis.btVisibility;
        retVal.hypothesisFeatures = features;
        retVal.created = hypothesis.created;
        retVal.updated = hypothesis.updated;
        retVal.executed = hypothesis.created;
        retVal.queries = new TreeSet<String>(hypothesis.queries);

        return  retVal;
    }

    public static Set<Group.GroupRole> fromTGroupRoles(Set<TGroupRole> roles){
        final Set<Group.GroupRole> retVal = new HashSet<Group.GroupRole>(roles.size());
        for(TGroupRole role : roles){
            switch(role){
                case VIEWER:
                    retVal.add(Group.GroupRole.VIEWER); break;
                case ADMIN:
                    retVal.add(Group.GroupRole.ADMIN); break;
                case CONTRIBUTOR:
                    retVal.add(Group.GroupRole.CONTRIBUTOR); break;
                default:
                    throw new RuntimeException("Translator can not convert role " + role);
            }
        }
        return retVal;
    }

    public static Set<TGroupRole> toTGroupRoles(Set<Group.GroupRole> roles){
        final Set<TGroupRole> retVal = new HashSet<TGroupRole>(roles.size());
        for(Group.GroupRole role : roles){
            switch(role){
                case VIEWER:
                    retVal.add(TGroupRole.VIEWER); break;
                case ADMIN:
                    retVal.add(TGroupRole.ADMIN); break;
                case CONTRIBUTOR:
                    retVal.add(TGroupRole.CONTRIBUTOR); break;
                default:
                    throw new RuntimeException("Translator can not convert role " + role);
            }
        }
        return retVal;
    }

    public static GroupMember fromTGroupMember(TGroupMember member){
        return new GroupMember(member.getName(), fromTGroupRoles(member.getRoles()));
    }

    public static Set<GroupMember> fromTGroupMembers(Set<TGroupMember> members){
        final Set<GroupMember> retVal = new HashSet<GroupMember>(members.size());
        for(TGroupMember member : members){
            retVal.add(fromTGroupMember(member));
        }
        return retVal;
    }

    public static TGroupMember toTGroupMember(GroupMember member){
        return new TGroupMember(member.getName(), toTGroupRoles(member.getRoles()));
    }

    public static Group fromTGroup(TGroup group){
        return new Group(group.groupName, group.createdBy, group.dateCreated, fromTGroupMembers(group.members));
    }

    public static TGroup toTGroup(Group group){
        final Set<TGroupMember> members = new HashSet<TGroupMember>(group.getMembers().size());
        for(GroupMember m : group.getMembers()){
            members.add(toTGroupMember(m));
        }
        return new TGroup(group.getGroupName(), group.getCreatedBy(), group.getDateCreated(), members);
    }

    public static AddUsersRequest fromTAddUsersRequest(TAddUsersRequest request){
        final AddUsersRequest retVal = new AddUsersRequest();
        retVal.setRequestor(request.getRequester());
        retVal.setSecurityTokens(request.getVisibilities().toArray(new String[request.getVisibilitiesSize()]));
        retVal.setUsers(fromTGroupMembers(request.getUsersToAdd()));
        retVal.setGroupName(request.getGroupName());
        return retVal;
    }

    public static CreateGroupRequest fromTCreateGroupRequest(TCreateGroupRequest request){
        final CreateGroupRequest retVal =  new CreateGroupRequest();
        retVal.setGroup(fromTGroup(request.getGroup()));
        retVal.setRequestor(request.getRequester());
        retVal.setSecurityTokens(request.getVisibilities().toArray(new String[request.getVisibilitiesSize()]));
        return retVal;
    }

    public static TDatasourceMetadata toTDatasourceMetadata(DatasourceMetadata metadata){
        return new TDatasourceMetadata(metadata.id, metadata.description, metadata.name, metadata.featureIds, metadata.bucketIds);
    }

    public static DatasourceMetadata fromTDatasourceMetadata(TDatasourceMetadata metadata){
        return new DatasourceMetadata(metadata.description, metadata.id, metadata.name, metadata.bucketIds, metadata.featureIds);
    }

    public static TBucketMetadata toTBucketMetadata(BucketMetadata metadata){
        return new TBucketMetadata(metadata.id, metadata.name, metadata.displayName, metadata.visibility, metadata.btVisibility,
                metadata.domainIdName, metadata.domainIdDescription, metadata.timestamp);
    }

    public static TFeatureMetadata toTFeatureMetadata(FeatureMetadata metadata){
        final Map<String, List<Map<String, Double>>> ratioBins = new HashMap<String, List<Map<String, Double>>>(metadata.ratioBins.size());
        final Map<String, List<String>> topN = new HashMap<String, List<String>>(metadata.topN.size());

        for(Map.Entry<String, ArrayList<Hashtable<String, Double>>> entry : metadata.ratioBins.entrySet()){
            ratioBins.put(entry.getKey(), new ArrayList<Map<String, Double>>(entry.getValue()));
        }

        for(Map.Entry<String, ArrayList<String>> entry : metadata.topN.entrySet()){
            topN.put(entry.getKey(), new ArrayList<String>(entry.getValue()));
        }

        final TFeatureMetadata retVal = new TFeatureMetadata()
                .setId(metadata.id)
                .setName(metadata.name)
                .setVisibility(metadata.visibility)
                .setBtVisibility(metadata.btVisibility)
                .setApi_version(metadata.api_version)
                .setJob_version(metadata.job_version)
                .setDescription(metadata.description)
                .setFmNamespace(metadata.namespace)
                .setType(metadata.type)
                .setDatasources(metadata.datasources)
                .setMin(metadata.min)
                .setMax(metadata.max)
                .setAllowedValues(metadata.allowedValues)
                .setFeatureFactCount(metadata.featureFactCount)
                .setBucketValueCount(metadata.bucketValueCount)
                .setAverages(metadata.averages)
                .setStandardDeviations(metadata.standardDeviations)
                .setRatioBins(ratioBins)
                .setTopN(topN);
        return retVal;
    }

}

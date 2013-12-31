package com._42six.amino.common.translator;

import com._42six.amino.common.Group;
import com._42six.amino.common.GroupMember;
import com._42six.amino.common.entity.Hypothesis;
import com._42six.amino.common.entity.HypothesisFeature;
import com._42six.amino.common.query.requests.AddUsersRequest;
import com._42six.amino.common.query.requests.CreateGroupRequest;
import com._42six.amino.common.thrift.*;

import java.util.HashSet;
import java.util.Set;

/**
 * Class for translating internal objects to/from thrift
 */
public class ThriftTranslator {
    public static THypothesisFeature toThriftHypothesisFeature(HypothesisFeature feature){
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

    public static THypothesis toThriftHypothesis(Hypothesis h){
        final Set<THypothesisFeature> features = new HashSet<THypothesisFeature>(h.hypothesisFeatures.size());
        for(HypothesisFeature feature : h.hypothesisFeatures){
            features.add(toThriftHypothesisFeature(feature));
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

}

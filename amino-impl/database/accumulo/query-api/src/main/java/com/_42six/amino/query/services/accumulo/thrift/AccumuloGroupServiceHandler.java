package com._42six.amino.query.services.accumulo.thrift;

import com._42six.amino.common.entity.Hypothesis;
import com._42six.amino.common.thrift.TAddUsersRequest;
import com._42six.amino.common.thrift.TCreateGroupRequest;
import com._42six.amino.common.thrift.TGroup;
import com._42six.amino.common.thrift.THypothesis;
import com._42six.amino.common.translator.ThriftTranslator;
import com._42six.amino.query.services.accumulo.AccumuloGroupService;
import com._42six.amino.query.thrift.services.ThriftGroupService;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.thrift.TException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class AccumuloGroupServiceHandler implements ThriftGroupService.Iface {

    private AccumuloGroupService groupService;

    public AccumuloGroupServiceHandler(AccumuloGroupService groupService){
        this.groupService = groupService;
    }

    @Override
    public boolean verifyGroupExists(String group, Set<String> visibilities) throws TException {
        try {
            return groupService.verifyGroupExists(group, visibilities.toArray(new String[visibilities.size()]));
        } catch (IOException e) {
            throw new TException(e);
        }
    }

    @Override
    public boolean verifyUserExists(String user, Set<String> visibilities) throws TException {
        try {
            return groupService.verifyUserExists(user, visibilities.toArray(new String[visibilities.size()]));
        } catch (IOException e) {
            throw new TException(e);
        }
    }

    @Override
    public void addToGroup(TAddUsersRequest request) throws TException {
        try {
            groupService.addToGroup(ThriftTranslator.fromTAddUsersRequest(request));
        } catch (Exception e) {
           throw new TException(e);
        }
    }

    @Override
    public void createGroup(TCreateGroupRequest request) throws TException {
        try {
            groupService.createGroup(ThriftTranslator.fromTCreateGroupRequest(request));
        } catch (Exception e) {
            throw new TException(e);
        }
    }

    @Override
    public Set<String> listGroups(String userId, Set<String> visibilities) throws TException {
        try {
            return groupService.listGroups(userId, visibilities.toArray(new String[visibilities.size()]));
        } catch (IOException e) {
            throw new TException(e);
        }
    }

    @Override
    public Set<String> getGroupsForUser(String userId, Set<String> visibilities) throws TException {
        try {
            return groupService.getGroupsForUser(userId, visibilities.toArray(new String[visibilities.size()]));
        } catch (IOException e) {
            throw new TException(e);
        }
    }

    @Override
    public void removeUserFromGroups(String requester, String userId, Set<String> groups, Set<String> visibilities) throws TException {
        final Authorizations auths = new Authorizations(visibilities.toArray(new String[visibilities.size()]));
        try {
            groupService.removeUserFromGroups(requester, userId, groups, auths);
        } catch (Exception e) {
            throw new TException(e);
        }
    }

    @Override
    public void removeUsersFromGroup(String requester, String group, Set<String> users, Set<String> visibilities) throws TException {
        final Authorizations auths = new Authorizations(visibilities.toArray(new String[visibilities.size()]));
        try {
            groupService.removeUsersFromGroup(requester, group, users, auths);
        } catch (Exception e) {
            throw new TException(e);
        }
    }

    @Override
    public List<THypothesis> getGroupHypothesesForUser(String userId, Set<String> visibilities, boolean userOwned) throws TException {
        try {
            final List<Hypothesis> hypotheses = groupService.getGroupHypothesesForUser(userId, visibilities.toArray(new String[visibilities.size()]), userOwned);
            final List<THypothesis> toReturn = new ArrayList<THypothesis>(hypotheses.size());
            for(Hypothesis h : hypotheses){
                toReturn.add(ThriftTranslator.toThriftHypothesis(h));
            }
            return toReturn;
        } catch (IOException e) {
            throw new TException(e);
        }
    }

    @Override
    public TGroup getGroup(String requester, String group, Set<String> visibilities) throws TException {
        try {
            return ThriftTranslator.toTGroup(groupService.getGroup(requester, group, visibilities.toArray(new String[visibilities.size()])));
        } catch (IOException e) {
            throw new TException(e);
        }
    }

}

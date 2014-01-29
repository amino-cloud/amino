package com._42six.amino.query.services.accumulo.thrift;

import com._42six.amino.query.thrift.services.ThriftGroupService;
import com.google.common.collect.Sets;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.util.Set;

/**
 * This is a simple example of a Thrift client for getting the groups that the testUser is in
 */
public class QueryClient {

    static ThriftGroupService.Client groupServiceClient;

    static final Set<String> VISIBILITIES = Sets.newHashSet("U");

    public static void main(String[] args){
        final TTransport transport = new TSocket("localhost", 9090);

        try {
            transport.open();
            final TProtocol protocol = new TBinaryProtocol(transport);
            groupServiceClient = new ThriftGroupService.Client(protocol);
            System.out.println(groupServiceClient.listGroups("testUser", VISIBILITIES));
        } catch (TTransportException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        } finally {
            transport.close();
        }
    }
}

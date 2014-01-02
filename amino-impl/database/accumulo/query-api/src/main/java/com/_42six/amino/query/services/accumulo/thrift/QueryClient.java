package com._42six.amino.query.services.accumulo.thrift;

import com._42six.amino.query.thrift.services.ThriftGroupService;
import com.google.common.collect.Lists;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.util.List;

/**
 * This is a simple example of a Client for
 */
public class QueryClient {

    static ThriftGroupService.Client groupServiceClient;

    static final List<String> VISIBILITIES = Lists.newArrayList("U");

    public static void main(String[] args){
        final TTransport transport = new TSocket("localhost", 9090);

        try {
            transport.open();
            final TProtocol protocol = new TBinaryProtocol(transport);
            groupServiceClient = new ThriftGroupService.Client(protocol);
        } catch (TTransportException e) {
            e.printStackTrace();
        } finally {
            transport.close();
        }
    }
}

package com._42six.amino.query.services.accumulo.thrift;

import com._42six.amino.query.services.accumulo.AccumuloGroupService;
import com._42six.amino.query.services.accumulo.AccumuloMetadataService;
import com._42six.amino.query.services.accumulo.AccumuloPersistenceService;
import com._42six.amino.query.services.accumulo.AccumuloQueryService;
import com._42six.amino.query.thrift.services.AminoThriftService;
import com._42six.amino.query.thrift.services.AminoThriftServiceHandler;
import org.apache.thrift.server.TServer;
// import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;

import java.io.IOException;

/**
 * Simple Thrift Server for accessing the query API
 */
public class QueryServer  {
    public static void main(String[] args) throws TTransportException, IOException {
        final String instanceName = "accumulo"; // TODO Change all of the hard coded values
        final String zooKeepers = "accumulo:2181";
        final String user = "amino";
        final String password = "pass";

        final AccumuloPersistenceService persistenceService = new AccumuloPersistenceService(instanceName, zooKeepers, user, password);
        final AccumuloGroupService groupService = new AccumuloGroupService(persistenceService);
        final AccumuloMetadataService metadataService = new AccumuloMetadataService(persistenceService);
        final AccumuloQueryService queryService = new AccumuloQueryService(persistenceService, metadataService);

        final AminoThriftService.Processor processor = new AminoThriftService.Processor(
                new AminoThriftServiceHandler(groupService, metadataService, queryService));

        final TServerTransport serverTransport = new TServerSocket(9090, 9999999);

        //final TServer server = new TSimpleServer(new TServer.Args(serverTransport).processor(processor));
        // Use this for multi-threaded server
        TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));

        System.out.println("Starting Amino Accumulo query server...");
        server.serve();
        System.out.println("Shutting down");
    }
}

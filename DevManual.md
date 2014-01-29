Quickstart
==========
Here are some instructions for getting Amino up and running with the Numbers example dataset

Step 0
------
Make sure that you have the pre-requisite technologies installed. This includes HDFS, Hadoop, ZooKeeper, and Accumulo.
Also, this assumes that you have at least Java 1.6 installed and Maven

Step 1
------
Clone the git repository.  For the rest of the instructions, assume that `$AMINO` points to the direcoty to which you
have cloned the repositories.

    git clone https://github.com/amino-cloud/amino.git $AMINO

Step 2
------
Build and install the Amino project

    cd $AMINO && mvn clean install

This will build 3 jars that are of interest:

    $AMINO/amino-impl/database/accumulo/iterators/target/amino-accumulo-iterators-2.1.0-SNAPSHOT-jar-with-dependencies.jar
    $AMINO/amino-impl/database/accumulo/common/target/amino-accumulo-common-2.1.0-SNAPSHOT-job.jar
    $AMINO/amino-impl/job/number/target/number-2.1.0-SNAPSHOT-job.jar

Step 3
-------
Deploy the Accumulo iterator to your Accumulo instances.   You will need to copy
`amino-accumulo-iterators-2.1.0-SNAPSHOT-jar-with-dependencies.jar` to `/opt/accumulo/lib/ext` on **each** of your
Accumulo nodes.  Accumulo **should** pick this up automatically, but occasionally the database needs to be bounced to
load in the iterators.

Step 4
------
Copy the other two jars to a directory on a machine that has access to HDFS, ZooKeeper, and Accumulo.

Step 5
------
Set up HDFS for the numbers data.  The directory structure and files should look like this:

    /amino/numbers/config/AminoDefaults.xml
    /amino/numbers/config/NumberLoader.xml
    /amino/numbers/in/numbers-1k.txt
    /amino/numbers/out/
    /amino/numbers/working

Example versions of `AminoDefaults.xml` and `NumberLoader.xml` can be found in `amino-impl/amino-configs`. Make sure that
you change the values in `AminoDefaults.xml` to point to your ZooKeeper and confgure your Accumulo instancename,
username, and password.
Nothing should need to be changed in `NumberLoader.xml`, but make sure that it does point to the `numbers-1k.txt` file.

The `numbers-1k.txt` file is simply a file containing the numbers 1-1000 each on their own line.  You can generate this
file in bash with the following:

    for (( i=1; i < 1000; i++ )) do echo $i >> numbers-1k.txt; done

Step 6
------
Now that all of the files are in place we can now run the Amino framework to extract the Numbers features from the data.
To do so, run the following commands:

    hadoop jar number-2.1.0-SNAPSHOT-job.jar com._42six.amino.api.framework.FrameworkDriver --amino_default_config_path /amino/numbers/config &&
    hadoop jar amino-accumulo-common-2.1.0-SNAPSHOT-job.jar com._42six.amino.bitmap.DatabasePrepJob /amino/numbers/out /amino/numbers/config &&
    hadoop jar amino-accumulo-common-2.1.0-SNAPSHOT-job.jar com._42six.amino.bitmap.ByBucketsJob /amino/numbers/out /amino/numbers/config /amino/numbers/working &&
    hadoop jar amino-accumulo-common-2.1.0-SNAPSHOT-job.jar com._42six.amino.bitmap.BitLookupJob /amino/numbers/out /amino/numbers/config /amino/numbers/working &&
    hadoop jar amino-accumulo-common-2.1.0-SNAPSHOT-job.jar com._42six.amino.bitmap.StatsJob /amino/numbers/out /amino/numbers/config &&
    hadoop jar amino-accumulo-common-2.1.0-SNAPSHOT-job.jar com._42six.amino.bitmap.HypothesisJob /amino/numbers/out /amino/numbers/config /amino/numbers/working &&
    hadoop jar amino-accumulo-common-2.1.0-SNAPSHOT-job.jar com._42six.amino.bitmap.reverse.ReverseBitmapJob /amino/numbers/out /amino/numbers/config &&
    hadoop jar amino-accumulo-common-2.1.0-SNAPSHOT-job.jar com._42six.amino.bitmap.reverse.ReverseFeatureLookupJob /amino/numbers/out /amino/numbers/config /amino/numbers/working &&
    hadoop jar amino-accumulo-common-2.1.0-SNAPSHOT-job.jar com._42six.amino.bitmap.FeatureMetadataJob /amino/numbers/config

This should run all of the jobs.  Hopefully all of them will work and everything will be set up in Accumulo.  To verify,
check to see that the tables were created and that there are data in them

    # accumulo shell -u username -p password
    amino@Accumulo-instance> table amino<tab>
    amino@Accumulo-instance> table amino_metadata_numbers
    amino@Accumulo-instance> scan


How to run Amino against your own data
======================================

The DataLoader
--------------

First, things first, you need data.   The data that Amino runs against currently must reside in HDFS.  In the example above
this meant that we placed the data in `/amino/numbers/in` on HDFS

Next, you need to write a `DataLoader`.  An example of a `DataLoader` can be found in
`amino-impl/dataloader/number/src/main/java/com/_42six/amino/impl/dataloader/number/NumberLoader.java`.  This class
implements the `DataLoader` interface which has the following methods that must be implemented:

Creating your Jobs
------------------


Querying the data
-----------------
Once you've run your jobs and the data is in Accumulo, you'll probably want to start querying it.  The data is accessible
from the Accumulo shell, unfortunately this is quite cumbersome even for some simple queries.  Because Amino uses
compressed bitmaps to store the features, viewing many of the tables in the shell will be of little utility.

To interact with the data, you will more than likely want to leverage the Thrift services for querying Amino.  The Thrift
files for these services can be found in `amino-core/amino-query-api/src/main/thrift`. The Thrift definitions for many
 of the common objects can be found in `amino-core/amino-common/src/thrift/` Here is an example of setting up a
 synchronous Java Thrift client and server for querying Amino.

__Server__
```java

    /**
     * Simple Thrift Server for accessing the Group information
     */
    public class QueryServer  {
        public static void main(String[] args) throws TTransportException, IOException {
            final String instanceName = "accumulo"; // TODO Change all of the hard coded values
            final String zooKeepers = "accumulo:2181";
            final String user = "amino";
            final String password = "pass";

            final AccumuloPersistenceService persistenceService = new AccumuloPersistenceService(instanceName, zooKeepers, user, password);
            final AccumuloGroupService groupService = new AccumuloGroupService(persistenceService);
            groupService.setGroupMembershipTable("amino_group_membership");
            groupService.setGroupMetadataTable("amino_group_metadata");

            final ThriftGroupService.Processor processor = new ThriftGroupService.Processor(new ThriftGroupServiceHandler(groupService));

            final TServerTransport serverTransport = new TServerSocket(9090, 9999999);

            //final TServer server = new TSimpleServer(new TServer.Args(serverTransport).processor(processor));
            // Use this for multi-threaded server
            TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));

            System.out.println("Starting Amino Accumulo query server...");
            server.serve();
            System.out.println("Shutting down");
        }
    }
```

__Client__
```java

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
```


Appendix
========

Terminology
-----------

* DataLoader - The mechanism for parsing the raw data and translating the data into a format that the Amino framework
can process.
* Enrichment Job -
* Hypothesis - A `Hypothesis` is a collection of features.  For example, you might have a Hypothesis that consists of
the features "Is Even" and "Starts with '1'"
* QueryResult - The results of 'running' a Hypothesis against the data that has been processed by Amino
* Feature - Something that describes the data to parse.  Think of it as an attribute.
* FeatureFact - The value of the feature.  So if your feature was "From City" the fact might be Chicago, or if the
feature was "Touchdowns thrown in one game" the feature fact could be the ratio "3 to 5"

Core Feature Fact Types
-------------
* NOMINAL - This is the basic feature type.  It describes some sort of discrete attribute, like "From City" (with a valid
value being Chicago)
* ORDINAL - A type of fact that has order.  For example "second", "third", "fourth", which would help to preserve ordering
instead of being lexicographically sorted.
* INTERVAL - A range of values - say 2.0 to 4.0
* RATIO - A range of values
* DATE - Represents a date
* DATEHOUR - Represents both date and time
* POLYGON - A list of coordinate points
* POINT - A lat/long coordinate


Code Layout
-----------

The Amino project consists of two main projects, `amino-core` and `amino-impl`.  `amino-core` is just that, it is all of
the core functionality of the Amino project. Most users of the Amino project will not have to deal with anything in here.
For the curious, it is structured into the following sub-directories:

* **amino-api** - The main API components for implementing Amino.
* **amino-archtype** - Maven archetype for creating a new dataloader and job
* **amino-bitmap** - The code related to creating the Amino bitmap indexes
* **amino-common** - All of the classes for representing data, services, Thrift interfaces, etc
* **amino-query-api** - All of the services for interacting between the user and the Amino data (think CRUD)

Then there is `amino-impl`. This is where all of the user specific implementations go. This is where you'll find the
datasource loaders, jobs for processing data, and implementations for storing the data.

Currently, there is an implementation for setting up Amino in Accumulo.  There are also example jobs and dataloaders for
the numbers dataset.  The numbers dataset is a simply a dataset of numbers of the users choosing (say 1-1000).  This
simple dataset is useful for showing how to create dataloaders to read the data into Amino, how to create jobs to get
the data into Amino and how to create features such as "Is Even" or "Starts With".

The project is laid out as such:

* **amino-configs** - Any configuration files needed for a dataset
* **database** - Database implementations for persisting Amino information.  Currently this includes just Accumulo
* **dataloader** - Where the dataloaders for the datasets reside. There is an example dataloader for the numbers dataset
* **ingestion-tools** - Any generic tools that might be helpful with ingestion
* **job** - All of the `AminoJob`s for processing datasets
* **reducer** - The `AminoReducer`s for pulling out specific features from a dataset
* **security** - Implementations for providing security information about users

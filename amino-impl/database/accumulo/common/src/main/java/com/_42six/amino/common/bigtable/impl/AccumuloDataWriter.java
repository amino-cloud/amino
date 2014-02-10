package com._42six.amino.common.bigtable.impl;


import com._42six.amino.common.bigtable.BigTableDataWriter;
import com._42six.amino.common.bigtable.Mutation;
import com._42six.amino.common.bigtable.MutationProto.ColumnValue;
import com.google.protobuf.ByteString;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;

import java.io.IOException;

public class AccumuloDataWriter implements BigTableDataWriter
{
	public static final String ACCUMULO_INSTANCE = "accumulo.instance";
    public static final String ACCUMULO_ZOOKEEPERS = "accumulo.zookeepers";
    public static final String ACCUMULO_USERNAME = "accumulo.username";
    public static final String ACCUMULO_PASSWORD = "accumulo.password";

	private RecordWriter<Text, org.apache.accumulo.core.data.Mutation> recordWriter;
	private Configuration config;

	@Override
	public void initializeFormat(Job job) throws IOException
	{
		final Configuration conf = job.getConfiguration();
		String instanceName = conf.get(ACCUMULO_INSTANCE);		
		String password = conf.get(ACCUMULO_PASSWORD);
		String username = conf.get(ACCUMULO_USERNAME);
		String zookeepers = conf.get(ACCUMULO_ZOOKEEPERS);

//        try {
//            AccumuloOutputFormat.setConnectorInfo(job, username, password);
//        } catch (AccumuloSecurityException e) {
//            e.printStackTrace();
//            throw new IOException(e);
//        }
//        AccumuloOutputFormat.setCreateTables(job, true);
//        AccumuloOutputFormat.setDefaultTableName(job, null);
//        AccumuloOutputFormat.setZooKeeperInstance(job, instanceName, zookeepers);
		AccumuloOutputFormat.setOutputInfo(conf, username, password.getBytes(), true, null);
		AccumuloOutputFormat.setZooKeeperInstance(conf, instanceName, zookeepers);
	}	
	
	@Override
	public OutputFormat getOutputFormat() throws IOException
	{
		return new AccumuloOutputFormat();
	}
	
	@Override
	@SuppressWarnings({"rawtypes", "unchecked"})
	public void setRecordWriter(RecordWriter writer) throws IOException
	{
		this.recordWriter = writer;		
	}	
	
	@Override
	public void putNext(Text table, Mutation m) throws IOException
	{
		try {
			org.apache.accumulo.core.data.Mutation btMutation = new org.apache.accumulo.core.data.Mutation(new Text(m.getRow()));
			for(ColumnValue cv : m.getColumnValues()) 
			{
				if(cv.getIsDelete())
				{
					btMutation.putDelete(fromByteString(cv.getColumnFamily()), fromByteString(cv.getColumnQualifier()), new ColumnVisibility(fromByteString(cv.getColumnVisibility())), cv.getTimestamp());
				}
				else
				{
					btMutation.put(fromByteString(cv.getColumnFamily()), fromByteString(cv.getColumnQualifier()), new ColumnVisibility(fromByteString(cv.getColumnVisibility())), cv.getTimestamp(), new Value(cv.getValue().toByteArray()));
				}		
			}
			recordWriter.write(table, btMutation);
		} catch(InterruptedException e) {
			throw new IOException(e);	
		}
	}

	@Override
	public void setConfig(Configuration conf)
	{
		this.config = conf;
	}
	
	private static Text fromByteString(ByteString bs)
	{
		return new Text(bs.toByteArray());
	}
}

package com._42six.amino.common;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;

public class JobUtilities 
{
	public static void deleteDirectory(Configuration conf, String outputPath) throws Exception
	{
		FsShell shell = new FsShell();
		shell.setConf(conf);

		String[] delCommand = new String[2];
		delCommand[0] = "-rmr";
		delCommand[1] = outputPath;
		shell.run(delCommand);
	}
	
	public static int failureDirHasFiles(Configuration conf, String failureDir) throws IOException 
	{
		FileSystem fs = FileSystem.get(conf);
        FileStatus[] status = fs.listStatus(new Path(failureDir));
        int fileCount = status.length;
        System.out.println(fileCount + " files found in the failures directory on bulk import.");
        fs.close();
        if (fileCount > 0) return 1;
        else return 0;
	}
}

package com._42six.amino.common;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;

public class JobUtilities 
{
    public static void resetWorkingDirectory(Configuration conf, String workingDir) throws Exception
    {
        deleteDirectory(conf, workingDir);
        final FsShell shell = new FsShell(conf);
        
        String[] command = new String[3];
        command[0] = "-mkdir";
        command[1] = "-p";
        command[2] = workingDir + "/failures";
        shell.run(command);

        command = new String[4];
        command[0] = "-chgrp";
        command[1] = "-R";
        command[2] = conf.get("amino.hdfs.workingDirectory.group");
        command[3] = workingDir;
        shell.run(command);

        command = new String[4];
        command[0] = "-chmod";
        command[1] = "-R";
        command[2] = "g+rwx";
        command[3] = workingDir;
        shell.run(command);
    }

	public static void deleteDirectory(Configuration conf, String outputPath) throws Exception
	{
		final FsShell shell = new FsShell();
		shell.setConf(conf);

		final String[] delCommand = new String[3];
//		delCommand[0] = "-rmr";
        delCommand[0] = "-rm";
        delCommand[1] = "-r";
		delCommand[2] = outputPath;
		shell.run(delCommand);
	}
	
	public static int failureDirHasFiles(Configuration conf, String failureDir) throws IOException 
	{
        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
            final FileStatus[] status = fs.listStatus(new Path(failureDir));
            int fileCount = status.length;
            System.out.println(fileCount + " files found in the failures directory on bulk import.");
            return (fileCount > 0) ? 1 : 0;
        } finally {
            if(fs != null){fs.close();}
        }
    }
}

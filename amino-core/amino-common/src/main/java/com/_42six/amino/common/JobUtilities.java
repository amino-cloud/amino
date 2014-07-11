package com._42six.amino.common;

import com._42six.amino.common.util.PathUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.IOException;

public class JobUtilities
{
    /**
     * Resets the working directory to a clean state
     *
     * @param conf The Hadoop {@link org.apache.hadoop.conf.Configuration}
     * @param workingDir The path to the working directory
     * @throws IOException if the old dir could not be deleted or the new one created
     */
    public static void resetWorkingDirectory(Configuration conf, String workingDir) throws IOException
    {
        // Remove the old working dir if it exists
        deleteDirectory(conf, workingDir);

        // Create the new working dir structure
        try(FileSystem fs = FileSystem.get(conf)) {
            fs.mkdirs(new Path(PathUtils.concat(workingDir, "failures")), FsPermission.valueOf("-rwxrwxrwx")); // TODO - See what this needs to be
        }

        // Change the ownership
        // setGroupAndPermissions(conf, workingDir);
    }

    public static void setGroupAndPermissions(Configuration conf, String workingDir) throws Exception
    {
        final String workingGroup = conf.get("amino.hdfs.workingDirectory.group","accumulo");

        final String[] chModCmd = new String[4];
        chModCmd[0] = "-chmod";
        chModCmd[1] = "-R";
        chModCmd[2] = "ugo=rwx"; // TODO - See what this needs to be
        chModCmd[3] = workingDir;

        final String[] chGrpCmd = new String[4];
        chGrpCmd[0] = "-chgrp";
        chGrpCmd[1] = "-R";
        chGrpCmd[2] = workingGroup;
        chGrpCmd[3] = workingDir;

        FsShell shell = null;
        try{
            shell = new FsShell(conf);
            shell.run(chGrpCmd);
            shell.run(chModCmd);
        } finally {
            if (shell != null){
                shell.close();
            }
        }
    }

    /**
     * Recursively deletes the directory, if it exists
     *
     * @param conf The Hadoop {@link org.apache.hadoop.conf.Configuration}
     * @param dirToDelete The directory to recursively delete
     * @throws IOException If the directory could not be deleted
     */
    public static void deleteDirectory(Configuration conf, String dirToDelete) throws IOException
    {
        final Path dir = new Path(dirToDelete);
        try(FileSystem fs = FileSystem.get(conf)) {
            if (fs.exists(dir)) {
                fs.delete(dir, true);
            }
        }
    }

    /**
     * Checks the failures directory to see if there were any failures.  Returns a job status code
     * @param conf The Hadoop {@link org.apache.hadoop.conf.Configuration}
     * @param failureDir The directory to check for failures
     * @return 0 on success, 1 if any failures
     * @throws IOException
     */
    public static int failureDirHasFiles(Configuration conf, String failureDir) throws IOException
    {
        int fileCount = 1;
        try(FileSystem fs = FileSystem.get(conf)) {
            final FileStatus[] status = fs.listStatus(new Path(failureDir));
            fileCount = status.length;
            System.out.println(fileCount + " files found in the failures directory on bulk import.");
        }
        return (fileCount > 0) ? 1 : 0;
    }
}

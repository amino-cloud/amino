package com._42six.amino.bitmap;

import com._42six.amino.common.util.PathUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;

public class IngestUtilities 
{
    private static final Logger logger = LoggerFactory.getLogger(IngestUtilities.class);

	public static ArrayList<FileStatus> grabAllVettedFileStati(Configuration conf, FileSystem fs, String inputDir) throws IOException
	{
		//if this is a directory has multiple data paths, choose the subfolder with the largest size
		FileStatus[] stati = null;
		Set<String> pathSet = PathUtils.getMultipleJobDataPaths(conf, inputDir);
		if (pathSet.size() == 1) {
			stati = fs.listStatus(new Path(pathSet.toArray(new String[1])[0]));
		}
		else {
			Path largestFolder = null;
			long largestSize = 0;
			for (String pathStr : pathSet) {
				Path path = new Path(pathStr);
				//if this is a directory, and it's block size is larger, make it the new largest folder
				if (!fs.isFile(path)) {
					long size = 0;
					for (FileStatus subStatus : fs.listStatus(path)) {
						size += subStatus.getBlockSize();
					}
					if (largestFolder == null || size > largestSize) {
						largestFolder = path;
						largestSize = size;
					}
				}
			}
			if (largestFolder == null) {
				logger.info("No folders found in input directory ["
						+ inputDir + "], so using root directory for sample.");
				String realPath = inputDir.substring(0, inputDir.length() - 1);
				stati = fs.listStatus(new Path(realPath));
			}
			else {
				logger.info("Using directory [" + largestFolder
						+ "] for sample because it has the largest size [" + largestSize + "].");
				stati = fs.listStatus(largestFolder);
			}
		}
		
		//remove certain files (_SUCCESS and _LOG for example)
		ArrayList<FileStatus> vettedStatus = new ArrayList<FileStatus>();
		for (FileStatus stat : stati) {
			if (!stat.getPath().getName().startsWith("_")) {
				vettedStatus.add(stat);
			}
		}
		
		return vettedStatus;
	}
}

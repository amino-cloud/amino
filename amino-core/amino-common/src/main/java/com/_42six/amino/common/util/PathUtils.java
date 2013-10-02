package com._42six.amino.common.util;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class PathUtils {
	
	private static final String JOB_DATA_FOLDER = "data";
	private static final String JOB_METADATA_FOLDER = "cache";
	private static final String CACHE_PATH_PROPERTY = "amino.job.cache.path";
	
	public static String getJobDataPath(String rootPath) {
		return rootPath.endsWith("/") ? rootPath + JOB_DATA_FOLDER : rootPath + "/" + JOB_DATA_FOLDER;
	}
	
	public static String getJobCachePath(String rootPath) {
		return rootPath.endsWith("/") ? rootPath + JOB_METADATA_FOLDER : rootPath + "/" + JOB_METADATA_FOLDER;
	}
	
	@SuppressWarnings("serial")
	public static Set<String> getJobDataPaths(Configuration conf, final String rootPath) throws IOException {
		if (rootPath.endsWith("*")) {
			Set<String> pathSet = new HashSet<String>();
			
			//go through sub directories and add the job data directories
			FileSystem fs = FileSystem.get(conf);
			for (FileStatus status : fs.listStatus(new Path(rootPath.substring(0, rootPath.length() - 1)))) {
				if (!fs.isFile(status.getPath())) {
					Path dataPath = new Path(getJobDataPath(status.getPath().toString()));
					if (!fs.exists(dataPath)) {
						throw new IOException("Missing data directory [" + dataPath 
								+ "] in input directory [" + status.getPath() + "].");
					}
					pathSet.add(dataPath.toString());
				}
				else {
					System.err.println("Ignoring file [" + status.getPath() 
							+ "] found in path rootPath [" + rootPath + "].");
				}
			}
			if (pathSet.isEmpty()) {
				throw new IOException("No job data folders found in input path [" + rootPath + "].");
			}
			return pathSet;
		}
		else {
			return new HashSet<String>() {{ add(getJobDataPath(rootPath)); }};
		}
	}
	
	@SuppressWarnings("serial")
	public static Set<String> getJobCachePaths(Configuration conf, final String rootPath) throws IOException {
		if (rootPath.endsWith("*")) {
			Set<String> pathSet = new HashSet<String>();
			
			//go through sub directories and add the job data directories
			FileSystem fs = FileSystem.get(conf);
			for (FileStatus status : fs.listStatus(new Path(rootPath.substring(0, rootPath.length() - 1)))) {
				if (!fs.isFile(status.getPath())) {
					Path cachePath = new Path(getJobCachePath(status.getPath().toString()));
					if (!fs.exists(cachePath)) {
						throw new IOException("Missing cache directory [" + cachePath 
								+ "] in input directory [" + status.getPath() + "].");
					}
					pathSet.add(cachePath.toString());
				}
				else {
					System.err.println("Ignoring file [" + status.getPath() 
							+ "] found in path rootPath [" + rootPath + "].");
				}
			}
			if (pathSet.isEmpty()) {
				throw new IOException("No cache data folders found in input path [" + rootPath + "].");
			}
			return pathSet;
		}
		else {
			return new HashSet<String>() {{ add(getJobCachePath(rootPath)); }};
		}
	}
	
	public static String getCachePath(Configuration conf) {
		return conf.get(CACHE_PATH_PROPERTY);
	}
	
	public static String[] getCachePaths(Configuration conf) {
		return conf.getStrings(CACHE_PATH_PROPERTY);
	}
	
	public static void setCachePath(Configuration conf, String path) {
		conf.set(CACHE_PATH_PROPERTY, path);
	}
	
	public static void setCachePaths(Configuration conf, Set<String> paths) {
		conf.set(CACHE_PATH_PROPERTY, StringUtils.join(paths, ','));
	}
}

package com._42six.amino.common.service;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Utility class for adding files to the distributed cache.
 * Each DistributedCacheService keeps a list of hashes for 
 * cached files to ensure a file isn't added more than once.
 */
public class DistributedCacheService {
	
	private static final String AMINO_HDFS_CACHE_FILES = "amino.hdfs.cache.files";
	private static final String AMINO_HDFS_CACHE_ARCHIVES = "amino.hdfs.cache.archives";
	
	private Set<Integer> cacheFileHashes;
	private Set<Integer> cacheArchiveHashes;
	
	public DistributedCacheService() throws IOException {
		cacheFileHashes = new HashSet<Integer>();
		cacheArchiveHashes = new HashSet<Integer>();
	}
	
	/**
	 * Add files to the the distributed cache using the comma delimited list
	 * of values in these properties:
	 * 		- AMINO_HDFS_CACHE_FILES
	 * 		- AMINO_HDFS_CACHE_ARCHIVES.
	 * If a file is specified, add to the distributed cache.
	 * If a folder is specified, recursively add files to the distributed cache.
	 * 
	 * @param conf
	 * @throws IOException If a file/folder specified does not exist or there
	 * is a problem working with the file system.
	 */
	public void addFilesToDistributedCache(Configuration conf) throws IOException {
		FileSystem fs = FileSystem.get(conf);

		//cache files
		String[] hdfsFiles = conf.getStrings(AMINO_HDFS_CACHE_FILES);
		if (hdfsFiles != null && hdfsFiles.length > 0) {
			for (String filePath : hdfsFiles) {
				if (filePath != null && filePath.length() > 0) {
					Set<Path> pathSet = getHdfsFilePathsRecursively(new Path(filePath), conf, fs);
					for (Path path : pathSet) {
						addCacheFile(conf, path);
					}
				}
			}
		}
		
		//cache archives
		String[] hdfsArchives = conf.getStrings(AMINO_HDFS_CACHE_ARCHIVES);
		if (hdfsArchives != null && hdfsArchives.length > 0) {
			for (String filePath : hdfsArchives) {
				if (filePath != null && filePath.length() > 0) {
					Set<Path> pathSet = getHdfsFilePathsRecursively(new Path(filePath), conf, fs);
					for (Path path : pathSet) {
						addCacheArchive(conf, path);
					}
				}
			}
		}
	}
	
	public void addCacheFile(Configuration conf, Path path) {
		if (!cacheFileHashes.contains(path.hashCode())) {
			DistributedCache.addCacheFile(path.toUri(), conf);
			cacheFileHashes.add(path.hashCode());
			System.out.println("Added file [" + path + "] to the Distributed Cache");
		}
	}
	
	public void addCacheArchive(Configuration conf, Path path) {
		if (!cacheArchiveHashes.contains(path.hashCode())) {
			DistributedCache.addCacheArchive(path.toUri(), conf);
			cacheArchiveHashes.add(path.hashCode());
			System.out.println("Added archive [" + path + "] to the Distributed Cache");
		}
	}
	
	/**
	 * Gets a list of files for this folder or file.
	 * 
	 * @param rootPath File or folder path
	 * @param conf
	 * @param fs
	 * @return
	 * @throws IOException if the path doesn't exist
	 */
	private static Set<Path> getHdfsFilePathsRecursively(Path rootPath, Configuration conf, FileSystem fs) throws IOException {
		Set<Path> pathOut = new HashSet<Path>();
		
		//if path doesn't exist, throw exception
		if (!fs.exists(rootPath)) {
			throw new FileNotFoundException("HDFS path [" + rootPath 
					+ "] does not exist and cannot be added to the DistributedCache!");
		}
		//if file, just add the file
		else if (fs.isFile(rootPath)) {
			pathOut.add(rootPath);
		}
		//if directory, recursively add the folder contents
		else {
			for (FileStatus status : fs.listStatus(rootPath)) {
				pathOut.addAll(getHdfsFilePathsRecursively(status.getPath(), conf, fs));
			}
		}
		
		return pathOut;
	}

}

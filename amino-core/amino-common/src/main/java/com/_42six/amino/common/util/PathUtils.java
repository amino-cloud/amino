package com._42six.amino.common.util;

import com._42six.amino.common.AminoConfiguration;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;


public class PathUtils {

    private static final Logger logger = LoggerFactory.getLogger(PathUtils.class);

    private static final String JOB_WORKING_FOLDER = "working";
	private static final String JOB_DATA_FOLDER = "data";
	private static final String JOB_METADATA_FOLDER = "cache";
	private static final String CACHE_PATH_PROPERTY = "amino.job.cache.path";
	private static final String JOB_CACHE_METADATA_FOLDER = "metadata";

    /**
     * Takes the comma seperated list of base paths and returns the output directory of each path
     * @param paths A comma separated list of paths to search through
     * @return a {@link java.util.Set} of output paths from Analytic jobs
     */
    public static Set<String> getOutputDirs(String paths){
        final Set<String> outputPaths = new HashSet<>();
        for(String path : paths.split(",")){
            outputPaths.add(concat(path, "out"));
        }
        return outputPaths;
    }

    /**
     * Checks to see if the path exists on HDFS.  If it does not, and the create flag is set to true in the configuration,
     * it will attempt to create the Path.  If not, it will throw an Exception that the path doesn't exist.
     *
     * @param paths Comma separated list of Path to check the existence of
     * @param conf The Hadoop Configuration
     * @throws IOException Problem creating the path or path doesn't exist
     */
    public static void pathsExists(String paths, Configuration conf) throws IOException {
        final boolean create = conf.getBoolean(AminoConfiguration.CREATE_IF_NOT_EXIST, false);
        pathsExists(paths, conf, create);
    }

    /**
     * Checks to see if the path exists on HDFS.  If it does not, and the create flag is set to true, it will attempt to
     * create the Path.  If not, it will throw an Exception that the path doesn't exist.
     *
     * @param paths Comma separated list of Path to check the existence of
     * @param conf The Hadoop Configuration
     * @param createIfNotExist Whether or not to try and create the Path if it doesn't already exist
     * @throws IOException Problem creating the path or path doesn't exist
     */
    public static void pathsExists(String paths, Configuration conf, boolean createIfNotExist) throws IOException {
        final FileSystem fs = FileSystem.get(conf);
        for(String path : paths.split(",")){
            final Path p = new Path(path);
            if(!fs.exists(p)){
                if(createIfNotExist){
                    fs.mkdirs(p);
                } else {
                    throw new IOException("Path '" + path + "' does not exist");
                }
            }
        }
    }

    /**
     * Simple method to concatenate a directory onto a base path.  Use this instead of
     * {@link org.apache.commons.io.FilenameUtils#concat(String, String)} as that will automatically call
     * {@link org.apache.commons.io.FilenameUtils#normalize(String)}, which for HDFS URI's will strip out one of the
     * /'s in hdfs:// making it an invalid URI.
     * @param base The base part of the URI
     * @param end The part to concatenate
     * @return The combination of the base and end, with a / in the middle if need be
     */
    public static String concat(String base, String end){
        return base.endsWith("/") ? base + end : base + "/" + end;
    }

    public static String getJobWorkingPath(String rootPath) {
        return concat(rootPath, JOB_WORKING_FOLDER);
    }

	public static String getJobDataPath(String rootPath) {
        return concat(rootPath, JOB_DATA_FOLDER);
	}
	
	public static String getJobCachePath(String rootPath) {
        return concat(rootPath, JOB_METADATA_FOLDER); // TODO - why isn't this CACHE_METADATA
	}

    /**
     * Grabs all of the data paths from multiple base paths.
     *
     * @param conf The {@link org.apache.hadoop.conf.Configuration} of the job
     * @param basePaths A ',' separated string of the project base baths to traverse through
     * @return {@link java.util.Set} of all of the data directories
     * @throws IOException
     * @see com._42six.amino.common.util.PathUtils#getJobDataPaths(org.apache.hadoop.conf.Configuration, String)
     */
    public static Set<String> getMultipleJobDataPaths(final Configuration conf, final String basePaths) throws IOException {
        final Set<String> dataPaths = new HashSet<>();
        for(String path : basePaths.split(",")){
            dataPaths.addAll(getJobDataPaths(conf, path));
        }
        return dataPaths;
    }

	@SuppressWarnings("serial")
	public static Set<String> getJobDataPaths(Configuration conf, final String rootPath) throws IOException {
		if (rootPath.endsWith("*")) {
			Set<String> pathSet = new HashSet<String>();
			
			// go through sub directories and add the job data directories
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

    /**
     * Grabs all of the cache paths from multiple base paths.
     *
     * @param conf The {@link org.apache.hadoop.conf.Configuration} of the job
     * @param basePaths A ',' separated string of the project base baths to traverse through
     * @return {@link java.util.Set} of all of the cache directories
     * @throws IOException
     * @see com._42six.amino.common.util.PathUtils#getJobCachePaths(org.apache.hadoop.conf.Configuration, String)
     */
    public static Set<String> getMultipleJobCachePaths(final Configuration conf, final String basePaths) throws IOException {
        final Set<String> cachePaths = new HashSet<>();
        for(String path : basePaths.split(",")){
            cachePaths.addAll(getJobCachePaths(conf, path));
        }
        return cachePaths;
    }

	@SuppressWarnings("serial")
	public static Set<String> getJobCachePaths(Configuration conf, final String rootPath) throws IOException {
		logger.info("Getting Job Cache path for root path <" + rootPath + ">");
        if (rootPath.endsWith("*")) {
			Set<String> pathSet = new HashSet<String>();
			
			// go through sub directories and add the job data directories
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

    /**
     * Grabs all of the metadata output directories from multiple base paths.
     *
     * @param conf The {@link org.apache.hadoop.conf.Configuration} of the job
     * @param basePaths A ',' separated string of the project base baths to traverse through
     * @return {@link java.util.Set} of all of the metadata directories
     * @throws IOException
     * @see com._42six.amino.common.util.PathUtils#getJobMetadataPaths(org.apache.hadoop.conf.Configuration, String)
     */
    public static Set<String> getMultipleJobMetadataPaths(final Configuration conf, final String basePaths) throws IOException {
        logger.info("Getting base paths from: " + basePaths);
        final Set<String> metadataPaths = new HashSet<>();
        for(String path : basePaths.split(",")){
            metadataPaths.addAll(getJobMetadataPaths(conf, path));
        }
        return metadataPaths;
    }

    /**
     * Traverses through the rootPath looking for the metadata output directories of previous job runs and returns
     * them as a set
     *
     * @param conf The {@link org.apache.hadoop.conf.Configuration} of the job
     * @param rootPath The base path to start the traversal from
     * @return {@link java.util.Set} of all the metadata directories in the tree
     * @throws IOException
     */
	public static Set<String> getJobMetadataPaths(Configuration conf, final String rootPath) throws IOException {
		final Set<String> cachePaths = getJobCachePaths(conf, rootPath);
		final FileSystem fs = FileSystem.get(conf);
		final Set<String> metadataPaths = new HashSet<>();

		for (String cachePath : cachePaths) {
			final Path metadataFolder = new Path(concat(cachePath, JOB_CACHE_METADATA_FOLDER));
			if (!fs.exists(metadataFolder)) {
				throw new IOException("Missing metadata directory [" + metadataFolder 
						+ "] in directory [" + cachePath + "]");
			}
			else if (fs.isFile(metadataFolder)) {
				throw new IOException("Metadata path [" + metadataFolder + "] exists, but is a file, not a folder");
			}
			else {
				metadataPaths.add(metadataFolder.toString());
			}
		}
		return metadataPaths;
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

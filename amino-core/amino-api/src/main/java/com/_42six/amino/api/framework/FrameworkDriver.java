package com._42six.amino.api.framework;


import com._42six.amino.api.framework.enrichment.*;
import com._42six.amino.api.job.AminoEnrichmentJob;
import com._42six.amino.api.job.AminoJob;
import com._42six.amino.api.job.AminoReuseEnrichmentJob;
import com._42six.amino.common.*;
import com._42six.amino.common.service.DistributedCacheService;
import com._42six.amino.common.util.PathUtils;
import com._42six.amino.data.*;
import com._42six.amino.data.impl.EnrichmentDataLoader;
import com._42six.amino.data.utilities.CacheBuilder;
import com.google.common.base.Preconditions;
import org.apache.commons.cli.*;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.*;

public final class FrameworkDriver extends Configured implements Tool {
    private static final String AMINO_NUM_REDUCERS = "amino.num.reducers";
    private static final String AMINO_NUM_REDUCERS_ENRICH_PHASE1 = "amino.enrich.phase1.num.reducers";
    private static final String AMINO_NUM_REDUCERS_ENRICH_PHASE2 = "amino.enrich.phase2.num.reducers";

    private static final int DEFAULT_NUM_REDUCERS = 14;

    private static final Logger logger = LoggerFactory.getLogger(FrameworkDriver.class);

    private static final int JOB_TYPE_NORMAL = 0;
    private static final int JOB_TYPE_ENRICHMENT = 1;
    private static final int JOB_TYPE_REUSE_ENRICHMENT = 2;

    private String enrichmentOutput = "";
    private static boolean stopOnFirstPhase = false;

    public static final String STATUS_FILE = "status.pid";

    /**
     * Used to mark the current status of the job.  Currently it is written to HDFS in STATUS_FILE, but might be a good idea
     * to write it to Zookeeper in the future.
     */
    public static enum JobStatus{
        RUNNING,
        FAILED,
        COMPLETE,
        QUEUED // Not currently used
    }

    static class XMLFileNameFilter implements FilenameFilter {
        public boolean accept(File directory, String filename) {
            return filename.endsWith(".xml");
        }
    }

    public static Options constructGnuOptions() {
        final Options gnuOptions = new Options();
        Option aminoDefaultConfigurationOption = new Option("d", "amino_default_config_path", true, "The path where the amino default file lives.");
        aminoDefaultConfigurationOption.setRequired(true);
        gnuOptions.addOption(aminoDefaultConfigurationOption);
        gnuOptions.addOption("b", "base_dir", true, "The base directory of the running job");
        gnuOptions.addOption("c", "amino_config_file_path", true, "A CSV of filenames or paths which will be acted like a classpath setting up configurations!");
        gnuOptions.addOption("stop", "stop_on_first_phase", false, "Stop after the first phase of an AminoEnrichmentJob");

        Option propertyOverride = new Option("D", "property_override", true, "A map of key/value configuration properties to override (ie: 'key=value')");
        propertyOverride.setValueSeparator('=');
        propertyOverride.setArgs(2);
        gnuOptions.addOption(propertyOverride);

        return gnuOptions;
    }

    public static List<String> getUserConfigFiles(final String path) throws IOException {
        File configPath = new File(path);
        if (!configPath.exists()) {
            throw new IOException(path + " does not exist!");
        }
        // We are going to assume that this is a top level directory with configuration files in it, we are not going to recur downwards
        LinkedList<String> paths = new LinkedList<String>();
        if (configPath.isDirectory()) {
            for (File file : configPath.listFiles(new XMLFileNameFilter())) {
                paths.push(file.getAbsolutePath());
            }
        } else {
            paths.push(configPath.getAbsolutePath());
        }
        return paths;
    }

    /**
     * Load the Configuration file with the values from the command line and config files, and place stuff in the
     * DistrubutedCacheService as needed
     *
     * @param conf The Configuration to populate
     * @param args The command line arguments
     * @throws Exception
     */
    public static void initalizeConf(Configuration conf, String[] args) throws Exception{
        // Parse the arguments and make sure the required args are there
        final CommandLine commandLine;
        final Options options = constructGnuOptions();
        try{
            commandLine = new GnuParser().parse(options, args);
        } catch (MissingOptionException ex){
            HelpFormatter help = new HelpFormatter();
            help.printHelp("hadoop jar <jarFile> " + FrameworkDriver.class.getCanonicalName(), options);
            return;
        } catch (Exception ex){
            ex.printStackTrace();
            return;
        }

        final String userConfFilePath = commandLine.getOptionValue("amino_config_file_path", "");
        final String aminoDefaultConfigPath = commandLine.getOptionValue("amino_default_config_path");
        final String baseDir = commandLine.getOptionValue("base_dir");

        stopOnFirstPhase = commandLine.hasOption("stop");

        // Set the base dir config value if it was provided.
        if(StringUtils.isNotEmpty(baseDir)){
            conf.set(AminoConfiguration.BASE_DIR, baseDir);
        }

        conf.set(AminoConfiguration.DEFAULT_CONFIGURATION_PATH_KEY, aminoDefaultConfigPath);

        // create a single DistributedCacheService so that multiple cache entries are deduped.
        // Cache files are added after each config is loaded in case the the property value changes.
        final DistributedCacheService distributedCacheService = new DistributedCacheService();

        // 1. load AminoDefaults
        AminoConfiguration.loadAndMergeWithDefault(conf, true);
        distributedCacheService.addFilesToDistributedCache(conf);

        // 2. load user config files, allowing them to overwrite
        if (!StringUtils.isEmpty(userConfFilePath)) {
            for (String path : getUserConfigFiles(userConfFilePath)) {
                Configuration userConf = new Configuration(false);
                logger.info("Grabbing configuration information from: " + path);
                userConf.addResource(new FileInputStream(path));
                HadoopConfigurationUtils.mergeConfs(conf, userConf);
            }
        }
        distributedCacheService.addFilesToDistributedCache(conf);

        // 3. load command line arguments as properties, allowing them to overwrite
        final Properties propertyOverrides = commandLine.getOptionProperties("property_override");
        for (Object key : propertyOverrides.keySet()) {
            conf.set((String) key, (String) propertyOverrides.get(key));
        }
        distributedCacheService.addFilesToDistributedCache(conf);
    }

    public static void main(String[] args) throws Exception {
        final Configuration conf = new Configuration();
        initalizeConf(conf, args);

        int res = ToolRunner.run(conf, new FrameworkDriver(), args);
        System.exit(res);
    }

    /**
     * Updates the status PID file for the job.
     *
     * @param conf The {@link org.apache.hadoop.conf.Configuration} for the map reduce job
     * @param status The {@link com._42six.amino.api.framework.FrameworkDriver.JobStatus} to change to
     * @param pidDir The {@link org.apache.hadoop.fs.Path} to the PID file
     * @throws IOException
     */
    public static void updateStatus(Configuration conf, JobStatus status, Path pidDir) throws IOException {
        final Path pidFile = new Path(pidDir, STATUS_FILE);

        // Create the file if it doesn't exist and overwrite whatever might have been in it
        try(FileSystem fs = FileSystem.get(conf); FSDataOutputStream os = fs.create(pidFile, true) ){
            os.writeUTF(status.toString());
        }
    }

    /**
     * Updates the status PID file for the job.
     *
     * @param status The {@link com._42six.amino.api.framework.FrameworkDriver.JobStatus} to change to
     * @param pidDir The {@link org.apache.hadoop.fs.Path} to the PID file
     * @throws IOException
     */
    public void updateStatus(JobStatus status, Path pidDir) throws IOException{
        final Configuration conf = getConf();
        FrameworkDriver.updateStatus(conf, status, pidDir);
    }

    /**
     * Updates the status PID file for the currently running job.  Uses the currently configured BASE_DIR as the
     * location of the PID file.
     *
     * @param status The {@link com._42six.amino.api.framework.FrameworkDriver.JobStatus} to change to
     * @throws IOException
     */
    public void updateStatus(JobStatus status) throws IOException {
        final Configuration conf = getConf();
        final String baseDir = conf.get(AminoConfiguration.BASE_DIR, null);
        Preconditions.checkNotNull(baseDir, "basedir could not be found in the Amino configuration");
        updateStatus(status, new Path(baseDir));
    }

    public int run(String[] args) throws Exception {
        ServiceLoader<? extends AminoJob> jobs = ServiceLoader.load(AminoJob.class);
        if (!jobs.iterator().hasNext()) {
            jobs = ServiceLoader.load(AminoEnrichmentJob.class);
        }
        if (!jobs.iterator().hasNext()) {
            jobs = ServiceLoader.load(AminoReuseEnrichmentJob.class);
        }

        Configuration conf = getConf();
        // AminoConfiguration.loadDefault(conf, "AminoDefaults", true);
        // boolean complete = createTables(conf);
        boolean complete = true;

        updateStatus(JobStatus.RUNNING);

        //boolean complete = true;
        for (AminoJob aj : jobs) {
            aj.setConfig(conf);
            logger.info("Running Job -> " + aj.getJobName());
            if (complete) {
                Job job = new Job(conf, aj.getJobName());
                job.setJarByClass(aj.getClass());

                // Add the class to the conf it can be grabbed in the Reduce phase
                AminoDriverUtils.setAminoJob(job.getConfiguration(), aj.getClass());

                int jobType = setJobParameters(job, aj);

                // Call job configuration for special properties
                jobConfiguration(job);

                complete = job.waitForCompletion(true);

                if (jobType == JOB_TYPE_ENRICHMENT || jobType == JOB_TYPE_REUSE_ENRICHMENT) {
                    if (!stopOnFirstPhase) {
                        stopOnFirstPhase = conf.getBoolean("stop.on.first.phase", stopOnFirstPhase);
                    }
                    if (complete && !stopOnFirstPhase) {
                        complete = runSecondPhaseEnrichmentJob((AminoEnrichmentJob) aj, conf, jobType);
                        if (jobType == JOB_TYPE_REUSE_ENRICHMENT) ((AminoReuseEnrichmentJob) aj).directoryCleanup(conf);
                    } else if (!complete) {
                        System.err.println("Job failed, unable to run second enrichment step");
                    }
                }
            }
        }

        updateStatus(complete ? JobStatus.COMPLETE : JobStatus.FAILED);

        return complete ? 0 : 1;
    }

    /**
     * Adds configuration options to the job.
     *
     * @param job Map Reduce job that will be run
     */
    private void jobConfiguration(Job job) {
        // TODO: Can this go in setJobParameters?

        // Set the "user classpath first" configuration option. In some versions of hadoop
        // the TaskRunner doesn't seem to check this property when assembling the classpath
        try {
            Method m = Job.class.getMethod("setUserClassesTakesPrecedence", new Class[]{boolean.class});
            m.invoke(job, job.getConfiguration().getBoolean("mapreduce.user.classpath.first", false));
        } catch (Exception e) {
            System.err.println("Warning: Job Configuration for mapreduce.user.classpath.first failed");
        }
    }

    private int setJobParameters(Job job, AminoJob aj) throws Exception {
        final Configuration conf = job.getConfiguration();
        final Class<? extends DataLoader> dataLoaderClass = aj.getDataLoaderClass();
        AminoInputFormat.setDataLoader(job.getConfiguration(), dataLoaderClass.newInstance());

        if (aj instanceof AminoEnrichmentJob) {
            String output = "";
            int returnType = JOB_TYPE_ENRICHMENT;

            if (aj instanceof AminoReuseEnrichmentJob) {
                System.out.println("Running REUSE Enrichment Join Job");

                AminoReuseEnrichmentJob reuseJob = (AminoReuseEnrichmentJob) aj;
                AminoInputFormat.setDataLoader(job.getConfiguration(), reuseJob.getFirstPhaseDataLoaderClass().newInstance());

                String root = conf.get(AminoDriverUtils.ENRICHMENT_ROOT_OUTPUT);
                String front = "";
                if (!root.endsWith("/")) front = "/";
                root += front;
                String dir = reuseJob.getOutputSubDirectory(conf);
                output += root + dir;

                returnType = JOB_TYPE_REUSE_ENRICHMENT;
            } else {
                System.out.println("Running Enrichment Join Job");
            }

            int numReducers = conf.getInt(AMINO_NUM_REDUCERS_ENRICH_PHASE1, conf.getInt(AMINO_NUM_REDUCERS, DEFAULT_NUM_REDUCERS));
            job.setNumReduceTasks(numReducers);

            // Our Framework mapper and reducer
            job.setMapperClass(FrameworkEnrichmentJoinMapper.class);
            job.setCombinerClass(FrameworkEnrichmentJoinCombiner.class);
            job.setReducerClass(FrameworkEnrichmentJoinReducer.class);

            job.setMapOutputKeyClass(EnrichmentJoinKey.class); // Different
            job.setMapOutputValueClass(MapWritable.class);

            job.setOutputKeyClass(BucketStripped.class);
            job.setOutputValueClass(MapWritable.class); // Different

            job.setPartitionerClass(NaturalKeyPartitioner.class);
            job.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);
            job.setSortComparatorClass(CompositeKeyComparator.class);

            job.setInputFormatClass(AminoMultiInputFormat.class);

            AminoEnrichmentJob aej = (AminoEnrichmentJob) aj;
            // AminoMultiInputFormat.setJoinDataLoader(conf, aej.getEnrichmentDataLoader().newInstance());
            AminoMultiInputFormat.setJoinDataLoaders(conf, aej.getEnrichmentDataLoaders());
            AminoMultiInputFormat.setEnrichWorker(conf, aej.getEnrichWorker().newInstance());

            job.setOutputFormatClass(SequenceFileOutputFormat.class);

            // TODO If it already exists, and its age is less than job running frequency, just reuse it instead of doing the above job...
            if (output.length() == 0) {
                output = getEnrichmentOutputPath(aej, conf);
            }
            System.out.println("Output will be written to: " + PathUtils.getJobDataPath(output));

            SequenceFileOutputFormat.setOutputPath(job, new Path(PathUtils.getJobDataPath(output)));
            JobUtilities.deleteDirectory(conf, output);

            CacheBuilder.buildCaches(AminoDataUtils.getDataLoader(conf), aj, output, conf);

            return returnType;

        } else {
            System.out.println("\n==================== Running Amino Job =================\n");

            // Our Framework mapper and reducer
            job.setMapperClass(FrameworkMapper.class);
            job.setReducerClass(FrameworkReducer.class);

            job.setMapOutputKeyClass(BucketStripped.class);
            job.setMapOutputValueClass(MapWritable.class);

            job.setOutputKeyClass(BucketStripped.class);
            job.setOutputValueClass(AminoWritable.class);

            job.setInputFormatClass(AminoInputFormat.class);

            job.setOutputFormatClass(AminoOutputFormat.class);
            job.setNumReduceTasks(conf.getInt(AMINO_NUM_REDUCERS, DEFAULT_NUM_REDUCERS));

            AminoOutputFormat.setAminoConfigPath(job, job.getConfiguration().get(AminoConfiguration.DEFAULT_CONFIGURATION_PATH_KEY));

            String output = conf.get("amino.output");
            System.out.println("Output will be written to: " + PathUtils.getJobDataPath(output));
            AminoOutputFormat.setOutputPath(job, new Path(PathUtils.getJobDataPath(output)));
            JobUtilities.deleteDirectory(conf, output);

            CacheBuilder.buildCaches(AminoDataUtils.getDataLoader(conf), aj, output, conf);
            return JOB_TYPE_NORMAL;
        }
    }

    private String getEnrichmentOutputPath(AminoEnrichmentJob aej, Configuration conf) {
        String output = conf.get(AminoDriverUtils.ENRICHMENT_ROOT_OUTPUT);
        String front = "";
        if (!output.endsWith("/")) {
            front = "/";
        }
        output += front + aej.getDataLoaderClass().getSimpleName() + "_" + aej.getEnrichWorker().getSimpleName();
        this.enrichmentOutput = output;

        return output;
    }

    private boolean runSecondPhaseEnrichmentJob(AminoEnrichmentJob aej, Configuration conf, int jobType) throws Exception {
        System.out.println("Running Amino Job");

        final Job job = new Job(conf, aej.getJobName() + " phase 2");
        job.setJarByClass(aej.getClass());

        AminoDriverUtils.setAminoJob(job.getConfiguration(), aej.getClass());

        if (jobType == JOB_TYPE_ENRICHMENT) {
            job.getConfiguration().set(AminoDriverUtils.ENRICHMENT_OUTPUT, this.enrichmentOutput);
        } else if (jobType == JOB_TYPE_REUSE_ENRICHMENT) {
            String root = conf.get(AminoDriverUtils.ENRICHMENT_ROOT_OUTPUT);
            String front = "";
            if (!root.endsWith("/")) {
                front = "/";
            }
            root += front;

            final Iterable<String> inputs = ((AminoReuseEnrichmentJob) aej).getSecondPhaseEnrichmentInputDirectories(job.getConfiguration());
            String inputStr = "";
            System.out.println("Using enrichment input paths:");
            for (String input : inputs) {
                if (inputStr.length() > 0) {
                    inputStr += "," + PathUtils.getJobDataPath(root + input);
                } else {
                    inputStr += PathUtils.getJobDataPath(root + input);
                }
                System.out.println(PathUtils.getJobDataPath(root + input));
            }

            job.getConfiguration().set(AminoDriverUtils.ENRICHMENT_OUTPUT, inputStr);

            //Need to do this because the first phase data loader is sitting in this slot currently
            AminoInputFormat.setDataLoader(job.getConfiguration(), aej.getDataLoaderClass().newInstance());
        }

        int numReducers = job.getConfiguration().getInt(AMINO_NUM_REDUCERS_ENRICH_PHASE2, job.getConfiguration().getInt(AMINO_NUM_REDUCERS, DEFAULT_NUM_REDUCERS));
        job.setNumReduceTasks(numReducers);

        job.setMapperClass(FrameworkMapper.class);
        job.setReducerClass(FrameworkReducer.class);

        job.setMapOutputKeyClass(BucketStripped.class);
        job.setMapOutputValueClass(MapWritable.class);

        job.setOutputKeyClass(BucketStripped.class);
        job.setOutputValueClass(AminoWritable.class);

        job.setInputFormatClass(AminoMultiInputFormat.class);
        AminoMultiInputFormat.setDataLoader(job.getConfiguration(), aej.getDataLoaderClass().newInstance());

        // Call job configuration for special properties
        jobConfiguration(job);

        @SuppressWarnings("serial")
        ArrayList<Class<? extends DataLoader>> joinSource = new ArrayList<Class<? extends DataLoader>>() {{
            add(EnrichmentDataLoader.class);
        }};
        AminoMultiInputFormat.setJoinDataLoaders(job.getConfiguration(), joinSource);

        job.setOutputFormatClass(AminoOutputFormat.class);
        AminoOutputFormat.setAminoConfigPath(job, job.getConfiguration().get(AminoConfiguration.DEFAULT_CONFIGURATION_PATH_KEY));

        String output = job.getConfiguration().get("amino.output");
        System.out.println("Output will be written to: " + PathUtils.getJobDataPath(output));
        AminoOutputFormat.setOutputPath(job, new Path(PathUtils.getJobDataPath(output)));
        JobUtilities.deleteDirectory(job.getConfiguration(), output);
        CacheBuilder.buildCaches(AminoDataUtils.getDataLoader(job.getConfiguration()), aej, output, job.getConfiguration());

        return job.waitForCompletion(true);
    }
}

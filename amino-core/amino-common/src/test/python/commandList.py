# True = Only execute if previous command was successful
# False = Ignore return code of previous command
# Current limitation: doesn't support two jobs executing at the same time with different boolean command list operators (ie. one is true, one is false)

commands = [

#blow out the workspace directory
[['/opt/hadoop/bin/hadoop fs -rmr /amino/numbers/out', False]],

#execute the jobs to create the features sequentially
[['/opt/hadoop/bin/hadoop jar number-1.7.0-SNAPSHOT-job.jar com._42six.amino.api.framework.FrameworkDriver --amino_default_config_path /amino/numbers/config', False]],

#run the indexing jobs in parallel
[['/opt/hadoop/bin/hadoop jar amino-bitmap-1.7.0-SNAPSHOT-job.jar com._42six.amino.bitmap.BtMetadataImporterJob /amino/numbers/config', True],
['/opt/hadoop/bin/hadoop jar amino-bitmap-1.7.0-SNAPSHOT-job.jar com._42six.amino.bitmap.BitmapJob /amino/numbers/out /amino/numbers/config', True],
['/opt/hadoop/bin/hadoop jar amino-bitmap-1.7.0-SNAPSHOT-job.jar com._42six.amino.bitmap.StatsJob /amino/numbers/out /amino/numbers/config', True],
['/opt/hadoop/bin/hadoop jar amino-bitmap-1.7.0-SNAPSHOT-job.jar com._42six.amino.bitmap.HypothesisJob /amino/numbers/out /amino/numbers/config /amino/numbers/working', True]],

#write some final values and swap the tables
[['/opt/hadoop/bin/hadoop jar amino-bitmap-1.7.0-SNAPSHOT-job.jar com._42six.amino.bitmap.FeatureMetadataJob /amino/numbers/config', True]]

]

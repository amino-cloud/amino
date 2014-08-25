package com._42six.amino.common;

import org.apache.hadoop.conf.Configuration;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.Properties;

public class HadoopConfigurationUtils {
	
	/**
	 * Diff two configurations, it echos the results to System.out
	 * @param fromConfig the 'golden' version of a configuration to diff against
	 * @param toConfig the variable version to diff with
	 */
	public static void diffHadoopConfigurations(final Configuration fromConfig, final Configuration toConfig) {
		
		Properties fromProps = HadoopConfigurationUtils.configuraitonToProperties(fromConfig);
		Properties toProps = HadoopConfigurationUtils.configuraitonToProperties(toConfig);
		LinkedList<String> removeList = new LinkedList<>();
		
		
		for(Object fromKey : fromProps.keySet()) {
			String key = (String)fromKey;
			if(toProps.containsKey(key)) {
				String fromValue = (String) fromProps.get(key);
				String toValue = (String) toProps.get(key);
				if(fromValue.compareTo(toValue) == 0) {
					toProps.remove(key);
					removeList.add(key);
				}
			}
		}
		
		// clean out our from
		for(final String key : removeList) {
			fromProps.remove(key);
		}
		
		System.out.println("FROM PROPERTIES!!!!!!");
		// Loop through whats left and show me the differneces
		for(Object fromKey : fromProps.keySet()) {
			System.out.println(fromKey + ":" + fromProps.get(fromKey));
		}
		
		System.out.println("TO PROPERTIES!!!!!!");
		// Loop through whats left and show me the differneces
		for(Object toKey : toProps.keySet()) {
			System.out.println(toKey + ":" + toProps.get(toKey));
		}
		
	}
	
	/**
	 * Turn a configuration into a properties object
	 * @param config configuration to convert to properties object
	 * @return a properties object representing the keys and values inside the configuration
	 */
	public static Properties configuraitonToProperties(final Configuration config) {
		Properties props = new Properties();
		
		for(Entry<String, String> entry : config) {
			props.put(entry.getKey(), entry.getValue());
		}
		
		return props;
	}
	
	
	/**
	 * Dump the configuration into the print writer
	 * @param config the configuration to dump
	 * @param writer the writer to dump to
	 */
	public static void dumpConfiguration(final Configuration config, final PrintWriter writer) {
		for(Entry<String, String> entry : config) {
			writer.println(entry.getKey() + ":" + entry.getValue());
		}
	}
	
	
	/**
	 * Dump the configuration into the print stream
	 * @param config the configuration to dump
	 * @param outputStream the stream to dump it to
	 */
	public static void dumpConfiguration(final Configuration config, final PrintStream outputStream) {
		for(Entry<String, String> entry : config) {
			outputStream.println(entry.getKey() + ":" + entry.getValue());
		}
	}
	
	/**
	 * Merge to configurations, replace everything in the original configuration with the values in the replacement configuration.
	 * @param origConf the configuration to be merged into
	 * @param replaceConf the configuration merge from
	 */
	public static void mergeConfs(Configuration origConf, Configuration replaceConf) {
		for(Entry<String, String> entry : replaceConf) {
			origConf.set(entry.getKey(), entry.getValue());
		}
	}
	
}

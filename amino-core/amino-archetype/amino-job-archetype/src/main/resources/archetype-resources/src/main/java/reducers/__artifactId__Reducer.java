#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}.reducers;

import org.apache.hadoop.io.MapWritable;

import ${package}.api.job.AminoReducer;
import ${package}.common.AminoWritable;

public class ${artifactId}Reducer implements AminoReducer {

	@Override
	public Iterable<AminoWritable> reduce(Iterable<MapWritable> daos) {
		// TODO Add the reduce step for your amino job here
		return null;
	}

}

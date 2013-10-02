#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}.job;

import java.util.LinkedList;

import ${package}.api.job.AminoJob;
import ${package}.api.job.AminoReducer;
import ${package}.api.job.JobOutputEstimate;
import ${package}.data.DataLoader;
import ${package}.reducers.${artifactId}Reducer;

public class ${artifactId}Job implements AminoJob  {

	@Override
	public JobOutputEstimate getJobEstimate() {
		// TODO Get the job estimate for this class
		return null;
	}

	@Override
	public String getJobName() {
		// TODO Return the name of the job
		return null;
	}

	@Override
	public Class<? extends DataLoader> getDataLoaderClass() {
		// TODO Return the data loader that this will use
		return null;
	}

	@Override
	public Iterable<Class<? extends AminoReducer>> getAminoReducerClasses() {
		// TODO Add and return the Reducer classes that you want run
		LinkedList<Class<? extends AminoReducer>> reducerClasses = new LinkedList<Class<? extends AminoReducer>>();
		reducerClasses.add(${artifactId}Reducer.class);
		return reducerClasses;
	}

}

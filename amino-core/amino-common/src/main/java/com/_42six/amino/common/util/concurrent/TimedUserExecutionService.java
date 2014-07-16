package com._42six.amino.common.util.concurrent;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

//import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * ExecutionService that limits both the number of concurrently running tasks  
 * and the amount of tasks per user. 
 */
@SuppressWarnings("UnusedDeclaration")
public class TimedUserExecutionService {
	
	// ************************************************************************
	// Fields
	// ************************************************************************
	
	//private static final Logger log = Logger.getLogger(TimedUserExecutionService.class);
	
	protected ListeningExecutorService service;
	protected final Map<String, AtomicInteger> currentUsageTable;
	protected 	ThreadPoolExecutor executor;
	
	// Properties of the ThreadPoolExecutor
	protected int corePoolSize = 3;
	protected int maxPoolSize = 20;
	protected long keepAlive = 10;
	protected TimeUnit keepAliveUnits = TimeUnit.MINUTES;
	protected BlockingQueue<Runnable> tpeQueue;
	
	// Timeout properties
	private long defaultTimeout = 10;
	private TimeUnit defaultTimeoutUnits = TimeUnit.MINUTES;
	private long maxTimeout = 60;
	private TimeUnit maxTimeoutUnits = TimeUnit.MINUTES; 

	final private int maxTasksPerUser; // Final because we won't want to deal with the headaches of resizing currentUsageTable counts

	// ************************************************************************
	// Getters and Setters
	// ************************************************************************
	
	public long getDefaultTimeout() {
		return defaultTimeout;
	}
	
	public TimedUserExecutionService setDefaultTimeout(long timeout) {
		this.defaultTimeout = timeout;
		return this;
		
	}
	
	public TimeUnit getDefaultTimeoutUnits() {
		return defaultTimeoutUnits;
	}

	public TimedUserExecutionService setDefaultTimeoutUnits(TimeUnit defaultTimeoutUnits) {
		this.defaultTimeoutUnits = defaultTimeoutUnits;
		return this;
	}

	public long getMaxTimeout() {
		return maxTimeout;
	}
	
	public TimedUserExecutionService setMaxTimeout(long maxTimeout) {
		this.maxTimeout = maxTimeout;
		return this;
	}
	
	public TimeUnit getMaxTimeoutUnits() {
		return maxTimeoutUnits;
	}

	public TimedUserExecutionService setMaxTimeoutUnits(TimeUnit maxTimeoutUnits) {
		this.maxTimeoutUnits = maxTimeoutUnits;
		return this;
	}
	
	public int getCorePoolSize(){
		return corePoolSize;
	}
	
	public TimedUserExecutionService setCorePoolSize(int size){
		corePoolSize = size;
		if(executor != null) { executor.setCorePoolSize(size); } 
		return this;
	}
	
	public int getMaximumPoolSize(){
		return maxPoolSize;
	}
	
	public TimedUserExecutionService setMaximumPoolSize(int size){
		maxPoolSize = size;
		if(executor != null) { executor.setMaximumPoolSize(size); } 
		return this;
	}
	
	public long getKeepAlive(TimeUnit unit){
		return keepAlive;
	}
	
	public TimedUserExecutionService setKeepAlive(long time){
		keepAlive = time;
		if(executor != null) { executor.setKeepAliveTime(time, keepAliveUnits); } 
		return this;
	}
	
	public TimeUnit getKeepAliveUnits(){
		return keepAliveUnits;
	}
	
	public TimedUserExecutionService setKeepAliveUnits(TimeUnit units){
		keepAliveUnits = units;
		if(executor != null) { executor.setKeepAliveTime(keepAlive, units);  } 
		return this;
	}
	
	// ************************************************************************
	// Constructors
	// ************************************************************************
	
	public TimedUserExecutionService(int maxTasksPerUser, int corePoolSize, int maxPoolSize, long keepAliveTime, TimeUnit units)
	{
		this.maxTasksPerUser = maxTasksPerUser;
		this.tpeQueue = new SynchronousQueue<Runnable>();
		this.executor = new ThreadPoolExecutor(corePoolSize, maxPoolSize, keepAliveTime, units, this.tpeQueue );
		this.service = MoreExecutors.listeningDecorator(this.executor);
		this.currentUsageTable = Collections.synchronizedMap(new HashMap<String, AtomicInteger>());
	}
	
	public TimedUserExecutionService(int maxTasksPerUser, int corePoolSize, int maxPoolSize, long keepAliveTime, TimeUnit units,
			long defaultTimeout, TimeUnit defaultTimeoutUnits, long maxTimeout, TimeUnit maxTimeoutUnits)
	{
		this(maxTasksPerUser, corePoolSize, maxPoolSize, keepAliveTime, units);
		this.defaultTimeout = defaultTimeout;
		this.defaultTimeoutUnits = defaultTimeoutUnits;
		this.maxTimeout = maxTimeout;
		this.maxTimeoutUnits = maxTimeoutUnits;
	}
	
	
	public TimedUserExecutionService(int maxTasksPerUser) {
		this(maxTasksPerUser, 3, 10, 10, TimeUnit.MINUTES);
	}

	// ************************************************************************
	// Public Methods
	// ************************************************************************
	
	public <T> T timedCall(Callable<T> task, TimedCallInfo info) throws RejectedExecutionException, InterruptedException, TimeoutException, ExecutionException {
		String user = Preconditions.checkNotNull(info.user);
		long timeout = (info.timeout < 0) ? defaultTimeout : info.timeout;
		TimeUnit units = (info.units == null) ? TimeUnit.MINUTES : info.units;
		
		return timedCall(task, user, timeout, units, info.mayInterruptIfRunning);
	}
	
	/**
	 * Executes the {@link Callable} for the user, timing out after the default amount of time.
	 * If the user exceeds their quota, a {@link RejectedExecutionException} will be thrown 
	 * @param task The {@link Callable} to execute
	 * @param user The name of the  user queue to run against
	 * @param mayInterruptIfRunning true if the task should be interrupted if there were any problems or the timeout is reached
	 * @return The return value of the task.  No value returned if task times out
	 * @throws InterruptedException if the task was interrupted
	 * @throws TimeoutException if task did not complete in time
	 * @throws ExecutionException if the task fails executing
	 * @throws RejectedExecutionException if the user exceeds their amount of concurrent task requests
	 */
	public <T> T timedCall(Callable<T> task, String user, boolean mayInterruptIfRunning ) 
			throws RejectedExecutionException, InterruptedException, TimeoutException, ExecutionException
	{
		return timedCall(task, user, defaultTimeout, mayInterruptIfRunning);
	}
	
	/**
	 * Executes the {@link Callable} for the user, timing out after the specified amount of time.
	 * If the user exceeds their quota, a {@link RejectedExecutionException} will be thrown 
	 * @param task The {@link Callable} to execute
	 * @param user The name of the  user queue to run against
	 * @param timeout The amount of time in minutes to wait before giving up
	 * @param mayInterruptIfRunning true if the task should be interrupted if there were any problems or the timeout is reached
	 * @return The return value of the task.  No value returned if task times out
	 * @throws InterruptedException if the task was interrupted
	 * @throws TimeoutException if task did not complete in time
	 * @throws ExecutionException if the task fails executing
	 * @throws RejectedExecutionException if the user exceeds their amount of concurrent task requests
	 */
	public <T> T timedCall(Callable<T> task, String user, long timeout,  boolean mayInterruptIfRunning) 
			throws RejectedExecutionException, InterruptedException, TimeoutException, ExecutionException
	{
		return timedCall(task, user, timeout, TimeUnit.MINUTES, mayInterruptIfRunning);
	}
	
	/**
	 * Executes the {@link Callable} for the user, timing out after the specified amount of time.
	 * If the user exceeds their quota, a {@link RejectedExecutionException} will be thrown 
	 * @param task The {@link Callable} to execute
	 * @param user The name of the  user queue to run against
	 * @param timeout The amount of time to wait before giving up
	 * @param unit The units of timeout
	 * @param mayInterruptIfRunning true if the task should be interrupted if there were any problems or the timeout is reached
	 * @return The return value of the task.  No value returned if task times out
	 * @throws InterruptedException if the task was interrupted
	 * @throws TimeoutException if task did not complete in time
	 * @throws ExecutionException if the task fails executing
	 * @throws RejectedExecutionException if the user exceeds their amount of concurrent task requests
	 */
	public <T> T timedCall(Callable<T> task, String user, long timeout, TimeUnit unit, boolean mayInterruptIfRunning) 
			throws InterruptedException, TimeoutException, ExecutionException, RejectedExecutionException
	{
		Preconditions.checkNotNull(task, "Task was null");
		Preconditions.checkNotNull(user, "Must provider user for queue");
		
		ListenableFuture<T> futureTask;
		
		// Attempt to add a task to the queue
		synchronized (currentUsageTable) {
			// If it's there, increment it
			if(currentUsageTable.containsKey(user)){
				if(currentUsageTable.get(user).incrementAndGet() > maxTasksPerUser){
					currentUsageTable.get(user).decrementAndGet();
					throw new RejectedExecutionException(String.format("User has already hit maximum number of concurrent tasks - %d", maxTasksPerUser));
				}				
			}
			else {
				// Need to create new counter
				currentUsageTable.put(user, new AtomicInteger(1));
			}
		}
				
		// Schedule the call the run
		try{
			futureTask = getService().submit(task);
		} catch(RejectedExecutionException  e){
			decrementUserCount(user);
			throw new RejectedExecutionException("Maximum number of concurrent queries reached", e);
		}
		
		// Add a callback for cleaning up the user queues when the task is terminated
		Futures.addCallback(futureTask, new UserFutureCallback<T>(user));

		// Get the result
		T result;
		long time;
		TimeUnit timeUnit;
		try{
			// Make sure timeout value is valid
			if(timeout <= 0){
				time = defaultTimeout;
				timeUnit = defaultTimeoutUnits;
			} else if(unit.toMillis(timeout) > maxTimeoutUnits.toMillis(maxTimeout)){
				time = maxTimeout;
				timeUnit = maxTimeoutUnits;
			} else {
				time = timeout;
				timeUnit = unit;
			}
			
			result = futureTask.get(time, timeUnit);
		} catch(InterruptedException ex){
			futureTask.cancel(mayInterruptIfRunning); 
			throw ex;
		} catch(TimeoutException ex){
			futureTask.cancel(mayInterruptIfRunning); 
			throw new TimeoutException("The Task could not be completed before the timeout of " + Long.toString(timeout) + " " + unit.toString());
		} catch (ExecutionException ex) {
			futureTask.cancel(mayInterruptIfRunning);
			throw ex;
		}

		return result;		
	}

	/**
	 * Returns statistics on the state of the user queues and the {@link ThreadPoolExecutor}
	 * @return Formated string of stats
	 */
	public String getStats(){
		final StringBuilder sb = new StringBuilder();
		final ThreadPoolExecutor tpe = getExecutor();
		sb.append(String.format("Tasks run since start: %d | Completed TaskCount: %d | Active Count: %d | Pool Size: %d | Largest Pool Size: %d, Max Pool Size: %d\n", 
				tpe.getTaskCount(),
				tpe.getCompletedTaskCount(),
				tpe.getActiveCount(),
				tpe.getPoolSize(),
				tpe.getLargestPoolSize(),
				tpe.getMaximumPoolSize()
				)
		);
		
		for(Entry<String, AtomicInteger> e : currentUsageTable.entrySet()){
			sb.append(String.format("User: %s | Max: %d | Active: %d\n", e.getKey(), maxTasksPerUser, e.getValue().get()));
		}
		
		return sb.toString();
	}
	
	// ************************************************************************
	// Protected and private methods
	// ************************************************************************
	
	protected ThreadPoolExecutor getExecutor(){
		if(executor == null){
			executor = new ThreadPoolExecutor(corePoolSize, maxPoolSize, keepAlive, keepAliveUnits, tpeQueue);
		}
		return executor;
	}
	
	protected ListeningExecutorService getService(){
		if(service == null){
			service = MoreExecutors.listeningDecorator(getExecutor());
		}
		return service;
	}
	
	/**
	 * Reduces the count of of the user queue by one, removing it from the map if there are no more
	 * tasks being fun for the user
	 * @param user The user queue to update
	 */
	protected void decrementUserCount(String user){
		// Make sure everything is on the up and up
		synchronized(currentUsageTable){
			if(!currentUsageTable.containsKey(user)){
				throw new RuntimeException("The user LUT doesn't have any records for user: " + user);
			}
			
			// Decrement the count now that we are done. If we are back to 0,
			// clean up the table so we don't have stuff lingering around taking up memory
			if(currentUsageTable.get(user).decrementAndGet() == 0){
				currentUsageTable.remove(user);
			}
		}
	}
	
	// ************************************************************************
	// Inner classes
	// ************************************************************************
	
	/**
	 * Simple class for keeping track of what user is associated with a task and cleans up the associated
	 * user queue when the task is done
	 * @param <T> The type of the result of the call
	 */
	class UserFutureCallback<T> implements FutureCallback<T> {
		private String user;
		public UserFutureCallback(String user) {
			this.user = user;
		}

		@Override
		public void onSuccess(T result) {
			decrementUserCount(user);
		}

		@Override
		public void onFailure(Throwable t) {
			decrementUserCount(user);	
		}		
	}
	
}

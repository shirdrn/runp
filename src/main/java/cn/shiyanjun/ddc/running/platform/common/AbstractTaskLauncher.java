package cn.shiyanjun.ddc.running.platform.common;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.shiyanjun.ddc.api.Context;
import cn.shiyanjun.ddc.api.Task;
import cn.shiyanjun.ddc.api.TaskResult;
import cn.shiyanjun.ddc.api.common.AbstractComponent;
import cn.shiyanjun.ddc.api.constants.TaskStatus;
import cn.shiyanjun.ddc.api.utils.NamedThreadFactory;
import cn.shiyanjun.ddc.running.platform.api.TaskLauncher;

public abstract class AbstractTaskLauncher extends AbstractComponent implements TaskLauncher {

	private static final Log LOG = LogFactory.getLog(AbstractTaskLauncher.class);
	private int type;
	private final ConcurrentMap<Integer, RunningTask> waitingTasks = new ConcurrentHashMap<>();
	private final ConcurrentMap<Integer, RunningTask> runningTasks = new ConcurrentHashMap<>();
	private final ConcurrentMap<Integer, RunningTask> completedTasks = new ConcurrentHashMap<>();
	private final ConcurrentMap<Integer, Future<TaskResult>> taskResultFutures = new ConcurrentHashMap<>();
	private ExecutorService taskExecutorService;
	private ExecutorService workerExecutorService;
	private final Lock lock = new ReentrantLock();
	private volatile boolean running = true;
	private final int maxConcurrentRunningTaskCount;
	private final Object signalKickOffTaskLock = new Object();
	
	public AbstractTaskLauncher(Context context) {
		super(context);
		maxConcurrentRunningTaskCount = context.getInt("", 3);
	}
	
	@Override
	public void start() {
		taskExecutorService = Executors.newCachedThreadPool(new NamedThreadFactory("TASK"));
		workerExecutorService = Executors.newFixedThreadPool(2, new NamedThreadFactory("RESULT"));
		workerExecutorService.execute(new TaskScheduler());
		workerExecutorService.execute(new TaskResultMonitor());
	}
	
	@Override
	public void stop() {
		taskExecutorService.shutdown();
		running = false;
		workerExecutorService.shutdown();
	}

	@Override
	public void setType(int type) {
		this.type = type;		
	}
	
	@Override
	public int getType() {
		return type;
	}

	@Override
	public void launchTask(int taskId) {
		lock.lock();
		Task task = null;
		final RunningTask rTask = new RunningTask();
		try {
			task = createTask(taskId);
			if(runningTasks.size() >= maxConcurrentRunningTaskCount) {
				rTask.setTask(task);
				long now = System.currentTimeMillis();
				rTask.setStartTime(now);
				rTask.updateLastActiveTime(now);
				waitingTasks.putIfAbsent(taskId, rTask);
				LOG.info("Task waiting: taskId=" + taskId);
			} else {
				submitTask(taskId, rTask);
			}
		} catch(Exception e) {
			LOG.error("Fail to launch task: taskId=" + taskId + ", task=" + task);
		} finally {
			lock.unlock();
		}
	}

	private void submitTask(int taskId, RunningTask rTask) {
		runningTasks.putIfAbsent(taskId, rTask);
		Future<TaskResult> f = taskExecutorService.submit(new TaskRunner(rTask.getTask()));
		taskResultFutures.putIfAbsent(taskId, f);
		LOG.info("Task launched: " + rTask.getTask());
	}

	/**
	 * Execute a {@link Task} instance.
	 * @param task
	 * @return
	 */
	protected abstract TaskResult runTask(Task task);

	/**
	 * Create a {@link Task} instance from the given task ID.
	 * @param taskId
	 * @return
	 */
	protected abstract Task createTask(int taskId);
	
	private final class TaskRunner implements Callable<TaskResult> {
		
		private final Task task;
		
		public TaskRunner(Task task) {
			this.task = task;
		}
		
		@Override
		public TaskResult call() throws Exception {
			return runTask(task);
		}
	}
	
	private final class TaskScheduler extends Thread {
		
		@Override
		public void run() {
			while(running) {
				try {
					synchronized(signalKickOffTaskLock) {
						if(runningTasks.size() >= maxConcurrentRunningTaskCount) {
							signalKickOffTaskLock.wait();
						}
						if(!waitingTasks.isEmpty()) {
							Iterator<Integer> iter = waitingTasks.keySet().iterator();
							int taskId = iter.next();
							RunningTask rTask = waitingTasks.get(taskId);
							iter.remove();
							submitTask(taskId, rTask);
						}
					}
					
					Thread.sleep(3000);
				} catch (Exception e) {
					LOG.warn("", e);
				}
			}
		}
	}
	
	private final class TaskResultMonitor extends Thread {
		
		private final int checkTaskResultIntervalMillis;
		private final int fetchTaskResultTimeoutMillis;
		
		public TaskResultMonitor() {
			final Context ctx = context;
			checkTaskResultIntervalMillis = ctx.getInt("", 2000);
			fetchTaskResultTimeoutMillis = ctx.getInt("", 5000);
		}
		
		@Override
		public void run() {
			while(running) {
				try {
					while(!taskResultFutures.isEmpty()) {
						final Iterator<Map.Entry<Integer,Future<TaskResult>>> iter = taskResultFutures.entrySet().iterator();
						while(iter.hasNext()) {
							Map.Entry<Integer,Future<TaskResult>> entry = iter.next();
							final int taskId = entry.getKey();
							final Future<TaskResult> f = entry.getValue();
							try {
								if(!f.isDone() && !f.isCancelled()) {
									boolean isAlive = doTaskAlive(taskId, f);
									if(!isAlive) {
										iter.remove();
										// notify to schedule next waiting task
										scheduleNextTask();
									}
									continue;
								}
								
								// task is done
								if(f.isDone()) {
									doTaskCompletion(taskId, f.get());
									iter.remove();
									scheduleNextTask();
								}
							} catch (Exception e) {
								LOG.error("Fail to fetch task result: taskId=" + taskId);
								doTaskFailure(taskId, e);
								iter.remove();
								scheduleNextTask();
							}
						}
					}
					Thread.sleep(checkTaskResultIntervalMillis);
				} catch (InterruptedException e) {
				} catch (Exception e) {
					LOG.warn("", e);
				}
			}
		}

		private void scheduleNextTask() {
			if(runningTasks.size() < maxConcurrentRunningTaskCount) {
				synchronized(signalKickOffTaskLock) {
					signalKickOffTaskLock.notify();
				}
			}
		}

		private void doTaskFailure(int taskId, Exception cause) {
			RunningTask rTask = runningTasks.remove(taskId);
			long doneTime = System.currentTimeMillis();
			rTask.updateLastActiveTime(doneTime);
			rTask.setDoneTime(doneTime);
			rTask.getTask().setStatus(TaskStatus.FAILED);
			rTask.setCause(cause);
			completedTasks.put(taskId, rTask);
		}

		private void doTaskCompletion(int taskId, TaskResult taskResult) {
			RunningTask rTask = runningTasks.remove(taskId);
			long doneTime = System.currentTimeMillis();
			rTask.updateLastActiveTime(doneTime);
			rTask.setDoneTime(doneTime);
			if(taskResult != null) {
				rTask.getTask().setStatus(TaskStatus.SUCCEEDED);
			} else {
				rTask.getTask().setStatus(TaskStatus.FAILED);
			}
			completedTasks.put(taskId, rTask);
		}

		private boolean doTaskAlive(int taskId, Future<TaskResult> f) throws InterruptedException, ExecutionException {
			TaskResult result = null;
			try {
				result = f.get(fetchTaskResultTimeoutMillis, TimeUnit.MILLISECONDS);
			} catch (TimeoutException e) {
				if(runningTasks.containsKey(taskId)) {
					RunningTask rTask = runningTasks.get(taskId);
					rTask.updateLastActiveTime(System.currentTimeMillis());
				} else {
					LOG.fatal("Task summitted, but not in running task queue: taskId=" + taskId);
				}
				return true;
			}
			
			// if task has already completed
			if(result != null) {
				doTaskCompletion(taskId, result);
			}
			return false;
		}
	}

}

package cn.shiyanjun.ddc.running.platform.common;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import cn.shiyanjun.ddc.api.Context;
import cn.shiyanjun.ddc.api.common.AbstractComponent;
import cn.shiyanjun.ddc.api.constants.TaskStatus;
import cn.shiyanjun.ddc.api.utils.NamedThreadFactory;
import cn.shiyanjun.ddc.network.common.PeerMessage;
import cn.shiyanjun.ddc.network.common.RpcMessage;
import cn.shiyanjun.ddc.network.common.RunnableMessageListener;
import cn.shiyanjun.ddc.running.platform.api.Task;
import cn.shiyanjun.ddc.running.platform.api.TaskLauncher;
import cn.shiyanjun.ddc.running.platform.api.TaskResult;
import cn.shiyanjun.ddc.running.platform.constants.JsonKeys;
import cn.shiyanjun.ddc.running.platform.constants.MessageType;
import cn.shiyanjun.ddc.running.platform.utils.Time;

public abstract class AbstractTaskLauncher extends AbstractComponent implements TaskLauncher {

	private static final Log LOG = LogFactory.getLog(AbstractTaskLauncher.class);
	private int type;
	private final ConcurrentMap<Long, RunningTask> waitingTasks = Maps.newConcurrentMap();
	private final ConcurrentMap<Long, RunningTask> runningTasks = Maps.newConcurrentMap();
	private final ConcurrentMap<Long, RunningTask> completedTasks = Maps.newConcurrentMap();
	private final ConcurrentMap<Long, Future<TaskResult>> taskResultFutures = Maps.newConcurrentMap();
	private ExecutorService launcherExecutorService;
	private final Lock lock = new ReentrantLock();
	private volatile boolean running = true;
	private final int maxConcurrentRunningTaskCount;
	private final Object signalKickOffTaskLock = new Object();
	private RunnableMessageListener<PeerMessage> taskProgressReporter;
	private ScheduledExecutorService scheduledExecutorService;
	
	public AbstractTaskLauncher(Context context) {
		super(context);
		maxConcurrentRunningTaskCount = context.getInt("", 3);
	}
	
	@Override
	public void start() {
		Preconditions.checkArgument(taskProgressReporter != null, "Task progress reporter not set!");
		launcherExecutorService = Executors.newCachedThreadPool(new NamedThreadFactory("LAUNCHER"));
		launcherExecutorService.execute(new TaskScheduler());
		launcherExecutorService.execute(new TaskResultMonitor());
		
		scheduledExecutorService = Executors.newScheduledThreadPool(1, new NamedThreadFactory("RESULT-REPORTER"));
		int period = 10000;
		scheduledExecutorService.scheduleAtFixedRate(() -> reportTaskProgress(), 5000, period, TimeUnit.MILLISECONDS);
	}
	
	@Override
	public void setTaskProgressReporter(RunnableMessageListener<PeerMessage> taskProgressReporter) {
		this.taskProgressReporter = taskProgressReporter;		
	}
	
	@Override
	public void stop() {
		launcherExecutorService.shutdown();
		running = false;
	}
	
	private void reportTaskProgress() {
		Iterator<Entry<Long, RunningTask>> completedTasksIter = completedTasks.entrySet().iterator();
		while(completedTasksIter.hasNext()) {
			Entry<Long, RunningTask> entry = completedTasksIter.next();
			PeerMessage message = createTaskProgressMessage(entry.getKey(), entry.getValue().task.getTaskStatus());
			taskProgressReporter.addMessage(message);
			completedTasksIter.remove();
		}
		
		checkNonConpletedQueues(runningTasks);
		checkNonConpletedQueues(waitingTasks);
	}
	
	private void checkNonConpletedQueues(ConcurrentMap<Long, RunningTask> q) {
		Iterator<Entry<Long, RunningTask>> iter = q.entrySet().iterator();
		while(iter.hasNext()) {
			Entry<Long, RunningTask> entry = iter.next();
			PeerMessage message = createTaskProgressMessage(entry.getKey(), TaskStatus.RUNNING);
			taskProgressReporter.addMessage(message);
		}
	}
	
	private PeerMessage createTaskProgressMessage(long taskId, TaskStatus taskStatus) {
		PeerMessage peerMessage = new PeerMessage();
		
		RpcMessage rpcMessage = new RpcMessage();
		rpcMessage.setId(Time.now());
		rpcMessage.setType(MessageType.TASK_PROGRESS.getCode());
		JSONObject body = new JSONObject(true);
		body.put(JsonKeys.TASK_ID, taskId);
		body.put(JsonKeys.TASK_STATUS, taskStatus.toString());
		rpcMessage.setBody(body.toString());
		rpcMessage.setTimestamp(Time.now());
		
		peerMessage.setRpcMessage(rpcMessage);
		return peerMessage;
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
	public void launchTask(long taskId, JSONObject params) {
		// report to master when a task was received to launch
		PeerMessage message = createTaskProgressMessage(taskId, TaskStatus.RUNNING);
		taskProgressReporter.addMessage(message);
		
		lock.lock();
		Task task = null;
		final RunningTask rTask = new RunningTask();
		try {
			task = createTask(params);
			task.setId(taskId);
			task.setType(type);
			rTask.task = task;
			long now = Time.now();
			rTask.lastActiveTime = now;
			if(runningTasks.size() >= maxConcurrentRunningTaskCount) {
				task.setTaskStatus(TaskStatus.QUEUEING);
				waitingTasks.putIfAbsent(taskId, rTask);
				LOG.info("Task waiting: taskId=" + taskId);
			} else {
				task.setTaskStatus(TaskStatus.RUNNING);
				rTask.startTime = now;
				submitTask(taskId, rTask);
				LOG.info("Task submitted: " + rTask);
			}
		} catch(Exception e) {
			LOG.error("Fail to launch task: taskId=" + taskId + ", task=" + task, e);
		} finally {
			lock.unlock();
		}
	}

	private void submitTask(long taskId, RunningTask rTask) {
		runningTasks.putIfAbsent(taskId, rTask);
		Future<TaskResult> f = launcherExecutorService.submit(new TaskRunner(rTask.task));
		taskResultFutures.putIfAbsent(taskId, f);
	}

	/**
	 * Execute a {@link Task} instance.
	 * @param task
	 * @return
	 */
	protected abstract TaskResult runTask(Task task);

	/**
	 * Create a {@link Task} instance from the given task parameters.
	 * @param taskId
	 * @return
	 */
	protected abstract Task createTask(JSONObject params);
	
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
							Iterator<Long> iter = waitingTasks.keySet().iterator();
							long taskId = iter.next();
							RunningTask rTask = waitingTasks.get(taskId);
							rTask.startTime = Time.now();
							iter.remove();
							submitTask(taskId, rTask);
							LOG.info("Task submitted: " + rTask);
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
						final Iterator<Map.Entry<Long, Future<TaskResult>>> iter = taskResultFutures.entrySet().iterator();
						while(iter.hasNext()) {
							Map.Entry<Long, Future<TaskResult>> entry = iter.next();
							final long taskId = entry.getKey();
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
								LOG.error("Fail to fetch task result: taskId=" + taskId, e);
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

		private void doTaskFailure(long taskId, Exception cause) {
			RunningTask rTask = runningTasks.remove(taskId);
			long doneTime = Time.now();
			rTask.lastActiveTime = doneTime;
			rTask.doneTime = doneTime;
			rTask.task.setTaskStatus(TaskStatus.FAILED);
			rTask.cause = cause;
			completedTasks.put(taskId, rTask);
		}

		private void doTaskCompletion(long taskId, TaskResult taskResult) {
			RunningTask rTask = runningTasks.remove(taskId);
			long doneTime = Time.now();
			rTask.lastActiveTime = doneTime;
			rTask.doneTime = doneTime;
			if(taskResult != null) {
				rTask.task.setTaskStatus(TaskStatus.SUCCEEDED);
			} else {
				rTask.task.setTaskStatus(TaskStatus.FAILED);
			}
			completedTasks.put(taskId, rTask);
			LOG.info("Task completed: " + rTask);
		}

		private boolean doTaskAlive(long taskId, Future<TaskResult> f) throws InterruptedException, ExecutionException {
			TaskResult result = null;
			try {
				result = f.get(fetchTaskResultTimeoutMillis, TimeUnit.MILLISECONDS);
			} catch (TimeoutException e) {
				if(runningTasks.containsKey(taskId)) {
					RunningTask rTask = runningTasks.get(taskId);
					rTask.lastActiveTime = Time.now();
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
	
	class RunningTask {

		Task task;
		long startTime;
		long doneTime;
		long lastActiveTime;
		Exception cause;
		
		@Override
		public String toString() {
			return new StringBuffer()
					.append("taskId=" + task.getId())
					.append(", taskType=" + task.getType())
					.append(", taskStatus=" + task.getTaskStatus())
					.append(", startTime=" + Time.readableTime(startTime))
					.append(", lastActiveTime=" + Time.readableTime(lastActiveTime))
					.append(", doneTime=").append(doneTime == 0L ? "" : Time.readableTime(doneTime))
					.toString();
		}

	}

}

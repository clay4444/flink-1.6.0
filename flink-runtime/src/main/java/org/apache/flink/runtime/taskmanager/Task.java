/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.taskmanager;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.fs.FileSystemSafetyNet;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.SafetyNetCloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.accumulators.AccumulatorRegistry;
import org.apache.flink.runtime.blob.BlobCacheService;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.decline.CheckpointDeclineTaskNotReadyException;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.filecache.FileCache;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.NetworkEnvironment;
import org.apache.flink.runtime.io.network.netty.PartitionProducerStateChecker;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionConsumableNotifier;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionMetrics;
import org.apache.flink.runtime.io.network.partition.consumer.InputGateMetrics;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.jobgraph.tasks.StoppableTask;
import org.apache.flink.runtime.jobmanager.PartitionProducerDisposedException;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.TaskStateManager;
import org.apache.flink.runtime.util.FatalExitExceptionHandler;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.WrappingRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The Task represents one execution of a parallel subtask on a TaskManager.
 * A Task wraps a Flink operator (which may be a user function) and
 * runs it, providing all services necessary for example to consume input data,
 * produce its results (intermediate result partitions) and communicate
 * with the JobManager.
 *
 * <p>The Flink operators (implemented as subclasses of
 * {@link AbstractInvokable} have only data readers, -writers, and certain event callbacks.
 * The task connects those to the network stack and actor messages, and tracks the state
 * of the execution and handles exceptions.
 *
 * <p>Tasks have no knowledge about how they relate to other tasks, or whether they
 * are the first attempt to execute the task, or a repeated attempt. All of that
 * is only known to the JobManager. All the task knows are its own runnable code,
 * the task's configuration, and the IDs of the intermediate results to consume and
 * produce (if any).
 *
 * <p>Each Task is run by one dedicated thread.
 *
 * Task 实现了 Runnable 接口，每个 Task 都会在一个单独的线程中运行
 */
public class Task implements Runnable, TaskActions, CheckpointListener {

	/** The class logger. */
	private static final Logger LOG = LoggerFactory.getLogger(Task.class);

	/** The tread group that contains all task threads. */
	private static final ThreadGroup TASK_THREADS_GROUP = new ThreadGroup("Flink Task Threads");

	/** For atomic state updates. */
	private static final AtomicReferenceFieldUpdater<Task, ExecutionState> STATE_UPDATER =
			AtomicReferenceFieldUpdater.newUpdater(Task.class, ExecutionState.class, "executionState");

	// ------------------------------------------------------------------------
	//  Constant fields that are part of the initial Task construction
	// ------------------------------------------------------------------------

	/** The job that the task belongs to. */
	private final JobID jobId;

	/** The vertex in the JobGraph whose code the task executes. */
	private final JobVertexID vertexId;

	/** The execution attempt of the parallel subtask. */
	private final ExecutionAttemptID executionId;

	/** ID which identifies the slot in which the task is supposed to run. */
	private final AllocationID allocationId;

	/** TaskInfo object for this task. */
	private final TaskInfo taskInfo;

	/** The name of the task, including subtask indexes. */
	private final String taskNameWithSubtask;

	/** The job-wide configuration object. */
	private final Configuration jobConfiguration;

	/** The task-specific configuration. */
	private final Configuration taskConfiguration;

	/** The jar files used by this task. */
	private final Collection<PermanentBlobKey> requiredJarFiles;

	/** The classpaths used by this task. */
	private final Collection<URL> requiredClasspaths;

	/** The name of the class that holds the invokable code. */
	private final String nameOfInvokableClass;

	/** Access to task manager configuration and host names. */
	private final TaskManagerRuntimeInfo taskManagerConfig;

	/** The memory manager to be used by this task. */
	private final MemoryManager memoryManager;

	/** The I/O manager to be used by this task. */
	private final IOManager ioManager;

	/** The BroadcastVariableManager to be used by this task. */
	private final BroadcastVariableManager broadcastVariableManager;

	/** The manager for state of operators running in this task/slot. */
	private final TaskStateManager taskStateManager;

	/** Serialized version of the job specific execution configuration (see {@link ExecutionConfig}). */
	private final SerializedValue<ExecutionConfig> serializedExecutionConfig;

	private final ResultPartition[] producedPartitions;

	private final SingleInputGate[] inputGates;

	private final Map<IntermediateDataSetID, SingleInputGate> inputGatesById;

	/** Connection to the task manager. */
	private final TaskManagerActions taskManagerActions;

	/** Input split provider for the task. */
	private final InputSplitProvider inputSplitProvider;

	/** Checkpoint notifier used to communicate with the CheckpointCoordinator. */
	private final CheckpointResponder checkpointResponder;

	/** All listener that want to be notified about changes in the task's execution state. */
	private final List<TaskExecutionStateListener> taskExecutionStateListeners;

	/** The BLOB cache, from which the task can request BLOB files. */
	private final BlobCacheService blobService;

	/** The library cache, from which the task can request its class loader. */
	private final LibraryCacheManager libraryCache;

	/** The cache for user-defined files that the invokable requires. */
	private final FileCache fileCache;

	/** The gateway to the network stack, which handles inputs and produced results. */
	private final NetworkEnvironment network;

	/** The registry of this task which enables live reporting of accumulators. */
	private final AccumulatorRegistry accumulatorRegistry;

	/** The thread that executes the task. */
	private final Thread executingThread;

	/** Parent group for all metrics of this task. */
	private final TaskMetricGroup metrics;

	/** Partition producer state checker to request partition states from. */
	private final PartitionProducerStateChecker partitionProducerStateChecker;

	/** Executor to run future callbacks. */
	private final Executor executor;

	// ------------------------------------------------------------------------
	//  Fields that control the task execution. All these fields are volatile
	//  (which means that they introduce memory barriers), to establish
	//  proper happens-before semantics on parallel modification
	// ------------------------------------------------------------------------

	/** atomic flag that makes sure the invokable is canceled exactly once upon error. */
	private final AtomicBoolean invokableHasBeenCanceled;

	/** The invokable of this task, if initialized. All accesses must copy the reference and
	 * check for null, as this field is cleared as part of the disposal logic. */
	@Nullable
	private volatile AbstractInvokable invokable;  	//这个Task对应的任务类型， 哪种 StreamTask

	/** The current execution state of the task. */
	private volatile ExecutionState executionState = ExecutionState.CREATED;

	/** The observed exception, in case the task execution failed. */
	private volatile Throwable failureCause;

	/** Serial executor for asynchronous calls (checkpoints, etc), lazily initialized. */
	private volatile ExecutorService asyncCallDispatcher;

	/** Initialized from the Flink configuration. May also be set at the ExecutionConfig */
	private long taskCancellationInterval;

	/** Initialized from the Flink configuration. May also be set at the ExecutionConfig */
	private long taskCancellationTimeout;

	/**
	 * This class loader should be set as the context class loader of the threads in
	 * {@link #asyncCallDispatcher} because user code may dynamically load classes in all callbacks.
	 */
	private ClassLoader userCodeClassLoader;

	/**
	 * <p><b>IMPORTANT:</b> This constructor may not start any work that would need to
	 * be undone in the case of a failing task deployment.</p>
	 */

	// 创建 task
	public Task(
		JobInformation jobInformation,
		TaskInformation taskInformation,
		ExecutionAttemptID executionAttemptID,
		AllocationID slotAllocationId,
		int subtaskIndex,
		int attemptNumber,
		Collection<ResultPartitionDeploymentDescriptor> resultPartitionDeploymentDescriptors,
		Collection<InputGateDeploymentDescriptor> inputGateDeploymentDescriptors,
		int targetSlotNumber,
		MemoryManager memManager,
		IOManager ioManager,
		NetworkEnvironment networkEnvironment,
		BroadcastVariableManager bcVarManager,
		TaskStateManager taskStateManager,
		TaskManagerActions taskManagerActions,
		InputSplitProvider inputSplitProvider,
		CheckpointResponder checkpointResponder,
		BlobCacheService blobService,
		LibraryCacheManager libraryCache,
		FileCache fileCache,
		TaskManagerRuntimeInfo taskManagerConfig,
		@Nonnull TaskMetricGroup metricGroup,
		ResultPartitionConsumableNotifier resultPartitionConsumableNotifier,
		PartitionProducerStateChecker partitionProducerStateChecker,
		Executor executor) {

		Preconditions.checkNotNull(jobInformation);
		Preconditions.checkNotNull(taskInformation);

		Preconditions.checkArgument(0 <= subtaskIndex, "The subtask index must be positive.");
		Preconditions.checkArgument(0 <= attemptNumber, "The attempt number must be positive.");
		Preconditions.checkArgument(0 <= targetSlotNumber, "The target slot number must be positive.");

		this.taskInfo = new TaskInfo(
				taskInformation.getTaskName(),
				taskInformation.getMaxNumberOfSubtaks(),
				subtaskIndex,
				taskInformation.getNumberOfSubtasks(),
				attemptNumber,
				String.valueOf(slotAllocationId));

		// 第一步是把构造函数里的这些变量赋值给当前task的fields。
		this.jobId = jobInformation.getJobId();
		this.vertexId = taskInformation.getJobVertexId();
		this.executionId  = Preconditions.checkNotNull(executionAttemptID);
		this.allocationId = Preconditions.checkNotNull(slotAllocationId);
		this.taskNameWithSubtask = taskInfo.getTaskNameWithSubtasks();
		this.jobConfiguration = jobInformation.getJobConfiguration();
		this.taskConfiguration = taskInformation.getTaskConfiguration();
		this.requiredJarFiles = jobInformation.getRequiredJarFileBlobKeys();
		this.requiredClasspaths = jobInformation.getRequiredClasspathURLs();
		this.nameOfInvokableClass = taskInformation.getInvokableClassName();
		this.serializedExecutionConfig = jobInformation.getSerializedExecutionConfig();

		Configuration tmConfig = taskManagerConfig.getConfiguration();
		this.taskCancellationInterval = tmConfig.getLong(TaskManagerOptions.TASK_CANCELLATION_INTERVAL);
		this.taskCancellationTimeout = tmConfig.getLong(TaskManagerOptions.TASK_CANCELLATION_TIMEOUT);

		this.memoryManager = Preconditions.checkNotNull(memManager);
		this.ioManager = Preconditions.checkNotNull(ioManager);
		this.broadcastVariableManager = Preconditions.checkNotNull(bcVarManager);
		this.taskStateManager = Preconditions.checkNotNull(taskStateManager);
		this.accumulatorRegistry = new AccumulatorRegistry(jobId, executionId);

		this.inputSplitProvider = Preconditions.checkNotNull(inputSplitProvider);
		this.checkpointResponder = Preconditions.checkNotNull(checkpointResponder);
		this.taskManagerActions = checkNotNull(taskManagerActions);

		this.blobService = Preconditions.checkNotNull(blobService);
		this.libraryCache = Preconditions.checkNotNull(libraryCache);
		this.fileCache = Preconditions.checkNotNull(fileCache);
		this.network = Preconditions.checkNotNull(networkEnvironment);
		this.taskManagerConfig = Preconditions.checkNotNull(taskManagerConfig);

		this.taskExecutionStateListeners = new CopyOnWriteArrayList<>();
		this.metrics = metricGroup;

		this.partitionProducerStateChecker = Preconditions.checkNotNull(partitionProducerStateChecker);
		this.executor = Preconditions.checkNotNull(executor);

		// create the reader and writer structures

		final String taskNameWithSubtaskAndId = taskNameWithSubtask + " (" + executionId + ')';

		// Produced intermediate result partitions
		this.producedPartitions = new ResultPartition[resultPartitionDeploymentDescriptors.size()];

		int counter = 0;

		// 接下来是初始化ResultPartition和InputGate。这两个类描述了task的输出数据和输入数据。

		for (ResultPartitionDeploymentDescriptor desc: resultPartitionDeploymentDescriptors) {
			ResultPartitionID partitionId = new ResultPartitionID(desc.getPartitionId(), executionId);

			this.producedPartitions[counter] = new ResultPartition(
				taskNameWithSubtaskAndId,
				this,
				jobId,
				partitionId,
				desc.getPartitionType(),
				desc.getNumberOfSubpartitions(),
				desc.getMaxParallelism(),
				networkEnvironment.getResultPartitionManager(),
				resultPartitionConsumableNotifier,
				ioManager,
				desc.sendScheduleOrUpdateConsumersMessage());

			++counter;
		}

		// Consumed intermediate result partitions
		this.inputGates = new SingleInputGate[inputGateDeploymentDescriptors.size()];
		this.inputGatesById = new HashMap<>();

		counter = 0;

		for (InputGateDeploymentDescriptor inputGateDeploymentDescriptor: inputGateDeploymentDescriptors) {
			SingleInputGate gate = SingleInputGate.create(
				taskNameWithSubtaskAndId,
				jobId,
				executionId,
				inputGateDeploymentDescriptor,
				networkEnvironment,
				this,
				metricGroup.getIOMetricGroup());

			inputGates[counter] = gate;
			inputGatesById.put(gate.getConsumedResultId(), gate);

			++counter;
		}

		invokableHasBeenCanceled = new AtomicBoolean(false);

		// finally, create the executing thread, but do not start it
		// 最后，创建一个Thread对象，并把自己放进该对象，这样在执行时，自己就有了自身的线程的引用。

		executingThread = new Thread(TASK_THREADS_GROUP, this, taskNameWithSubtask);   //执行的时候，就是执行的这个Thread
	}

	// ------------------------------------------------------------------------
	//  Accessors
	// ------------------------------------------------------------------------

	public JobID getJobID() {
		return jobId;
	}

	public JobVertexID getJobVertexId() {
		return vertexId;
	}

	public ExecutionAttemptID getExecutionId() {
		return executionId;
	}

	public AllocationID getAllocationId() {
		return allocationId;
	}

	public TaskInfo getTaskInfo() {
		return taskInfo;
	}

	public Configuration getJobConfiguration() {
		return jobConfiguration;
	}

	public Configuration getTaskConfiguration() {
		return this.taskConfiguration;
	}

	public SingleInputGate[] getAllInputGates() {
		return inputGates;
	}

	public ResultPartition[] getProducedPartitions() {
		return producedPartitions;
	}

	public SingleInputGate getInputGateById(IntermediateDataSetID id) {
		return inputGatesById.get(id);
	}

	public AccumulatorRegistry getAccumulatorRegistry() {
		return accumulatorRegistry;
	}

	public TaskMetricGroup getMetricGroup() {
		return metrics;
	}

	public Thread getExecutingThread() {
		return executingThread;
	}

	@VisibleForTesting
	long getTaskCancellationInterval() {
		return taskCancellationInterval;
	}

	@VisibleForTesting
	long getTaskCancellationTimeout() {
		return taskCancellationTimeout;
	}

	@Nullable
	@VisibleForTesting
	AbstractInvokable getInvokable() {
		return invokable;
	}

	// ------------------------------------------------------------------------
	//  Task Execution
	// ------------------------------------------------------------------------

	/**
	 * Returns the current execution state of the task.
	 * @return The current execution state of the task.
	 */
	public ExecutionState getExecutionState() {
		return this.executionState;
	}

	/**
	 * Checks whether the task has failed, is canceled, or is being canceled at the moment.
	 * @return True is the task in state FAILED, CANCELING, or CANCELED, false otherwise.
	 */
	public boolean isCanceledOrFailed() {
		return executionState == ExecutionState.CANCELING ||
				executionState == ExecutionState.CANCELED ||
				executionState == ExecutionState.FAILED;
	}

	/**
	 * If the task has failed, this method gets the exception that caused this task to fail.
	 * Otherwise this method returns null.
	 *
	 * @return The exception that caused the task to fail, or null, if the task has not failed.
	 */
	public Throwable getFailureCause() {
		return failureCause;
	}

	/**
	 * Starts the task's thread.
	 */
	public void startTaskThread() {
		executingThread.start();
	}

	/**
	 * The core work method that bootstraps the task and executes its code.
	 * Task对象本身就是一个Runable，因此在其run方法里定义了运行逻辑。
	 */
	@Override
	public void run() {

		// ----------------------------
		//  Initial State transition
		//  1. 先确定运行状态，也就是切换状态； 整个while 循环都是做这件事的
		// ----------------------------
		while (true) {
			ExecutionState current = this.executionState;
			// 如果当前的执行状态为CREATED，则将其设置为DEPLOYING状态
			if (current == ExecutionState.CREATED) {
				if (transitionState(ExecutionState.CREATED, ExecutionState.DEPLOYING)) {
					// success, we can start our work
					break;
				}
			}
			//如果当前执行状态为FAILED，则发出通知并退出run方法
			else if (current == ExecutionState.FAILED) {
				// we were immediately failed. tell the TaskManager that we reached our final state
				notifyFinalState();
				if (metrics != null) {
					metrics.close();
				}
				return;
			}
			//如果当前执行状态为CANCELING，则将其修改为CANCELED状态，并退出run
			else if (current == ExecutionState.CANCELING) {
				if (transitionState(ExecutionState.CANCELING, ExecutionState.CANCELED)) {
					// we were immediately canceled. tell the TaskManager that we reached our final state
					notifyFinalState();
					if (metrics != null) {
						metrics.close();
					}
					return;
				}
			}
			else {
				//否则说明发生了异常
				if (metrics != null) {
					metrics.close();
				}
				throw new IllegalStateException("Invalid state for beginning of operation of task " + this + '.');
			}
		}

		//从这里开始的所有资源获取和注册,最终都需要撤消
		// all resource acquisitions and registrations from here on
		// need to be undone in the end
		Map<String, Future<Path>> distributedCacheEntries = new HashMap<>();
		AbstractInvokable invokable = null;

		try {
			// ----------------------------
			//  Task Bootstrap - We periodicall check for canceling as a shortcut
			//  2. 启动任务的入口，会定期检查任务状态，主要做了初始化用户类加载器的工作；
			// ----------------------------

			// activate safety net for task thread
			LOG.info("Creating FileSystem stream leak safety net for task {}", this);
			FileSystemSafetyNet.initializeSafetyNetForThread();

			blobService.getPermanentBlobService().registerJob(jobId);

			// first of all, get a user-code classloader
			// this may involve downloading the job's JAR files and/or classes
			LOG.info("Loading JAR files for task {}.", this);

			// 导入用户类加载器并加载用户代码。
			userCodeClassLoader = createUserCodeClassloader();
			final ExecutionConfig executionConfig = serializedExecutionConfig.deserializeValue(userCodeClassLoader);

			if (executionConfig.getTaskCancellationInterval() >= 0) {
				// override task cancellation interval from Flink config if set in ExecutionConfig
				taskCancellationInterval = executionConfig.getTaskCancellationInterval();
			}

			if (executionConfig.getTaskCancellationTimeout() >= 0) {
				// override task cancellation timeout from Flink config if set in ExecutionConfig
				taskCancellationTimeout = executionConfig.getTaskCancellationTimeout();
			}

			if (isCanceledOrFailed()) {
				throw new CancelTaskException();
			}

			// ----------------------------------------------------------------
			// register the task with the network stack this operation may fail if the system does not have enough memory to run the necessary data exchanges the registration must also strictly be undone
			// 3. 使用 NetworkEnvironment 服务注册任务,
			// 		如果系统没有足够的内存来运行必要的数据交换，则此操作可能会失败，
			// 		如果注册失败，也必须严格保证撤消任务，防止资源的浪费

			// 主要做的工作是分配一些网络资源，监控指标，读入缓存文件等准备工作
			// ----------------------------------------------------------------

			LOG.info("Registering task at network: {}.", this);

			//向网络栈中注册 Task,为 ResultPartition 和 InputGate 分配缓冲池，和之前分析的 数据的输入和输出 的源码解析又联系在了一起，
			network.registerTask(this);

			// add metrics for buffers
			this.metrics.getIOMetricGroup().initializeBufferMetrics(this);

			// register detailed network metrics, if configured
			// 注册详细的网络问题指标，如果配置了的话；
			if (taskManagerConfig.getConfiguration().getBoolean(TaskManagerOptions.NETWORK_DETAILED_METRICS)) {
				// similar to MetricUtils.instantiateNetworkMetrics() but inside this IOMetricGroup
				MetricGroup networkGroup = this.metrics.getIOMetricGroup().addGroup("Network");
				MetricGroup outputGroup = networkGroup.addGroup("Output");
				MetricGroup inputGroup = networkGroup.addGroup("Input");

				// output metrics
				for (int i = 0; i < producedPartitions.length; i++) {
					ResultPartitionMetrics.registerQueueLengthMetrics(
						outputGroup.addGroup(i), producedPartitions[i]);
				}

				for (int i = 0; i < inputGates.length; i++) {
					InputGateMetrics.registerQueueLengthMetrics(
						inputGroup.addGroup(i), inputGates[i]);
				}
			}

			// next, kick off the background copying of files for the distributed cache
			// 接下来，启动分布式缓存的文件的后台复制，也就是读入指定的缓存文件。
			try {
				for (Map.Entry<String, DistributedCache.DistributedCacheEntry> entry :
						DistributedCache.readFileInfoFromConfig(jobConfiguration)) {
					LOG.info("Obtaining local cache file for '{}'.", entry.getKey());
					Future<Path> cp = fileCache.createTmpFile(entry.getKey(), entry.getValue(), jobId, executionId);
					distributedCacheEntries.put(entry.getKey(), cp);
				}
			}
			catch (Exception e) {
				throw new Exception(
					String.format("Exception while adding files to distributed cache of task %s (%s).", taskNameWithSubtask, executionId), e);
			}

			if (isCanceledOrFailed()) {
				throw new CancelTaskException();
			}

			// ----------------------------------------------------------------
			//  call the user code initialization methods
			//  4. 调用用户代码初始化方法
			// 主要的工作是构建一个执行环境，然后初始化用户类的执行方法(也就是初始化invokable，但是并没有真正执行，真正的执行在第五步)
			// ----------------------------------------------------------------

			TaskKvStateRegistry kvStateRegistry = network.createKvStateTaskRegistry(jobId, getJobVertexId());

			// 然后，再把task创建时传入的那一大堆变量用于创建一个执行环境Envrionment。
			//这个 Environment 中封装着这个task的所有信息，最核心的是 StreamConfig， 这个配置类中封装着一个Task具体要执行的信息(用户代码operator)  <<<<<  重要，千万不要忽略
			Environment env = new RuntimeEnvironment(
				jobId,
				vertexId,
				executionId,
				executionConfig,
				taskInfo,
				jobConfiguration,
				taskConfiguration,
				userCodeClassLoader,
				memoryManager,
				ioManager,
				broadcastVariableManager,
				taskStateManager,
				accumulatorRegistry,
				kvStateRegistry,
				inputSplitProvider,
				distributedCacheEntries,
				producedPartitions,
				inputGates,
				network.getTaskEventDispatcher(),
				checkpointResponder,
				taskManagerConfig,
				metrics,
				this);

			// now load and instantiate the task's invokable code
			// nameOfInvokableClass 是 JobVertex 的 invokableClassName，
			// 每一个 StreamNode 在添加的时候都会有一个 jobVertexClass 属性
			// 对于一个 operator chain，就是 head operator 对应的 invokableClassName，见 StreamingJobGraphGenerator.createChain
			// 通过反射创建 AbstractInvokable 对象
			// 对于 Stream 任务而言，就是 StreamTask 的子类，SourceStreamTask、OneInputStreamTask、TwoInputStreamTask 等
			invokable = loadAndInstantiateInvokable(userCodeClassLoader, nameOfInvokableClass, env); // <<<<  这里还有重要注释

			// ----------------------------------------------------------------
			//  actual task core work
			// 5. 实际任务的核心工作！！！！
			// 主要的工作就是执行invokable，执行用户的操作；
			// ----------------------------------------------------------------

			// we must make strictly sure that the invokable is accessible to the cancel() call
			// by the time we switched to running.
			// 我们必须严格确保在我们切换到任务状态到运行时，cancel（）方法的调用可以访问invokable。否则任务就无法停止了；
			this.invokable = invokable;

			// switch to the RUNNING state, if that fails, we have been canceled/failed in the meantime
			// 切换到RUNNING状态，如果失败，抛出任务被取消的异常；
			if (!transitionState(ExecutionState.DEPLOYING, ExecutionState.RUNNING)) {
				throw new CancelTaskException();
			}

			// notify everyone that we switched to running
			// 通知所有人任务已经切换到running 状态；
			notifyObservers(ExecutionState.RUNNING, null);
			taskManagerActions.updateTaskExecutionState(new TaskExecutionState(jobId, executionId, ExecutionState.RUNNING));

			// make sure the user code classloader is accessible thread-locally
			// 确保用户代码类加载器可以线程本地访问
			executingThread.setContextClassLoader(userCodeClassLoader);

			// run the invokable
			// 最重要的方法；因为这个方法就是用户代码所真正被执行的入口。比如我们写的什么 new MapFunction()的逻辑，
			// 最终就是在这里被执行的。这里说一下这个invokable，这是一个抽象类，提供了可以被TaskManager执行的对象的基本抽象。
			invokable.invoke(); //这里，真正执行了，但是operator是什么时候赋值进去的呢？？？？？ <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< 问题  答案看loadAndInstantiateInvokable()方法注释

			// make sure, we enter the catch block if the task leaves the invoke() method due
			// to the fact that it has been canceled
			if (isCanceledOrFailed()) {
				throw new CancelTaskException();
			}

			// 正常结束
			// ----------------------------------------------------------------
			//  finalization of a successful execution
			//  6. 成功执行任务，
			//  主要的工作就是释放一些资源，然后尝试把任务标记为 finished，标记失败，则取消任务；
			// ----------------------------------------------------------------

			// finish the produced partitions. if this fails, we consider the execution failed.
			for (ResultPartition partition : producedPartitions) {
				if (partition != null) {
					partition.finish();
				}
			}

			// try to mark the task as finished
			// if that fails, the task was canceled/failed in the meantime
			// 尝试把该任务标记为finished，如果失败，则调用取消任务，或者标记任务失败；
			if (transitionState(ExecutionState.RUNNING, ExecutionState.FINISHED)) {
				notifyObservers(ExecutionState.FINISHED, null);
			}
			else {
				throw new CancelTaskException();
			}
		}
		catch (Throwable t) {

			// unwrap wrapped exceptions to make stack traces more compact
			if (t instanceof WrappingRuntimeException) {
				t = ((WrappingRuntimeException) t).unwrap();
			}

			// ----------------------------------------------------------------
			// the execution failed. either the invokable code properly failed, or
			// an exception was thrown as a side effect of cancelling
			// ----------------------------------------------------------------

			try {
				// check if the exception is unrecoverable
				if (ExceptionUtils.isJvmFatalError(t) ||
						(t instanceof OutOfMemoryError && taskManagerConfig.shouldExitJvmOnOutOfMemoryError())) {

					// terminate the JVM immediately
					// don't attempt a clean shutdown, because we cannot expect the clean shutdown to complete
					try {
						LOG.error("Encountered fatal error {} - terminating the JVM", t.getClass().getName(), t);
					} finally {
						Runtime.getRuntime().halt(-1);
					}
				}

				// transition into our final state. we should be either in DEPLOYING, RUNNING, CANCELING, or FAILED
				// loop for multiple retries during concurrent state changes via calls to cancel() or
				// to failExternally()
				while (true) {
					ExecutionState current = this.executionState;

					if (current == ExecutionState.RUNNING || current == ExecutionState.DEPLOYING) {
						if (t instanceof CancelTaskException) {
							if (transitionState(current, ExecutionState.CANCELED)) {
								cancelInvokable(invokable);

								notifyObservers(ExecutionState.CANCELED, null);
								break;
							}
						}
						else {
							if (transitionState(current, ExecutionState.FAILED, t)) {
								// proper failure of the task. record the exception as the root cause
								String errorMessage = String.format("Execution of %s (%s) failed.", taskNameWithSubtask, executionId);
								failureCause = t;
								cancelInvokable(invokable);

								notifyObservers(ExecutionState.FAILED, new Exception(errorMessage, t));
								break;
							}
						}
					}
					else if (current == ExecutionState.CANCELING) {
						if (transitionState(current, ExecutionState.CANCELED)) {
							notifyObservers(ExecutionState.CANCELED, null);
							break;
						}
					}
					else if (current == ExecutionState.FAILED) {
						// in state failed already, no transition necessary any more
						break;
					}
					// unexpected state, go to failed
					else if (transitionState(current, ExecutionState.FAILED, t)) {
						LOG.error("Unexpected state in task {} ({}) during an exception: {}.", taskNameWithSubtask, executionId, current);
						break;
					}
					// else fall through the loop and
				}
			}
			catch (Throwable tt) {
				String message = String.format("FATAL - exception in exception handler of task %s (%s).", taskNameWithSubtask, executionId);
				LOG.error(message, tt);
				notifyFatalError(message, tt);
			}
		}
		finally {
			try {
				LOG.info("Freeing task resources for {} ({}).", taskNameWithSubtask, executionId);

				// clear the reference to the invokable. this helps guard against holding references
				// to the invokable and its structures in cases where this Task object is still referenced
				this.invokable = null;

				// stop the async dispatcher.
				// copy dispatcher reference to stack, against concurrent release
				ExecutorService dispatcher = this.asyncCallDispatcher;
				if (dispatcher != null && !dispatcher.isShutdown()) {
					dispatcher.shutdownNow();
				}

				// free the network resources
				network.unregisterTask(this);

				// free memory resources
				if (invokable != null) {
					memoryManager.releaseAll(invokable);
				}

				// remove all of the tasks library resources
				libraryCache.unregisterTask(jobId, executionId);
				fileCache.releaseJob(jobId, executionId);
				blobService.getPermanentBlobService().releaseJob(jobId);

				// close and de-activate safety net for task thread
				LOG.info("Ensuring all FileSystem streams are closed for task {}", this);
				FileSystemSafetyNet.closeSafetyNetAndGuardedResourcesForThread();

				notifyFinalState();
			}
			catch (Throwable t) {
				// an error in the resource cleanup is fatal
				String message = String.format("FATAL - exception in resource cleanup of task %s (%s).", taskNameWithSubtask, executionId);
				LOG.error(message, t);
				notifyFatalError(message, t);
			}

			// un-register the metrics at the end so that the task may already be
			// counted as finished when this happens
			// errors here will only be logged
			try {
				metrics.close();
			}
			catch (Throwable t) {
				LOG.error("Error during metrics de-registration of task {} ({}).", taskNameWithSubtask, executionId, t);
			}
		}
	}

	private ClassLoader createUserCodeClassloader() throws Exception {
		long startDownloadTime = System.currentTimeMillis();

		// triggers the download of all missing jar files from the job manager
		libraryCache.registerTask(jobId, executionId, requiredJarFiles, requiredClasspaths);

		LOG.debug("Getting user code class loader for task {} at library cache manager took {} milliseconds",
				executionId, System.currentTimeMillis() - startDownloadTime);

		ClassLoader userCodeClassLoader = libraryCache.getClassLoader(jobId);
		if (userCodeClassLoader == null) {
			throw new Exception("No user code classloader available.");
		}
		return userCodeClassLoader;
	}

	private void notifyFinalState() {
		taskManagerActions.notifyFinalState(executionId);
	}

	private void notifyFatalError(String message, Throwable cause) {
		taskManagerActions.notifyFatalError(message, cause);
	}

	/**
	 * Try to transition the execution state from the current state to the new state.
	 *
	 * @param currentState of the execution
	 * @param newState of the execution
	 * @return true if the transition was successful, otherwise false
	 */
	private boolean transitionState(ExecutionState currentState, ExecutionState newState) {
		return transitionState(currentState, newState, null);
	}

	/**
	 * Try to transition the execution state from the current state to the new state.
	 *
	 * @param currentState of the execution
	 * @param newState of the execution
	 * @param cause of the transition change or null
	 * @return true if the transition was successful, otherwise false
	 */
	private boolean transitionState(ExecutionState currentState, ExecutionState newState, Throwable cause) {
		if (STATE_UPDATER.compareAndSet(this, currentState, newState)) {
			if (cause == null) {
				LOG.info("{} ({}) switched from {} to {}.", taskNameWithSubtask, executionId, currentState, newState);
			} else {
				LOG.info("{} ({}) switched from {} to {}.", taskNameWithSubtask, executionId, currentState, newState, cause);
			}

			return true;
		} else {
			return false;
		}
	}

	// ----------------------------------------------------------------------------------------------------------------
	//  Stopping / Canceling / Failing the task from the outside
	// ----------------------------------------------------------------------------------------------------------------

	/**
	 * Stops the executing task by calling {@link StoppableTask#stop()}.
	 * <p>
	 * This method never blocks.
	 * </p>
	 *
	 * @throws UnsupportedOperationException if the {@link AbstractInvokable} does not implement {@link StoppableTask}
	 * @throws IllegalStateException if the {@link Task} is not yet running
	 */
	public void stopExecution() {
		// copy reference to stack, to guard against concurrent setting to null
		final AbstractInvokable invokable = this.invokable;

		if (invokable != null) {
			if (invokable instanceof StoppableTask) {
				LOG.info("Attempting to stop task {} ({}).", taskNameWithSubtask, executionId);
				final StoppableTask stoppable = (StoppableTask) invokable;

				Runnable runnable = () -> {
					try {
						stoppable.stop();
					} catch (Throwable t) {
						LOG.error("Stopping task {} ({}) failed.", taskNameWithSubtask, executionId, t);
						taskManagerActions.failTask(executionId, t);
					}
				};
				executeAsyncCallRunnable(runnable, String.format("Stopping source task %s (%s).", taskNameWithSubtask, executionId));
			} else {
				throw new UnsupportedOperationException(String.format("Stopping not supported by task %s (%s).", taskNameWithSubtask, executionId));
			}
		} else {
			throw new IllegalStateException(
				String.format(
					"Cannot stop task %s (%s) because it is not running.",
					taskNameWithSubtask,
					executionId));
		}
	}

	/**
	 * Cancels the task execution. If the task is already in a terminal state
	 * (such as FINISHED, CANCELED, FAILED), or if the task is already canceling this does nothing.
	 * Otherwise it sets the state to CANCELING, and, if the invokable code is running,
	 * starts an asynchronous thread that aborts that code.
	 *
	 * <p>This method never blocks.</p>
	 */
	public void cancelExecution() {
		LOG.info("Attempting to cancel task {} ({}).", taskNameWithSubtask, executionId);
		cancelOrFailAndCancelInvokable(ExecutionState.CANCELING, null);
	}

	/**
	 * Marks task execution failed for an external reason (a reason other than the task code itself
	 * throwing an exception). If the task is already in a terminal state
	 * (such as FINISHED, CANCELED, FAILED), or if the task is already canceling this does nothing.
	 * Otherwise it sets the state to FAILED, and, if the invokable code is running,
	 * starts an asynchronous thread that aborts that code.
	 *
	 * <p>This method never blocks.</p>
	 */
	@Override
	public void failExternally(Throwable cause) {
		LOG.info("Attempting to fail task externally {} ({}).", taskNameWithSubtask, executionId);
		cancelOrFailAndCancelInvokable(ExecutionState.FAILED, cause);
	}

	private void cancelOrFailAndCancelInvokable(ExecutionState targetState, Throwable cause) {
		while (true) {
			ExecutionState current = executionState;

			// if the task is already canceled (or canceling) or finished or failed,
			// then we need not do anything
			if (current.isTerminal() || current == ExecutionState.CANCELING) {
				LOG.info("Task {} is already in state {}", taskNameWithSubtask, current);
				return;
			}

			if (current == ExecutionState.DEPLOYING || current == ExecutionState.CREATED) {
				if (transitionState(current, targetState, cause)) {
					// if we manage this state transition, then the invokable gets never called
					// we need not call cancel on it
					this.failureCause = cause;
					notifyObservers(
						targetState,
						new Exception(
							String.format(
								"Cancel or fail execution of %s (%s).",
								taskNameWithSubtask,
								executionId),
							cause));
					return;
				}
			}
			else if (current == ExecutionState.RUNNING) {
				if (transitionState(ExecutionState.RUNNING, targetState, cause)) {
					// we are canceling / failing out of the running state
					// we need to cancel the invokable

					// copy reference to guard against concurrent null-ing out the reference
					final AbstractInvokable invokable = this.invokable;

					if (invokable != null && invokableHasBeenCanceled.compareAndSet(false, true)) {
						this.failureCause = cause;
						notifyObservers(
							targetState,
							new Exception(
								String.format(
									"Cancel or fail execution of %s (%s).",
									taskNameWithSubtask,
									executionId),
								cause));

						LOG.info("Triggering cancellation of task code {} ({}).", taskNameWithSubtask, executionId);

						// because the canceling may block on user code, we cancel from a separate thread
						// we do not reuse the async call handler, because that one may be blocked, in which
						// case the canceling could not continue

						// The canceller calls cancel and interrupts the executing thread once
						Runnable canceler = new TaskCanceler(
								LOG,
								invokable,
								executingThread,
								taskNameWithSubtask,
								producedPartitions,
								inputGates);

						Thread cancelThread = new Thread(
								executingThread.getThreadGroup(),
								canceler,
								String.format("Canceler for %s (%s).", taskNameWithSubtask, executionId));
						cancelThread.setDaemon(true);
						cancelThread.setUncaughtExceptionHandler(FatalExitExceptionHandler.INSTANCE);
						cancelThread.start();

						// the periodic interrupting thread - a different thread than the canceller, in case
						// the application code does blocking stuff in its cancellation paths.
						if (invokable.shouldInterruptOnCancel()) {
							Runnable interrupter = new TaskInterrupter(
									LOG,
									invokable,
									executingThread,
									taskNameWithSubtask,
									taskCancellationInterval);

							Thread interruptingThread = new Thread(
									executingThread.getThreadGroup(),
									interrupter,
									String.format("Canceler/Interrupts for %s (%s).", taskNameWithSubtask, executionId));
							interruptingThread.setDaemon(true);
							interruptingThread.setUncaughtExceptionHandler(FatalExitExceptionHandler.INSTANCE);
							interruptingThread.start();
						}

						// if a cancellation timeout is set, the watchdog thread kills the process
						// if graceful cancellation does not succeed
						if (taskCancellationTimeout > 0) {
							Runnable cancelWatchdog = new TaskCancelerWatchDog(
									executingThread,
									taskManagerActions,
									taskCancellationTimeout,
									LOG);

							Thread watchDogThread = new Thread(
									executingThread.getThreadGroup(),
									cancelWatchdog,
									String.format("Cancellation Watchdog for %s (%s).",
											taskNameWithSubtask, executionId));
							watchDogThread.setDaemon(true);
							watchDogThread.setUncaughtExceptionHandler(FatalExitExceptionHandler.INSTANCE);
							watchDogThread.start();
						}
					}
					return;
				}
			}
			else {
				throw new IllegalStateException(String.format("Unexpected state: %s of task %s (%s).",
					current, taskNameWithSubtask, executionId));
			}
		}
	}

	// ------------------------------------------------------------------------
	//  State Listeners
	// ------------------------------------------------------------------------

	public void registerExecutionListener(TaskExecutionStateListener listener) {
		taskExecutionStateListeners.add(listener);
	}

	private void notifyObservers(ExecutionState newState, Throwable error) {
		TaskExecutionState stateUpdate = new TaskExecutionState(jobId, executionId, newState, error);

		for (TaskExecutionStateListener listener : taskExecutionStateListeners) {
			listener.notifyTaskExecutionStateChanged(stateUpdate);
		}
	}

	// ------------------------------------------------------------------------
	//  Partition State Listeners
	// ------------------------------------------------------------------------

	@Override
	public void triggerPartitionProducerStateCheck(
		JobID jobId,
		final IntermediateDataSetID intermediateDataSetId,
		final ResultPartitionID resultPartitionId) {

		CompletableFuture<ExecutionState> futurePartitionState =
			partitionProducerStateChecker.requestPartitionProducerState(
				jobId,
				intermediateDataSetId,
				resultPartitionId);

		futurePartitionState.whenCompleteAsync(
			(ExecutionState executionState, Throwable throwable) -> {
				try {
					if (executionState != null) {
						onPartitionStateUpdate(
							intermediateDataSetId,
							resultPartitionId,
							executionState);
					} else if (throwable instanceof TimeoutException) {
						// our request timed out, assume we're still running and try again
						onPartitionStateUpdate(
							intermediateDataSetId,
							resultPartitionId,
							ExecutionState.RUNNING);
					} else if (throwable instanceof PartitionProducerDisposedException) {
						String msg = String.format("Producer %s of partition %s disposed. Cancelling execution.",
							resultPartitionId.getProducerId(), resultPartitionId.getPartitionId());
						LOG.info(msg, throwable);
						cancelExecution();
					} else {
						failExternally(throwable);
					}
				} catch (IOException | InterruptedException e) {
					failExternally(e);
				}
			},
			executor);
	}

	// ------------------------------------------------------------------------
	//  Notifications on the invokable
	// ------------------------------------------------------------------------

	/**
	 * Calls the invokable to trigger a checkpoint.
	 *
	 * @param checkpointID The ID identifying the checkpoint.
	 * @param checkpointTimestamp The timestamp associated with the checkpoint.
	 * @param checkpointOptions Options for performing this checkpoint.
	 *
	 * 作为Source的Task，调用这个方法，触发checkpoint逻辑，注意是异步进行的；
	 */
	public void triggerCheckpointBarrier(
			final long checkpointID,
			long checkpointTimestamp,
			final CheckpointOptions checkpointOptions) {

		final AbstractInvokable invokable = this.invokable;  //这个Task执行的任务类型， StreamTask的子类；
		//该类创建了一个 CheckpointMetaData 的对象，：
		final CheckpointMetaData checkpointMetaData = new CheckpointMetaData(checkpointID, checkpointTimestamp);

		if (executionState == ExecutionState.RUNNING && invokable != null) {  //确定任务状态

			// build a local closure
			final String taskName = taskNameWithSubtask;
			final SafetyNetCloseableRegistry safetyNetCloseableRegistry =
				FileSystemSafetyNet.getSafetyNetCloseableRegistryForThread();

			// 并且生成了一个 Runable匿名类用于执行checkpoint，然后以异步的方式触发了该Runable
			Runnable runnable = new Runnable() {
				@Override
				public void run() {
					// set safety net from the task's context for checkpointing thread
					LOG.debug("Creating FileSystem stream leak safety net for {}", Thread.currentThread().getName());
					FileSystemSafetyNet.setSafetyNetCloseableRegistryForThread(safetyNetCloseableRegistry);

					try {
						// invokable事实上就是我们的StreamTask了,
						// Task类实际上是将checkpoint委托给了更具体的类去执行，而StreamTask也将委托给更具体的类，直到业务代码。
						boolean success = invokable.triggerCheckpoint(checkpointMetaData, checkpointOptions);
						if (!success) {
							checkpointResponder.declineCheckpoint(
									getJobID(), getExecutionId(), checkpointID,
									new CheckpointDeclineTaskNotReadyException(taskName));
						}
					}
					catch (Throwable t) {
						if (getExecutionState() == ExecutionState.RUNNING) {
							failExternally(new Exception(
								"Error while triggering checkpoint " + checkpointID + " for " +
									taskNameWithSubtask, t));
						} else {
							LOG.debug("Encountered error while triggering checkpoint {} for " +
								"{} ({}) while being not in state running.", checkpointID,
								taskNameWithSubtask, executionId, t);
						}
					} finally {
						FileSystemSafetyNet.setSafetyNetCloseableRegistryForThread(null);
					}
				}
			};
			executeAsyncCallRunnable(runnable, String.format("Checkpoint Trigger for %s (%s).", taskNameWithSubtask, executionId));
		}
		else {
			LOG.debug("Declining checkpoint request for non-running task {} ({}).", taskNameWithSubtask, executionId);

			// send back a message that we did not do the checkpoint
			checkpointResponder.declineCheckpoint(jobId, executionId, checkpointID,
					new CheckpointDeclineTaskNotReadyException(taskNameWithSubtask));
		}
	}

	@Override
	public void notifyCheckpointComplete(final long checkpointID) {
		final AbstractInvokable invokable = this.invokable;

		if (executionState == ExecutionState.RUNNING && invokable != null) {

			Runnable runnable = new Runnable() {
				@Override
				public void run() {
					try {
						invokable.notifyCheckpointComplete(checkpointID);
						taskStateManager.notifyCheckpointComplete(checkpointID);
					} catch (Throwable t) {
						if (getExecutionState() == ExecutionState.RUNNING) {
							// fail task if checkpoint confirmation failed.
							failExternally(new RuntimeException(
								"Error while confirming checkpoint",
								t));
						}
					}
				}
			};
			executeAsyncCallRunnable(runnable, "Checkpoint Confirmation for " +
				taskNameWithSubtask);
		}
		else {
			LOG.debug("Ignoring checkpoint commit notification for non-running task {}.", taskNameWithSubtask);
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * Answer to a partition state check issued after a failed partition request.
	 */
	@VisibleForTesting
	void onPartitionStateUpdate(
			IntermediateDataSetID intermediateDataSetId,
			ResultPartitionID resultPartitionId,
			ExecutionState producerState) throws IOException, InterruptedException {

		if (executionState == ExecutionState.RUNNING) {
			final SingleInputGate inputGate = inputGatesById.get(intermediateDataSetId);

			if (inputGate != null) {
				if (producerState == ExecutionState.SCHEDULED
					|| producerState == ExecutionState.DEPLOYING
					|| producerState == ExecutionState.RUNNING
					|| producerState == ExecutionState.FINISHED) {

					// Retrigger the partition request
					inputGate.retriggerPartitionRequest(resultPartitionId.getPartitionId());

				} else if (producerState == ExecutionState.CANCELING
					|| producerState == ExecutionState.CANCELED
					|| producerState == ExecutionState.FAILED) {

					// The producing execution has been canceled or failed. We
					// don't need to re-trigger the request since it cannot
					// succeed.
					if (LOG.isDebugEnabled()) {
						LOG.debug("Cancelling task {} after the producer of partition {} with attempt ID {} has entered state {}.",
							taskNameWithSubtask,
							resultPartitionId.getPartitionId(),
							resultPartitionId.getProducerId(),
							producerState);
					}

					cancelExecution();
				} else {
					// Any other execution state is unexpected. Currently, only
					// state CREATED is left out of the checked states. If we
					// see a producer in this state, something went wrong with
					// scheduling in topological order.
					String msg = String.format("Producer with attempt ID %s of partition %s in unexpected state %s.",
						resultPartitionId.getProducerId(),
						resultPartitionId.getPartitionId(),
						producerState);

					failExternally(new IllegalStateException(msg));
				}
			} else {
				failExternally(new IllegalStateException("Received partition producer state for " +
						"unknown input gate " + intermediateDataSetId + "."));
			}
		} else {
			LOG.debug("Task {} ignored a partition producer state notification, because it's not running.", taskNameWithSubtask);
		}
	}

	/**
	 * Utility method to dispatch an asynchronous call on the invokable.
	 *
	 * @param runnable The async call runnable.
	 * @param callName The name of the call, for logging purposes.
	 */
	private void executeAsyncCallRunnable(Runnable runnable, String callName) {
		// make sure the executor is initialized. lock against concurrent calls to this function
		synchronized (this) {
			if (executionState != ExecutionState.RUNNING) {
				return;
			}

			// get ourselves a reference on the stack that cannot be concurrently modified
			ExecutorService executor = this.asyncCallDispatcher;
			if (executor == null) {
				// first time use, initialize
				checkState(userCodeClassLoader != null, "userCodeClassLoader must not be null");
				executor = Executors.newSingleThreadExecutor(
						new DispatcherThreadFactory(
							TASK_THREADS_GROUP,
							"Async calls on " + taskNameWithSubtask,
							userCodeClassLoader));
				this.asyncCallDispatcher = executor;

				// double-check for execution state, and make sure we clean up after ourselves
				// if we created the dispatcher while the task was concurrently canceled
				if (executionState != ExecutionState.RUNNING) {
					executor.shutdown();
					asyncCallDispatcher = null;
					return;
				}
			}

			LOG.debug("Invoking async call {} on task {}", callName, taskNameWithSubtask);

			try {
				executor.submit(runnable);
			}
			catch (RejectedExecutionException e) {
				// may be that we are concurrently finished or canceled.
				// if not, report that something is fishy
				if (executionState == ExecutionState.RUNNING) {
					throw new RuntimeException("Async call was rejected, even though the task is running.", e);
				}
			}
		}
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	private void cancelInvokable(AbstractInvokable invokable) {
		// in case of an exception during execution, we still call "cancel()" on the task
		if (invokable != null && invokableHasBeenCanceled.compareAndSet(false, true)) {
			try {
				invokable.cancel();
			}
			catch (Throwable t) {
				LOG.error("Error while canceling task {}.", taskNameWithSubtask, t);
			}
		}
	}

	@Override
	public String toString() {
		return String.format("%s (%s) [%s]", taskNameWithSubtask, executionId, executionState);
	}

	/**
	 * Instantiates the given task invokable class, passing the given environment (and possibly
	 * the initial task state) to the task's constructor.
	 *
	 * <p>The method will first try to instantiate the task via a constructor accepting both
	 * the Environment and the TaskStateSnapshot. If no such constructor exists, and there is
	 * no initial state, the method will fall back to the stateless convenience constructor that
	 * accepts only the Environment.
	 *
	 * @param classLoader The classloader to load the class through.
	 * @param className The name of the class to load.
	 * @param environment The task environment.
	 *
	 * @return The instantiated invokable task object.
	 *
	 * 注：这里如果又忘记了， 可以看一下 StreamGraph 的 addOperator() 方法，简单说一下就是flink把任务预先分成了几种类型，SourceStreamTask / OneInputStreamTask ...
	 * 在构建图的过程中，用户的代码逻辑是按照三层逻辑去封装的： DataStream -> transformation -> StreamOperator，但是上面的类型和用户代码封装逻辑是没关系的，是另外一套东西，这里不要弄混了
	 * 在往StreamGraph图中加入StreamNode的时候，会根据operator的类型，来加入一个ClassName(SourceStreamTask.class / OneInputStreamTask.class)， 这个ClassName对应的就是这里的 ClassName，他们的父类都是 AbstractInvokable
	 *
	 * 根据任务类型， 反射生成 AbstractInvokable 实例；
	 *
	 * 问题：这里反射生成的是 AbstractInvokable 实例，但是用户的代码是封装在operator中的， 这俩是如何联系起来的呢？
	 * 答： 核心是 Environment 这个类，创建具体的StreamTask的时候，都是调用的带 Environment 参数的构造器，这个 Environment 中封装着这个task的所有信息，最核心的是 StreamConfig， 这个配置类中封装着一个Task具体要执行的信息(用户代码operator)
	 */
	private static AbstractInvokable loadAndInstantiateInvokable(
		ClassLoader classLoader,
		String className,
		Environment environment) throws Throwable {

		final Class<? extends AbstractInvokable> invokableClass;
		try {
			invokableClass = Class.forName(className, true, classLoader) //反射
				.asSubclass(AbstractInvokable.class);
		} catch (Throwable t) {
			throw new Exception("Could not load the task's invokable class.", t);
		}

		Constructor<? extends AbstractInvokable> statelessCtor;

		try {
			statelessCtor = invokableClass.getConstructor(Environment.class);  //获得这个StreamTask的这个构造器，只有 Environment 这一个参数的；
		} catch (NoSuchMethodException ee) {
			throw new FlinkException("Task misses proper constructor", ee);
		}

		// instantiate the class
		try {
			//noinspection ConstantConditions  --> cannot happen
			return statelessCtor.newInstance(environment);   //用这个带Environment参数的构造器，来new 实例  <<<<<<< 重要
		} catch (InvocationTargetException e) {
			// directly forward exceptions from the eager initialization
			throw e.getTargetException();
		} catch (Exception e) {
			throw new FlinkException("Could not instantiate the task's invokable class.", e);
		}
	}

	// ------------------------------------------------------------------------
	//  Task cancellation
	//
	//  The task cancellation uses in total three threads, as a safety net
	//  against various forms of user- and JVM bugs.
	//
	//    - The first thread calls 'cancel()' on the invokable and closes
	//      the input and output connections, for fast thread termination
	//    - The second thread periodically interrupts the invokable in order
	//      to pull the thread out of blocking wait and I/O operations
	//    - The third thread (watchdog thread) waits until the cancellation
	//      timeout and then performs a hard cancel (kill process, or let
	//      the TaskManager know)
	//
	//  Previously, thread two and three were in one thread, but we needed
	//  to separate this to make sure the watchdog thread does not call
	//  'interrupt()'. This is a workaround for the following JVM bug
	//   https://bugs.java.com/bugdatabase/view_bug.do?bug_id=8138622
	// ------------------------------------------------------------------------

	/**
	 * This runner calls cancel() on the invokable, closes input-/output resources,
	 * and initially interrupts the task thread.
	 */
	private static class TaskCanceler implements Runnable {

		private final Logger logger;
		private final AbstractInvokable invokable;
		private final Thread executer;
		private final String taskName;
		private final ResultPartition[] producedPartitions;
		private final SingleInputGate[] inputGates;

		public TaskCanceler(
				Logger logger,
				AbstractInvokable invokable,
				Thread executer,
				String taskName,
				ResultPartition[] producedPartitions,
				SingleInputGate[] inputGates) {

			this.logger = logger;
			this.invokable = invokable;
			this.executer = executer;
			this.taskName = taskName;
			this.producedPartitions = producedPartitions;
			this.inputGates = inputGates;
		}

		@Override
		public void run() {
			try {
				// the user-defined cancel method may throw errors.
				// we need do continue despite that
				try {
					invokable.cancel();
				} catch (Throwable t) {
					ExceptionUtils.rethrowIfFatalError(t);
					logger.error("Error while canceling the task {}.", taskName, t);
				}

				// Early release of input and output buffer pools. We do this
				// in order to unblock async Threads, which produce/consume the
				// intermediate streams outside of the main Task Thread (like
				// the Kafka consumer).
				//
				// Don't do this before cancelling the invokable. Otherwise we
				// will get misleading errors in the logs.
				for (ResultPartition partition : producedPartitions) {
					try {
						partition.destroyBufferPool();
					} catch (Throwable t) {
						ExceptionUtils.rethrowIfFatalError(t);
						LOG.error("Failed to release result partition buffer pool for task {}.", taskName, t);
					}
				}

				for (SingleInputGate inputGate : inputGates) {
					try {
						inputGate.releaseAllResources();
					} catch (Throwable t) {
						ExceptionUtils.rethrowIfFatalError(t);
						LOG.error("Failed to release input gate for task {}.", taskName, t);
					}
				}

				// send the initial interruption signal, if requested
				if (invokable.shouldInterruptOnCancel()) {
					executer.interrupt();
				}
			}
			catch (Throwable t) {
				ExceptionUtils.rethrowIfFatalError(t);
				logger.error("Error in the task canceler for task {}.", taskName, t);
			}
		}
	}

	/**
	 * This thread sends the delayed, periodic interrupt calls to the executing thread.
	 */
	private static final class TaskInterrupter implements Runnable {

		/** The logger to report on the fatal condition. */
		private final Logger log;

		/** The invokable task. */
		private final AbstractInvokable task;

		/** The executing task thread that we wait for to terminate. */
		private final Thread executerThread;

		/** The name of the task, for logging purposes. */
		private final String taskName;

		/** The interval in which we interrupt. */
		private final long interruptIntervalMillis;

		TaskInterrupter(
				Logger log,
				AbstractInvokable task,
				Thread executerThread,
				String taskName,
				long interruptIntervalMillis) {

			this.log = log;
			this.task = task;
			this.executerThread = executerThread;
			this.taskName = taskName;
			this.interruptIntervalMillis = interruptIntervalMillis;
		}

		@Override
		public void run() {
			try {
				// we initially wait for one interval
				// in most cases, the threads go away immediately (by the cancellation thread)
				// and we need not actually do anything
				executerThread.join(interruptIntervalMillis);

				// log stack trace where the executing thread is stuck and
				// interrupt the running thread periodically while it is still alive
				while (task.shouldInterruptOnCancel() && executerThread.isAlive()) {
					// build the stack trace of where the thread is stuck, for the log
					StackTraceElement[] stack = executerThread.getStackTrace();
					StringBuilder bld = new StringBuilder();
					for (StackTraceElement e : stack) {
						bld.append(e).append('\n');
					}

					log.warn("Task '{}' did not react to cancelling signal for {} seconds, but is stuck in method:\n {}",
							taskName, (interruptIntervalMillis / 1000), bld);

					executerThread.interrupt();
					try {
						executerThread.join(interruptIntervalMillis);
					}
					catch (InterruptedException e) {
						// we ignore this and fall through the loop
					}
				}
			} catch (Throwable t) {
				ExceptionUtils.rethrowIfFatalError(t);
				log.error("Error in the task canceler for task {}.", taskName, t);
			}
		}
	}

	/**
	 * Watchdog for the cancellation.
	 * If the task thread does not go away gracefully within a certain time, we
	 * trigger a hard cancel action (notify TaskManager of fatal error, which in
	 * turn kills the process).
	 */
	private static class TaskCancelerWatchDog implements Runnable {

		/** The logger to report on the fatal condition. */
		private final Logger log;

		/** The executing task thread that we wait for to terminate. */
		private final Thread executerThread;

		/** The TaskManager to notify if cancellation does not happen in time. */
		private final TaskManagerActions taskManager;

		/** The timeout for cancellation. */
		private final long timeoutMillis;

		TaskCancelerWatchDog(
				Thread executerThread,
				TaskManagerActions taskManager,
				long timeoutMillis,
				Logger log) {

			checkArgument(timeoutMillis > 0);

			this.log = log;
			this.executerThread = executerThread;
			this.taskManager = taskManager;
			this.timeoutMillis = timeoutMillis;
		}

		@Override
		public void run() {
			try {
				final long hardKillDeadline = System.nanoTime() + timeoutMillis * 1_000_000;

				long millisLeft;
				while (executerThread.isAlive()
						&& (millisLeft = (hardKillDeadline - System.nanoTime()) / 1_000_000) > 0) {

					try {
						executerThread.join(millisLeft);
					}
					catch (InterruptedException ignored) {
						// we don't react to interrupted exceptions, simply fall through the loop
					}
				}

				if (executerThread.isAlive()) {
					String msg = "Task did not exit gracefully within " + (timeoutMillis / 1000) + " + seconds.";
					log.error(msg);
					taskManager.notifyFatalError(msg, null);
				}
			}
			catch (Throwable t) {
				ExceptionUtils.rethrowIfFatalError(t);
				log.error("Error in Task Cancellation Watch Dog", t);
			}
		}
	}
}

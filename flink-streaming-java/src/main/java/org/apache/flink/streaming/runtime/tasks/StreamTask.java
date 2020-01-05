/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FileSystemSafetyNet;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.runtime.state.TaskStateManager;
import org.apache.flink.runtime.taskmanager.DispatcherThreadFactory;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFinalizer;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializer;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializerImpl;
import org.apache.flink.streaming.runtime.io.RecordWriterOutput;
import org.apache.flink.streaming.runtime.io.StreamRecordWriter;
import org.apache.flink.streaming.runtime.partitioner.ConfigurableStreamPartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Base class for all streaming tasks. A task is the unit of local processing that is deployed
 * and executed by the TaskManagers. Each task runs one or more {@link StreamOperator}s which form
 * the Task's operator chain. Operators that are chained together execute synchronously in the
 * same thread and hence on the same stream partition. A common case for these chains
 * are successive map/flatmap/filter tasks.
 *
 * <p>The task chain contains one "head" operator and multiple chained operators.
 * The StreamTask is specialized for the type of the head operator: one-input and two-input tasks,
 * as well as for sources, iteration heads and iteration tails.
 *
 * <p>The Task class deals with the setup of the streams read by the head operator, and the streams
 * produced by the operators at the ends of the operator chain. Note that the chain may fork and
 * thus have multiple ends.
 *
 * <p>The life cycle of the task is set up as follows:
 * <pre>{@code
 *  -- setInitialState -> provides state of all operators in the chain
 *
 *  -- invoke()
 *        |
 *        +----> Create basic utils (config, etc) and load the chain of operators
 *        +----> operators.setup()
 *        +----> task specific init()
 *        +----> initialize-operator-states()
 *        +----> open-operators()
 *        +----> run()
 *        +----> close-operators()
 *        +----> dispose-operators()
 *        +----> common cleanup
 *        +----> task specific cleanup()
 * }</pre>
 *
 * <p>The {@code StreamTask} has a lock object called {@code lock}. All calls to methods on a
 * {@code StreamOperator} must be synchronized on this lock object to ensure that no methods
 * are called concurrently.
 *
 * @param <OUT>
 * @param <OP>
 *
 * 可以理解为执行用户代码的容器， tm执行Task的时候，就是反射生成这个类，然后调用 invoke() 方法的过程；
 */
@Internal
public abstract class StreamTask<OUT, OP extends StreamOperator<OUT>>
		extends AbstractInvokable
		implements AsyncExceptionHandler {

	/** The thread group that holds all trigger timer threads. */
	public static final ThreadGroup TRIGGER_THREAD_GROUP = new ThreadGroup("Triggers");

	/** The logger used by the StreamTask and its subclasses. */
	private static final Logger LOG = LoggerFactory.getLogger(StreamTask.class);

	// ------------------------------------------------------------------------

	/**
	 * All interaction with the {@code StreamOperator} must be synchronized on this lock object to
	 * ensure that we don't have concurrent method calls that void consistent checkpoints.
	 */
	private final Object lock = new Object();

	/** the head operator that consumes the input streams of this task. */
	protected OP headOperator;

	/** The chain of operators executed by this task. */
	protected OperatorChain<OUT, OP> operatorChain;

	/** The configuration of this streaming task. */
	protected final StreamConfig configuration;

	/** Our state backend. We use this to create checkpoint streams and a keyed state backend. */
	protected StateBackend stateBackend;

	/** The external storage where checkpoint data is persisted. */
	private CheckpointStorage checkpointStorage;  //检查点存储的抽象，有两个实现： FsCheckpointStorage 和 MemoryBackendCheckpointStorage，checkpointStorage 则是从 stateBackend 中生成的；

	/**
	 * The internal {@link ProcessingTimeService} used to define the current
	 * processing time (default = {@code System.currentTimeMillis()}) and
	 * register timers for tasks to be executed in the future.
	 */
	protected ProcessingTimeService timerService;

	/** The map of user-defined accumulators of this task. */
	private final Map<String, Accumulator<?, ?>> accumulatorMap;

	/** The currently active background materialization threads. */
	private final CloseableRegistry cancelables = new CloseableRegistry();

	/**
	 * Flag to mark the task "in operation", in which case check needs to be initialized to true,
	 * so that early cancel() before invoke() behaves correctly.
	 */
	private volatile boolean isRunning;

	/** Flag to mark this task as canceled. */
	private volatile boolean canceled;

	/** Thread pool for async snapshot workers. */
	private ExecutorService asyncOperationsThreadPool;

	/** Handler for exceptions during checkpointing in the stream task. Used in synchronous part of the checkpoint. */
	private CheckpointExceptionHandler synchronousCheckpointExceptionHandler;

	/** Wrapper for synchronousCheckpointExceptionHandler to deal with rethrown exceptions. Used in the async part. */
	private AsyncCheckpointExceptionHandler asynchronousCheckpointExceptionHandler;

	private final List<StreamRecordWriter<SerializationDelegate<StreamRecord<OUT>>>> streamRecordWriters;

	// ------------------------------------------------------------------------

	/**
	 * Constructor for initialization, possibly with initial state (recovery / savepoint / etc).
	 *
	 * @param env The task environment for this task.
	 */
	protected StreamTask(Environment env) {
		this(env, null);
	}

	/**
	 * Constructor for initialization, possibly with initial state (recovery / savepoint / etc).
	 *
	 * <p>This constructor accepts a special {@link ProcessingTimeService}. By default (and if
	 * null is passes for the time provider) a {@link SystemProcessingTimeService DefaultTimerService}
	 * will be used.
	 *
	 * @param environment The task environment for this task.
	 * @param timeProvider Optionally, a specific time provider to use.
	 *
	 *
	 */
	protected StreamTask(
			Environment environment,
			@Nullable ProcessingTimeService timeProvider) {

		super(environment);

		this.timerService = timeProvider;
		this.configuration = new StreamConfig(getTaskConfiguration());  //  >>>>>>>>>>>>> 核心 StreamConfig 中保存了具体用户代码的operator；
		this.accumulatorMap = getEnvironment().getAccumulatorRegistry().getUserMap();
		this.streamRecordWriters = createStreamRecordWriters(configuration, environment);
	}

	// ------------------------------------------------------------------------
	//  Life cycle methods for specific implementations
	// ------------------------------------------------------------------------

	protected abstract void init() throws Exception;

	protected abstract void run() throws Exception;

	protected abstract void cleanup() throws Exception;

	protected abstract void cancelTask() throws Exception;

	// ------------------------------------------------------------------------
	//  Core work methods of the Stream Task
	// ------------------------------------------------------------------------

	public StreamTaskStateInitializer createStreamTaskStateInitializer() {
		return new StreamTaskStateInitializerImpl(
			getEnvironment(),
			stateBackend,
			timerService);
	}

	/**
	 * tm执行task时调用的 invokable 方法就是这个方法；
	 * @throws Exception
	 */
	@Override
	public final void invoke() throws Exception {

		boolean disposed = false;
		try {
			// -------- Initialize ---------
			//	先做一些赋值操作
			LOG.debug("Initializing {}.", getName());

			asyncOperationsThreadPool = Executors.newCachedThreadPool();

			CheckpointExceptionHandlerFactory cpExceptionHandlerFactory = createCheckpointExceptionHandlerFactory();

			synchronousCheckpointExceptionHandler = cpExceptionHandlerFactory.createCheckpointExceptionHandler(
				getExecutionConfig().isFailTaskOnCheckpointError(),
				getEnvironment());

			asynchronousCheckpointExceptionHandler = new AsyncCheckpointExceptionHandler(this);


			//创建状态存储后端
			stateBackend = createStateBackend();
			checkpointStorage = stateBackend.createCheckpointStorage(getEnvironment().getJobID());

			// if the clock is not already set, then assign a default TimeServiceProvider
			// 处理timer
			/**
			 * timerService 是一个计时器服务
			 * 将计时器作为StreamTask中的服务集成，StreamOperators可以通过调用StreamingRuntimeContext上的方法来使用它。
			 * 这也确保了不能与StreamOperator上的其他方法同时调用计时器回调。 ITCase确保了这种行为。
			 */
			if (timerService == null) {
				ThreadFactory timerThreadFactory = new DispatcherThreadFactory(TRIGGER_THREAD_GROUP,
					"Time Trigger for " + getName(), getUserCodeClassLoader());

				timerService = new SystemProcessingTimeService(this, getCheckpointLock(), timerThreadFactory);
			}

			//创建 OperatorChain，会加载每一个 operator，并调用 setup 方法
			//把之前JobGraph串起来的chain的信息形成实现，chain 操作
			/**
			 * 第二个要注意的是chain操作。前面提到了，flink会出于优化的角度，把一些算子chain成一个 整体的算子作为一个task来执行。
			 * 比如wordcount例子中，Source和FlatMap算子就被chain在了一起。在进行chain操作的时候，会设定头节点，并且指定输出的RecordWriter。
			 * 具体的原理
			 *
			 * 这里就看出来了，OperatorChain不是rpc传过来的，是任务在真正执行时 new 出来的；
			 */
			operatorChain = new OperatorChain<>(this, streamRecordWriters);  // <<<<<<<<<<<< 猜测一下：应该是从environment中取出 StreamConfig，然后构建OperatorChain的， yes
			headOperator = operatorChain.getHeadOperator();

			// task specific initialization
			//这个init操作的起名非常诡异，
			//因为这里主要是处理算子采用了自定义的checkpoint检查机制的情况，但是起了一个非常大众脸的名字
			init();    // <<<<<<<<<<<<<  // 和具体 StreamTask 子类相关的初始化操作

			// save the work of reloading state, etc, if the task is already canceled
			if (canceled) {
				throw new CancelTaskException();
			}

			// -------- Invoke --------
			LOG.debug("Invoking {}", getName());

			// we need to make sure that any triggers scheduled in open() cannot be
			// executed before all operators are opened
			synchronized (lock) {

				// both the following operations are protected by the lock
				// so that we avoid race conditions in the case that initializeState()
				// registers a timer, that fires before the open() is called.

				//初始化操作符状态，主要是一些state啥的
				/**
				 * 初始化每个算子的状态，初始化的对象变成了各个operator。如果是有 checkpoint的，那就从state信息里恢复，不然就作为全新的算子处理。
				 * 从源码中可以看到，flink针对keyed算子和普通算子做了不同的处理。keyed算子在初始化时需要计算出一个 group区间，
				 * 这个区间的值在整个生命周期里都不会再变化，后面key就会根据hash的不同结果，分配到特定的group中去计算。
				 * 顺便提一句，flink的keyed算子保存的是对每个数据的key的计算方法，而非真实的key，用户需要自己保证对每一行数据提供的keySelector的幂等性。
				 * 至于为什么要用KeyGroup的设计，这就牵扯到扩容的范畴了，将在后面的章节进行讲述。
				 */
				initializeState();  //状态初始化
				//对于富操作符，执行其open操作
				// 就是对各种RichOperator执行其open方法，通常可用于在执行计算之前加载资源。
				openAllOperators();
			}

			// final check to exit early before starting to run
			if (canceled) {
				throw new CancelTaskException();
			}

			// let the task do its work
			//真正开始执行的代码
			isRunning = true;

			/**
			 * 最终调用到run方法，该方法经过一系列跳转，最终调用chain上的第一个算子的run方法。
			 * 在wordcount的例子中，它最终调用了SocketTextStreamFunction的run，建立socket连接并读入文本。
			 */
			run();  //开始处理数据，这里通常是个循环, 模板方法，不同的子类，对应不用的实现

			// if this left the run() method cleanly despite the fact that this was canceled,
			// make sure the "clean shutdown" is not attempted
			if (canceled) {
				throw new CancelTaskException();
			}

			LOG.debug("Finished task {}", getName());

			// make sure no further checkpoint and notification actions happen.
			// we make sure that no other thread is currently in the locked scope before
			// we close the operators by trying to acquire the checkpoint scope lock
			// we also need to make sure that no triggers fire concurrently with the close logic
			// at the same time, this makes sure that during any "regular" exit where still
			synchronized (lock) {
				// this is part of the main logic, so if this fails, the task is considered failed
				closeAllOperators();

				// make sure no new timers can come
				timerService.quiesce();

				// only set the StreamTask to not running after all operators have been closed!
				// See FLINK-7430
				isRunning = false;
			}

			// make sure all timers finish
			timerService.awaitPendingAfterQuiesce();

			LOG.debug("Closed operators for task {}", getName());

			// make sure all buffered data is flushed
			operatorChain.flushOutputs();

			// make an attempt to dispose the operators such that failures in the dispose call
			// still let the computation fail
			tryDisposeAllOperators();
			disposed = true;
		}
		finally {
			// clean up everything we initialized
			isRunning = false;

			// Now that we are outside the user code, we do not want to be interrupted further
			// upon cancellation. The shutdown logic below needs to make sure it does not issue calls
			// that block and stall shutdown.
			// Additionally, the cancellation watch dog will issue a hard-cancel (kill the TaskManager
			// process) as a backup in case some shutdown procedure blocks outside our control.
			setShouldInterruptOnCancel(false);

			// clear any previously issued interrupt for a more graceful shutdown
			Thread.interrupted();

			// stop all timers and threads
			tryShutdownTimerService();

			// stop all asynchronous checkpoint threads
			try {
				cancelables.close();
				shutdownAsyncThreads();
			}
			catch (Throwable t) {
				// catch and log the exception to not replace the original exception
				LOG.error("Could not shut down async checkpoint threads", t);
			}

			// we must! perform this cleanup
			try {
				cleanup();
			}
			catch (Throwable t) {
				// catch and log the exception to not replace the original exception
				LOG.error("Error during cleanup of stream task", t);
			}

			// if the operators were not disposed before, do a hard dispose
			if (!disposed) {
				disposeAllOperators();
			}

			// release the output resources. this method should never fail.
			if (operatorChain != null) {
				// beware: without synchronization, #performCheckpoint() may run in
				//         parallel and this call is not thread-safe
				synchronized (lock) {
					operatorChain.releaseOutputs();
				}
			}
		}
	}

	@Override
	public final void cancel() throws Exception {
		isRunning = false;
		canceled = true;

		// the "cancel task" call must come first, but the cancelables must be
		// closed no matter what
		try {
			cancelTask();
		}
		finally {
			cancelables.close();
		}
	}

	public final boolean isRunning() {
		return isRunning;
	}

	public final boolean isCanceled() {
		return canceled;
	}

	/**
	 * Execute {@link StreamOperator#open()} of each operator in the chain of this
	 * {@link StreamTask}. Opening happens from <b>tail to head</b> operator in the chain, contrary
	 * to {@link StreamOperator#close()} which happens <b>head to tail</b>
	 * (see {@link #closeAllOperators()}.
	 */
	private void openAllOperators() throws Exception {
		for (StreamOperator<?> operator : operatorChain.getAllOperators()) {
			if (operator != null) {
				operator.open();
			}
		}
	}

	/**
	 * Execute {@link StreamOperator#close()} of each operator in the chain of this
	 * {@link StreamTask}. Closing happens from <b>head to tail</b> operator in the chain,
	 * contrary to {@link StreamOperator#open()} which happens <b>tail to head</b>
	 * (see {@link #openAllOperators()}.
	 */
	private void closeAllOperators() throws Exception {
		// We need to close them first to last, since upstream operators in the chain might emit
		// elements in their close methods.
		StreamOperator<?>[] allOperators = operatorChain.getAllOperators();
		for (int i = allOperators.length - 1; i >= 0; i--) {
			StreamOperator<?> operator = allOperators[i];
			if (operator != null) {
				operator.close();
			}
		}
	}

	/**
	 * Execute {@link StreamOperator#dispose()} of each operator in the chain of this
	 * {@link StreamTask}. Disposing happens from <b>tail to head</b> operator in the chain.
	 */
	private void tryDisposeAllOperators() throws Exception {
		for (StreamOperator<?> operator : operatorChain.getAllOperators()) {
			if (operator != null) {
				operator.dispose();
			}
		}
	}

	private void shutdownAsyncThreads() throws Exception {
		if (!asyncOperationsThreadPool.isShutdown()) {
			asyncOperationsThreadPool.shutdownNow();
		}
	}

	/**
	 * Execute @link StreamOperator#dispose()} of each operator in the chain of this
	 * {@link StreamTask}. Disposing happens from <b>tail to head</b> operator in the chain.
	 *
	 * <p>The difference with the {@link #tryDisposeAllOperators()} is that in case of an
	 * exception, this method catches it and logs the message.
	 */
	private void disposeAllOperators() {
		if (operatorChain != null) {
			for (StreamOperator<?> operator : operatorChain.getAllOperators()) {
				try {
					if (operator != null) {
						operator.dispose();
					}
				}
				catch (Throwable t) {
					LOG.error("Error during disposal of stream operator.", t);
				}
			}
		}
	}

	/**
	 * The finalize method shuts down the timer. This is a fail-safe shutdown, in case the original
	 * shutdown method was never called.
	 *
	 * <p>This should not be relied upon! It will cause shutdown to happen much later than if manual
	 * shutdown is attempted, and cause threads to linger for longer than needed.
	 */
	@Override
	protected void finalize() throws Throwable {
		super.finalize();
		if (timerService != null) {
			if (!timerService.isTerminated()) {
				LOG.info("Timer service is shutting down.");
				timerService.shutdownService();
			}
		}

		cancelables.close();
	}

	boolean isSerializingTimestamps() {
		TimeCharacteristic tc = configuration.getTimeCharacteristic();
		return tc == TimeCharacteristic.EventTime | tc == TimeCharacteristic.IngestionTime;
	}

	// ------------------------------------------------------------------------
	//  Access to properties and utilities
	// ------------------------------------------------------------------------

	/**
	 * Gets the name of the task, in the form "taskname (2/5)".
	 * @return The name of the task.
	 */
	public String getName() {
		return getEnvironment().getTaskInfo().getTaskNameWithSubtasks();
	}

	/**
	 * Gets the lock object on which all operations that involve data and state mutation have to lock.
	 * @return The checkpoint lock object.
	 */
	public Object getCheckpointLock() {
		return lock;
	}

	public CheckpointStorage getCheckpointStorage() {
		return checkpointStorage;
	}

	public StreamConfig getConfiguration() {
		return configuration;
	}

	public Map<String, Accumulator<?, ?>> getAccumulatorMap() {
		return accumulatorMap;
	}

	public StreamStatusMaintainer getStreamStatusMaintainer() {
		return operatorChain;
	}

	RecordWriterOutput<?>[] getStreamOutputs() {
		return operatorChain.getStreamOutputs();
	}

	// ------------------------------------------------------------------------
	//  Checkpoint and Restore
	// checkpoint 和 重启 功能, Task 的invokeable 指的就是这个 StreamTask，
	// Task 的 invokable.triggerCheckpoint(checkpointMetaData, checkpointOptions) 调用到这里
	// ------------------------------------------------------------------------

	// Source Task 的 invokable.triggerCheckpoint(checkpointMetaData, checkpointOptions) 调用到这里
	@Override
	public boolean triggerCheckpoint(CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions) throws Exception {
		try {
			// No alignment if we inject a checkpoint
			CheckpointMetrics checkpointMetrics = new CheckpointMetrics()
					.setBytesBufferedInAlignment(0L)
					.setAlignmentDurationNanos(0L);

			return performCheckpoint(checkpointMetaData, checkpointOptions, checkpointMetrics);  // 这里
		}
		catch (Exception e) {
			// propagate exceptions only if the task is still in "running" state
			if (isRunning) {
				throw new Exception("Could not perform checkpoint " + checkpointMetaData.getCheckpointId() +
					" for operator " + getName() + '.', e);
			} else {
				LOG.debug("Could not perform checkpoint {} for operator {} while the " +
					"invokable was not in state running.", checkpointMetaData.getCheckpointId(), getName(), e);
				return false;
			}
		}
	}

	/**
	 * 下游的Task(非Source的Task) 在收到barrier后触发这个方法
	 * CheckpointBarrierHandler 调用这个方法通知的当前StreamTask
	 */
	@Override
	public void triggerCheckpointOnBarrier(
			CheckpointMetaData checkpointMetaData,
			CheckpointOptions checkpointOptions,
			CheckpointMetrics checkpointMetrics) throws Exception {

		try {
			performCheckpoint(checkpointMetaData, checkpointOptions, checkpointMetrics);// <<<<<< 看这里，也是调用的这个方法，也就是说所有StreamTask处理checkpoint都是一样的逻辑
		}
		catch (CancelTaskException e) {
			LOG.info("Operator {} was cancelled while performing checkpoint {}.",
					getName(), checkpointMetaData.getCheckpointId());
			throw e;
		}
		catch (Exception e) {
			throw new Exception("Could not perform checkpoint " + checkpointMetaData.getCheckpointId() + " for operator " +
				getName() + '.', e);
		}
	}

	@Override
	public void abortCheckpointOnBarrier(long checkpointId, Throwable cause) throws Exception {
		LOG.debug("Aborting checkpoint via cancel-barrier {} for task {}", checkpointId, getName());

		// notify the coordinator that we decline this checkpoint
		getEnvironment().declineCheckpoint(checkpointId, cause);

		// notify all downstream operators that they should not wait for a barrier from us
		synchronized (lock) {
			operatorChain.broadcastCheckpointCancelMarker(checkpointId);
		}
	}

	//StreamTask checkpoint 的实现
	// 注意，从这里开始，整个执行链路上开始出现Barrier，而且所有StreamTask(包括SourceTask)处理checkpoint都是一样的逻辑
	private boolean performCheckpoint(
			CheckpointMetaData checkpointMetaData,
			CheckpointOptions checkpointOptions,
			CheckpointMetrics checkpointMetrics) throws Exception {

		LOG.debug("Starting checkpoint ({}) {} on task {}",
			checkpointMetaData.getCheckpointId(), checkpointOptions.getCheckpointType(), getName());

		synchronized (lock) {
			if (isRunning) {
				// 1. 如果task还在运行，那就可以进行checkpoint。方法是先向下游所有出口广播一个 Barrier，然后触发本task的State保存。
				// we can do a checkpoint

				// All of the following steps happen as an atomic step from the perspective of barriers and
				// records/watermarks/timers/callbacks.
				// We generally try to emit the checkpoint barrier as soon as possible to not affect downstream
				// checkpoint alignments

				// Step (1): Prepare the checkpoint, allow operators to do some pre-barrier work.
				//           The pre-barrier work should be nothing or minimal in the common case.
				// 第一步：准备进行checkpoint，让所有 operator 做一些准备工作
				operatorChain.prepareSnapshotPreBarrier(checkpointMetaData.getCheckpointId());

				// Step (2): Send the checkpoint barrier downstream
				// 第二步：先往下游发送 barrier，点进去继续进行分析， 注意，此时上游所有inputChannel的barrier都已经到了；
				operatorChain.broadcastCheckpointBarrier(
						checkpointMetaData.getCheckpointId(),
						checkpointMetaData.getTimestamp(),
						checkpointOptions);

				// Step (3): Take the state snapshot. This should be largely asynchronous, to not impact progress of the streaming topology
				// 第三步：进行状态快照，这应该是异步进行的，不影响流系统其他的处理进度
				// 完成 broadcastCheckpointBarrier 方法后，在 checkpointState() 方法中，StreamTask还 做了很多别的工作：
				checkpointState(checkpointMetaData, checkpointOptions, checkpointMetrics);  // <<<<<< 这里，核心  <<<<<<<<<<
				return true;
			}
			else {
				// 2. 如果task结束了，那我们就要通知下游取消本次checkpoint，方法是发送一个 CancelCheckpointMarker，这是类似于Barrier的另一种消息。
				// we cannot perform our checkpoint - let the downstream operators know that they
				// should not wait for any input from this operator

				// we cannot broadcast the cancellation markers on the 'operator chain', because it may not
				// yet be created
				final CancelCheckpointMarker message = new CancelCheckpointMarker(checkpointMetaData.getCheckpointId());
				Exception exception = null;

				for (StreamRecordWriter<SerializationDelegate<StreamRecord<OUT>>> streamRecordWriter : streamRecordWriters) {
					try {
						streamRecordWriter.broadcastEvent(message);
					} catch (Exception e) {
						exception = ExceptionUtils.firstOrSuppressed(
							new Exception("Could not send cancel checkpoint marker to downstream tasks.", e),
							exception);
					}
				}

				if (exception != null) {
					throw exception;
				}

				return false;
			}
		}
	}

	public ExecutorService getAsyncOperationsThreadPool() {
		return asyncOperationsThreadPool;
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		synchronized (lock) {
			if (isRunning) {
				LOG.debug("Notification of complete checkpoint for task {}", getName());

				for (StreamOperator<?> operator : operatorChain.getAllOperators()) {
					if (operator != null) {
						operator.notifyCheckpointComplete(checkpointId);
					}
				}
			}
			else {
				LOG.debug("Ignoring notification of complete checkpoint for not-running task {}", getName());
			}
		}
	}

	private void tryShutdownTimerService() {

		if (timerService != null && !timerService.isTerminated()) {

			try {
				final long timeoutMs = getEnvironment().getTaskManagerInfo().getConfiguration().
					getLong(TaskManagerOptions.TASK_CANCELLATION_TIMEOUT_TIMERS);

				if (!timerService.shutdownServiceUninterruptible(timeoutMs)) {
					LOG.warn("Timer service shutdown exceeded time limit of {} ms while waiting for pending " +
						"timers. Will continue with shutdown procedure.", timeoutMs);
				}
			} catch (Throwable t) {
				// catch and log the exception to not replace the original exception
				LOG.error("Could not shut down timer service", t);
			}
		}
	}

	/**
	 * >>>>>>>>>>>>>>>>>>>>>>>> 核心 <<<<<<<<<<<<<<<<<<<<<<<
	 * 在往下游发送完barrier之后，开始保存算子的state
	 *
	 * CheckpointStorage 是对状态存储系统的抽象，它有两个不同的实现，分别是 MemoryBackendCheckpointStorage 和 FsCheckpointStorage。CheckpointStorage则是从statebackend中生成的；
	 * MemoryBackendCheckpointStorage 会将所有算子的检查点状态存储在 JobManager 的内存中，通常不适合在生产环境中使用；
	 * 而 FsCheckpointStorage 则会把所有算子的检查点状态持久化存储在文件系统中。
	 * CheckpointStorageLocation 是对检查点状态存储位置的一个抽象，它能够提供获取检查点输出流的方法，通过输出流将状态和元数据写入到存储系统中。输出流关闭时可以获得状态句柄（StateHandle），后面可以使用句柄重新读取写入的状态。
	 */
	private void checkpointState(
			CheckpointMetaData checkpointMetaData,
			CheckpointOptions checkpointOptions,
			CheckpointMetrics checkpointMetrics) throws Exception {

		//1. 解析得到 CheckpointStorageLocation
		CheckpointStreamFactory storage = checkpointStorage.resolveCheckpointStorageLocation(
				checkpointMetaData.getCheckpointId(),
				checkpointOptions.getTargetLocation());

		//2. 将存储过程封装为 CheckpointingOperation，开始进行检查点存储操作
		CheckpointingOperation checkpointingOperation = new CheckpointingOperation(
			this,
			checkpointMetaData,
			checkpointOptions,
			storage,
			checkpointMetrics);

		checkpointingOperation.executeCheckpointing(); //执行存储过程
	}

	//初始化每个算子的状态，在该方法中，如果task是从失败中恢复的话，其保存的state也会被restore进来。
	private void initializeState() throws Exception {

		StreamOperator<?>[] allOperators = operatorChain.getAllOperators();

		for (StreamOperator<?> operator : allOperators) {
			if (null != operator) {
				operator.initializeState();
			}
		}
	}

	// ------------------------------------------------------------------------
	//  State backend
	// ------------------------------------------------------------------------

	private StateBackend createStateBackend() throws Exception {
		final StateBackend fromApplication = configuration.getStateBackend(getUserCodeClassLoader());

		return StateBackendLoader.fromApplicationOrConfigOrDefault(
				fromApplication,
				getEnvironment().getTaskManagerInfo().getConfiguration(),
				getUserCodeClassLoader(),
				LOG);
	}

	protected CheckpointExceptionHandlerFactory createCheckpointExceptionHandlerFactory() {
		return new CheckpointExceptionHandlerFactory();
	}

	/**
	 * Returns the {@link ProcessingTimeService} responsible for telling the current
	 * processing time and registering timers.
	 */
	public ProcessingTimeService getProcessingTimeService() {
		if (timerService == null) {
			throw new IllegalStateException("The timer service has not been initialized.");
		}
		return timerService;
	}

	/**
	 * Handles an exception thrown by another thread (e.g. a TriggerTask),
	 * other than the one executing the main task by failing the task entirely.
	 *
	 * <p>In more detail, it marks task execution failed for an external reason
	 * (a reason other than the task code itself throwing an exception). If the task
	 * is already in a terminal state (such as FINISHED, CANCELED, FAILED), or if the
	 * task is already canceling this does nothing. Otherwise it sets the state to
	 * FAILED, and, if the invokable code is running, starts an asynchronous thread
	 * that aborts that code.
	 *
	 * <p>This method never blocks.
	 */
	@Override
	public void handleAsyncException(String message, Throwable exception) {
		if (isRunning) {
			// only fail if the task is still running
			getEnvironment().failExternally(exception);
		}
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		return getName();
	}

	// ------------------------------------------------------------------------

	/**
	 * This runnable executes the asynchronous parts of all involved backend snapshots for the subtask.
	 *
	 * checkpoint 异步执行的部分
	 * 主要就是异步完成上面所有算子的OperatorSnapshotFutures(如果之前的模式是同步的，那这里本身就是已经完成的)，然后向 CheckpointCoordinator 汇报，此次checkpoint成功 (rpc到jobMaster)
	 */
	@VisibleForTesting
	protected static final class AsyncCheckpointRunnable implements Runnable, Closeable {

		private final StreamTask<?, ?> owner;

		private final Map<OperatorID, OperatorSnapshotFutures> operatorSnapshotsInProgress;

		private final CheckpointMetaData checkpointMetaData;
		private final CheckpointMetrics checkpointMetrics;

		private final long asyncStartNanos;

		private final AtomicReference<CheckpointingOperation.AsyncCheckpointState> asyncCheckpointState = new AtomicReference<>(
			CheckpointingOperation.AsyncCheckpointState.RUNNING);

		AsyncCheckpointRunnable(
			StreamTask<?, ?> owner,
			Map<OperatorID, OperatorSnapshotFutures> operatorSnapshotsInProgress,
			CheckpointMetaData checkpointMetaData,
			CheckpointMetrics checkpointMetrics,
			long asyncStartNanos) {

			this.owner = Preconditions.checkNotNull(owner);
			this.operatorSnapshotsInProgress = Preconditions.checkNotNull(operatorSnapshotsInProgress);
			this.checkpointMetaData = Preconditions.checkNotNull(checkpointMetaData);
			this.checkpointMetrics = Preconditions.checkNotNull(checkpointMetrics);
			this.asyncStartNanos = asyncStartNanos;
		}

		//核心，run()方法；
		@Override
		public void run() {
			FileSystemSafetyNet.initializeSafetyNetForThread();
			try {

				TaskStateSnapshot jobManagerTaskOperatorSubtaskStates =
					new TaskStateSnapshot(operatorSnapshotsInProgress.size());

				TaskStateSnapshot localTaskOperatorSubtaskStates =
					new TaskStateSnapshot(operatorSnapshotsInProgress.size());

				// 完成每一个 operator 的状态写入
				// 如果是同步 checkpoint，那么在此之前状态已经写入完成
				// 如果是异步 checkpoint，那么在这里才会写入状态
				for (Map.Entry<OperatorID, OperatorSnapshotFutures> entry : operatorSnapshotsInProgress.entrySet()) {

					OperatorID operatorID = entry.getKey();
					OperatorSnapshotFutures snapshotInProgress = entry.getValue();

					// finalize the async part of all by executing all snapshot runnables
					OperatorSnapshotFinalizer finalizedSnapshots =
						new OperatorSnapshotFinalizer(snapshotInProgress);

					jobManagerTaskOperatorSubtaskStates.putSubtaskStateByOperatorID(
						operatorID,
						finalizedSnapshots.getJobManagerOwnedState());

					localTaskOperatorSubtaskStates.putSubtaskStateByOperatorID(
						operatorID,
						finalizedSnapshots.getTaskLocalState());
				}

				final long asyncEndNanos = System.nanoTime();
				final long asyncDurationMillis = (asyncEndNanos - asyncStartNanos) / 1_000_000L;

				checkpointMetrics.setAsyncDurationMillis(asyncDurationMillis);

				if (asyncCheckpointState.compareAndSet(CheckpointingOperation.AsyncCheckpointState.RUNNING,
					CheckpointingOperation.AsyncCheckpointState.COMPLETED)) {

					// >>>>>>>>> 报告 snapshot 完成  (所有算子的 snapshot 都已完成)
					reportCompletedSnapshotStates(
						jobManagerTaskOperatorSubtaskStates,
						localTaskOperatorSubtaskStates,
						asyncDurationMillis);

				} else {
					LOG.debug("{} - asynchronous part of checkpoint {} could not be completed because it was closed before.",
						owner.getName(),
						checkpointMetaData.getCheckpointId());
				}
			} catch (Exception e) {
				handleExecutionException(e);
			} finally {
				owner.cancelables.unregisterCloseable(this);
				FileSystemSafetyNet.closeSafetyNetAndGuardedResourcesForThread();
			}
		}

		/**
		 * 报告 snapshot 完成
		 * 最终调用到 TaskStateManagerImpl 的 reportTaskStateSnapshots()方法，
		 * 主要逻辑其实就是向 CheckpointCoordinator 发送ACK，表示当前算子的checkpoint已经完成 (rpc到jobMaster)
		 */
		private void reportCompletedSnapshotStates(
			TaskStateSnapshot acknowledgedTaskStateSnapshot,
			TaskStateSnapshot localTaskStateSnapshot,
			long asyncDurationMillis) {

			TaskStateManager taskStateManager = owner.getEnvironment().getTaskStateManager();

			boolean hasAckState = acknowledgedTaskStateSnapshot.hasState();
			boolean hasLocalState = localTaskStateSnapshot.hasState();

			Preconditions.checkState(hasAckState || !hasLocalState,
				"Found cached state but no corresponding primary state is reported to the job " +
					"manager. This indicates a problem.");

			// we signal stateless tasks by reporting null, so that there are no attempts to assign empty state
			// to stateless tasks on restore. This enables simple job modifications that only concern
			// stateless without the need to assign them uids to match their (always empty) states.
			taskStateManager.reportTaskStateSnapshots(  //这里
				checkpointMetaData,
				checkpointMetrics,
				hasAckState ? acknowledgedTaskStateSnapshot : null,
				hasLocalState ? localTaskStateSnapshot : null);

			LOG.debug("{} - finished asynchronous part of checkpoint {}. Asynchronous duration: {} ms",
				owner.getName(), checkpointMetaData.getCheckpointId(), asyncDurationMillis);

			LOG.trace("{} - reported the following states in snapshot for checkpoint {}: {}.",
				owner.getName(), checkpointMetaData.getCheckpointId(), acknowledgedTaskStateSnapshot);
		}

		private void handleExecutionException(Exception e) {

			boolean didCleanup = false;
			CheckpointingOperation.AsyncCheckpointState currentState = asyncCheckpointState.get();

			while (CheckpointingOperation.AsyncCheckpointState.DISCARDED != currentState) {

				if (asyncCheckpointState.compareAndSet(
					currentState,
					CheckpointingOperation.AsyncCheckpointState.DISCARDED)) {

					didCleanup = true;

					try {
						cleanup();
					} catch (Exception cleanupException) {
						e.addSuppressed(cleanupException);
					}

					Exception checkpointException = new Exception(
						"Could not materialize checkpoint " + checkpointMetaData.getCheckpointId() + " for operator " +
							owner.getName() + '.',
						e);

					// We only report the exception for the original cause of fail and cleanup.
					// Otherwise this followup exception could race the original exception in failing the task.
					owner.asynchronousCheckpointExceptionHandler.tryHandleCheckpointException(
						checkpointMetaData,
						checkpointException);

					currentState = CheckpointingOperation.AsyncCheckpointState.DISCARDED;
				} else {
					currentState = asyncCheckpointState.get();
				}
			}

			if (!didCleanup) {
				LOG.trace("Caught followup exception from a failed checkpoint thread. This can be ignored.", e);
			}
		}

		@Override
		public void close() {
			if (asyncCheckpointState.compareAndSet(
				CheckpointingOperation.AsyncCheckpointState.RUNNING,
				CheckpointingOperation.AsyncCheckpointState.DISCARDED)) {

				try {
					cleanup();
				} catch (Exception cleanupException) {
					LOG.warn("Could not properly clean up the async checkpoint runnable.", cleanupException);
				}
			} else {
				logFailedCleanupAttempt();
			}
		}

		private void cleanup() throws Exception {
			LOG.debug(
				"Cleanup AsyncCheckpointRunnable for checkpoint {} of {}.",
				checkpointMetaData.getCheckpointId(),
				owner.getName());

			Exception exception = null;

			// clean up ongoing operator snapshot results and non partitioned state handles
			for (OperatorSnapshotFutures operatorSnapshotResult : operatorSnapshotsInProgress.values()) {
				if (operatorSnapshotResult != null) {
					try {
						operatorSnapshotResult.cancel();
					} catch (Exception cancelException) {
						exception = ExceptionUtils.firstOrSuppressed(cancelException, exception);
					}
				}
			}

			if (null != exception) {
				throw exception;
			}
		}

		private void logFailedCleanupAttempt() {
			LOG.debug("{} - asynchronous checkpointing operation for checkpoint {} has " +
					"already been completed. Thus, the state handles are not cleaned up.",
				owner.getName(),
				checkpointMetaData.getCheckpointId());
		}
	}

	public CloseableRegistry getCancelables() {
		return cancelables;
	}

	// ------------------------------------------------------------------------

	/**
	 * 检查点快照的过程被封装为 CheckpointingOperation
	 *
	 * CheckpointingOperation 中，快照操作分为两个阶段，第一阶段是同步执行的，第二阶段是异步执行的：
	 *
	 */
	private static final class CheckpointingOperation {

		private final StreamTask<?, ?> owner;

		private final CheckpointMetaData checkpointMetaData;
		private final CheckpointOptions checkpointOptions;
		private final CheckpointMetrics checkpointMetrics;
		private final CheckpointStreamFactory storageLocation;

		private final StreamOperator<?>[] allOperators;

		private long startSyncPartNano;
		private long startAsyncPartNano;

		// ------------------------

		//由于每一个StreamTask可能包含多个算子，因而内部使用一个 Map 维护 OperatorID -> OperatorSnapshotFutures 的关系。 OperatorSnapshotFutures是一个算子进行checkpoint过程的结果
		private final Map<OperatorID, OperatorSnapshotFutures> operatorSnapshotsInProgress;

		public CheckpointingOperation(
				StreamTask<?, ?> owner,
				CheckpointMetaData checkpointMetaData,
				CheckpointOptions checkpointOptions,
				CheckpointStreamFactory checkpointStorageLocation,
				CheckpointMetrics checkpointMetrics) {

			this.owner = Preconditions.checkNotNull(owner);
			this.checkpointMetaData = Preconditions.checkNotNull(checkpointMetaData);
			this.checkpointOptions = Preconditions.checkNotNull(checkpointOptions);
			this.checkpointMetrics = Preconditions.checkNotNull(checkpointMetrics);
			this.storageLocation = Preconditions.checkNotNull(checkpointStorageLocation);
			this.allOperators = owner.operatorChain.getAllOperators();
			this.operatorSnapshotsInProgress = new HashMap<>(allOperators.length);
		}

		//执行检查点快照
		public void executeCheckpointing() throws Exception {
			startSyncPartNano = System.nanoTime();

			try {
				//1. 同步执行的部分 (这里也不是同步执行的呀，为什么说是同步执行的呢？)
				//底层源码一直往下追就明白了，这里可以是同步执行的，也可以是异步执行的，取决于配置；
				for (StreamOperator<?> op : allOperators) {
					checkpointStreamOperator(op);  //<<< 这里，每个算子的执行结果(可能还没执行完)已经放在了operatorSnapshotsInProgress这个容器中
				}

				//此时operatorSnapshotsInProgress这个容器已经填充完了，所有算子的执行结果描述都放在了这个算子中；

				if (LOG.isDebugEnabled()) {
					LOG.debug("Finished synchronous checkpoints for checkpoint {} on task {}",
						checkpointMetaData.getCheckpointId(), owner.getName());
				}

				startAsyncPartNano = System.nanoTime();

				checkpointMetrics.setSyncDurationMillis((startAsyncPartNano - startSyncPartNano) / 1_000_000);

				// we are transferring ownership over snapshotInProgressList for cleanup to the thread, active on submit
				// 2. 异步执行的部分
				// checkpoint 可以配置成同步执行，也可以配置成异步执行的
				// 如果是同步执行的，在这里实际上所有的 runnable future 都是已经完成的状态

				//主要就是异步完成上面所有算子的OperatorSnapshotFutures(如果之前的模式是同步的，那这里本身就是已经完成的)，然后向 CheckpointCoordinator 汇报，此次checkpoint成功 (rpc到jobMaster)  所以接下来的代码跳到 CheckpointCoordinator.receiveAcknowledgeMessage
				AsyncCheckpointRunnable asyncCheckpointRunnable = new AsyncCheckpointRunnable(
					owner,
					operatorSnapshotsInProgress,
					checkpointMetaData,
					checkpointMetrics,
					startAsyncPartNano);

				owner.cancelables.registerCloseable(asyncCheckpointRunnable);
				owner.asyncOperationsThreadPool.submit(asyncCheckpointRunnable);

				if (LOG.isDebugEnabled()) {
					LOG.debug("{} - finished synchronous part of checkpoint {}." +
							"Alignment duration: {} ms, snapshot duration {} ms",
						owner.getName(), checkpointMetaData.getCheckpointId(),
						checkpointMetrics.getAlignmentDurationNanos() / 1_000_000,
						checkpointMetrics.getSyncDurationMillis());
				}
			} catch (Exception ex) {
				// Cleanup to release resources
				for (OperatorSnapshotFutures operatorSnapshotResult : operatorSnapshotsInProgress.values()) {
					if (null != operatorSnapshotResult) {
						try {
							operatorSnapshotResult.cancel();
						} catch (Exception e) {
							LOG.warn("Could not properly cancel an operator snapshot result.", e);
						}
					}
				}

				if (LOG.isDebugEnabled()) {
					LOG.debug("{} - did NOT finish synchronous part of checkpoint {}." +
							"Alignment duration: {} ms, snapshot duration {} ms",
						owner.getName(), checkpointMetaData.getCheckpointId(),
						checkpointMetrics.getAlignmentDurationNanos() / 1_000_000,
						checkpointMetrics.getSyncDurationMillis());
				}

				owner.synchronousCheckpointExceptionHandler.tryHandleCheckpointException(checkpointMetaData, ex);
			}
		}

		//对每个算子的状态进行checkpoint，主要就是调用算子的snapshotState()方法
		@SuppressWarnings("deprecation")
		private void checkpointStreamOperator(StreamOperator<?> op) throws Exception {
			if (null != op) {

				// 调用 StreamOperator.snapshotState 方法进行快照
				// 返回的结果是 runnable future，可能是已经执行完了，也可能没有执行完
				OperatorSnapshotFutures snapshotInProgress = op.snapshotState(  //看这里，调用了算子的 snapshotState() 方法， OperatorSnapshotFutures是一个算子进行checkpoint过程的结果
						checkpointMetaData.getCheckpointId(),
						checkpointMetaData.getTimestamp(),
						checkpointOptions,
						storageLocation);
				operatorSnapshotsInProgress.put(op.getOperatorID(), snapshotInProgress);
			}
		}

		private enum AsyncCheckpointState {
			RUNNING,
			DISCARDED,
			COMPLETED
		}
	}

	/**
	 * Wrapper for synchronous {@link CheckpointExceptionHandler}. This implementation catches unhandled, rethrown
	 * exceptions and reports them through {@link #handleAsyncException(String, Throwable)}. As this implementation
	 * always handles the exception in some way, it never rethrows.
	 */
	static final class AsyncCheckpointExceptionHandler implements CheckpointExceptionHandler {

		/** Owning stream task to which we report async exceptions. */
		final StreamTask<?, ?> owner;

		/** Synchronous exception handler to which we delegate. */
		final CheckpointExceptionHandler synchronousCheckpointExceptionHandler;

		AsyncCheckpointExceptionHandler(StreamTask<?, ?> owner) {
			this.owner = Preconditions.checkNotNull(owner);
			this.synchronousCheckpointExceptionHandler =
				Preconditions.checkNotNull(owner.synchronousCheckpointExceptionHandler);
		}

		@Override
		public void tryHandleCheckpointException(CheckpointMetaData checkpointMetaData, Exception exception) {
			try {
				synchronousCheckpointExceptionHandler.tryHandleCheckpointException(checkpointMetaData, exception);
			} catch (Exception unhandled) {
				AsynchronousException asyncException = new AsynchronousException(unhandled);
				owner.handleAsyncException("Failure in asynchronous checkpoint materialization", asyncException);
			}
		}
	}

	@VisibleForTesting
	public static <OUT> List<StreamRecordWriter<SerializationDelegate<StreamRecord<OUT>>>> createStreamRecordWriters(
			StreamConfig configuration,
			Environment environment) {
		List<StreamRecordWriter<SerializationDelegate<StreamRecord<OUT>>>> streamRecordWriters = new ArrayList<>();
		List<StreamEdge> outEdgesInOrder = configuration.getOutEdgesInOrder(environment.getUserClassLoader());
		Map<Integer, StreamConfig> chainedConfigs = configuration.getTransitiveChainedTaskConfigsWithSelf(environment.getUserClassLoader());

		for (int i = 0; i < outEdgesInOrder.size(); i++) {
			StreamEdge edge = outEdgesInOrder.get(i);
			streamRecordWriters.add(
				createStreamRecordWriter(
					edge,
					i,
					environment,
					environment.getTaskInfo().getTaskName(),
					chainedConfigs.get(edge.getSourceId()).getBufferTimeout()));
		}
		return streamRecordWriters;
	}

	private static <OUT> StreamRecordWriter<SerializationDelegate<StreamRecord<OUT>>> createStreamRecordWriter(
			StreamEdge edge,
			int outputIndex,
			Environment environment,
			String taskName,
			long bufferTimeout) {
		@SuppressWarnings("unchecked")
		StreamPartitioner<OUT> outputPartitioner = (StreamPartitioner<OUT>) edge.getPartitioner();

		LOG.debug("Using partitioner {} for output {} of task {}", outputPartitioner, outputIndex, taskName);

		ResultPartitionWriter bufferWriter = environment.getWriter(outputIndex);

		// we initialize the partitioner here with the number of key groups (aka max. parallelism)
		if (outputPartitioner instanceof ConfigurableStreamPartitioner) {
			int numKeyGroups = bufferWriter.getNumTargetKeyGroups();
			if (0 < numKeyGroups) {
				((ConfigurableStreamPartitioner) outputPartitioner).configure(numKeyGroups);
			}
		}

		StreamRecordWriter<SerializationDelegate<StreamRecord<OUT>>> output =
			new StreamRecordWriter<>(bufferWriter, outputPartitioner, bufferTimeout, taskName);
		output.setMetricGroup(environment.getMetricGroup().getIOMetricGroup());
		return output;
	}
}

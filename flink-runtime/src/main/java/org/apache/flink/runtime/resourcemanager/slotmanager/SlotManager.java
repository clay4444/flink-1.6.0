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

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.clusterframework.types.TaskManagerSlot;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.SlotRequest;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.SlotStatus;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.exceptions.SlotAllocationException;
import org.apache.flink.runtime.taskexecutor.exceptions.SlotOccupiedException;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * The slot manager is responsible for maintaining a view on all registered task manager slots,
 * their allocation and all pending slot requests. Whenever a new slot is registered or and
 * allocated slot is freed, then it tries to fulfill another pending slot request. Whenever there
 * are not enough slots available the slot manager will notify the resource manager about it via
 * {@link ResourceActions#allocateResource(ResourceProfile)}.
 *
 * <p>In order to free resources and avoid resource leaks, idling task managers (task managers whose
 * slots are currently not used) and pending slot requests time out triggering their release and
 * failure, respectively.
 *
 * SlotManager 维护了所有已经注册的TaskExecutor 的所有 slot 的状态，它们的分配情况；
 * 还维护了所有处于等待状态的 slot 请求；
 *
 * 每当有一个新的 slot 注册或者一个已经分配的 slot 被释放的时候，SlotManager 会试图去满足处于等待状态 slot request。
 * 如果可用的 slot 不足以满足要求，SlotManager 会通过 ResourceActions#allocateResource(ResourceProfile) 来告知 ResourceManager,
 * ResourceManager 可能会尝试启动新的 TaskExecutor (如 Yarn 模式下)。
 *
 * 此外，长时间处于空闲状态的 TaskExecutor 或者长时间没有被满足的 pending slot request，会触发超时机制进行处理。
 *
 * ResoureManager 实际上是通过 ->SlotManager<- 来管理 TaskExecutor 所注册的所有 slot，
 */
public class SlotManager implements AutoCloseable {
	private static final Logger LOG = LoggerFactory.getLogger(SlotManager.class);

	/** Scheduled executor for timeouts. */
	private final ScheduledExecutor scheduledExecutor;

	/** Timeout for slot requests to the task manager. */
	private final Time taskManagerRequestTimeout;

	/** Timeout after which an allocation is discarded. */
	private final Time slotRequestTimeout;

	/** Timeout after which an unused TaskManager is released. */
	private final Time taskManagerTimeout;

	/** Map for all registered slots. */
	private final HashMap<SlotID, TaskManagerSlot> slots;  //所有的 slot (具体点是TaskManagerSlot)

	/** Index of all currently free slots. */
	private final LinkedHashMap<SlotID, TaskManagerSlot> freeSlots;  //空闲的 slot

	/** All currently registered task managers. */
	private final HashMap<InstanceID, TaskManagerRegistration> taskManagerRegistrations;  //所有注册的tm

	/** Map of fulfilled and active allocations for request deduplication purposes. */
	private final HashMap<AllocationID, SlotID> fulfilledSlotRequests;

	/** Map of pending/unfulfilled slot allocation requests. */
	private final HashMap<AllocationID, PendingSlotRequest> pendingSlotRequests;  //在排队的 SlotRequest

	/** ResourceManager's id. */
	private ResourceManagerId resourceManagerId;

	/** Executor for future callbacks which have to be "synchronized". */
	private Executor mainThreadExecutor;

	/** Callbacks for resource (de-)allocations. */
	private ResourceActions resourceActions;

	private ScheduledFuture<?> taskManagerTimeoutCheck;

	private ScheduledFuture<?> slotRequestTimeoutCheck;

	/** True iff the component has been started. */
	private boolean started;

	public SlotManager(
			ScheduledExecutor scheduledExecutor,
			Time taskManagerRequestTimeout,
			Time slotRequestTimeout,
			Time taskManagerTimeout) {
		this.scheduledExecutor = Preconditions.checkNotNull(scheduledExecutor);
		this.taskManagerRequestTimeout = Preconditions.checkNotNull(taskManagerRequestTimeout);
		this.slotRequestTimeout = Preconditions.checkNotNull(slotRequestTimeout);
		this.taskManagerTimeout = Preconditions.checkNotNull(taskManagerTimeout);

		slots = new HashMap<>(16);
		freeSlots = new LinkedHashMap<>(16);
		taskManagerRegistrations = new HashMap<>(4);
		fulfilledSlotRequests = new HashMap<>(16);
		pendingSlotRequests = new HashMap<>(16);

		resourceManagerId = null;
		resourceActions = null;
		mainThreadExecutor = null;
		taskManagerTimeoutCheck = null;
		slotRequestTimeoutCheck = null;

		started = false;
	}

	public int getNumberRegisteredSlots() {
		return slots.size();
	}

	public int getNumberRegisteredSlotsOf(InstanceID instanceId) {
		TaskManagerRegistration taskManagerRegistration = taskManagerRegistrations.get(instanceId);

		if (taskManagerRegistration != null) {
			return taskManagerRegistration.getNumberRegisteredSlots();
		} else {
			return 0;
		}
	}

	public int getNumberFreeSlots() {
		return freeSlots.size();
	}

	public int getNumberFreeSlotsOf(InstanceID instanceId) {
		TaskManagerRegistration taskManagerRegistration = taskManagerRegistrations.get(instanceId);

		if (taskManagerRegistration != null) {
			return taskManagerRegistration.getNumberFreeSlots();
		} else {
			return 0;
		}
	}

	public int getNumberPendingSlotRequests() {return pendingSlotRequests.size(); }

	// ---------------------------------------------------------------------------------------------
	// Component lifecycle methods
	// 生命周期
	// ---------------------------------------------------------------------------------------------

	/**
	 * Starts the slot manager with the given leader id and resource manager actions.
	 *
	 * @param newResourceManagerId to use for communication with the task managers
	 * @param newMainThreadExecutor to use to run code in the ResourceManager's main thread
	 * @param newResourceActions to use for resource (de-)allocations
	 *
	 * start：
	 * 主要会启动两个周期性的检测任务，一个用于检测tm是否长时间处于空闲状态，另一个用于检测slot request 是否超时；
	 */
	public void start(ResourceManagerId newResourceManagerId, Executor newMainThreadExecutor, ResourceActions newResourceActions) {
		LOG.info("Starting the SlotManager.");

		this.resourceManagerId = Preconditions.checkNotNull(newResourceManagerId);
		mainThreadExecutor = Preconditions.checkNotNull(newMainThreadExecutor);
		resourceActions = Preconditions.checkNotNull(newResourceActions);

		started = true;

		//检查 TaskExecutor 是否长时间处于 idle 状态
		taskManagerTimeoutCheck = scheduledExecutor.scheduleWithFixedDelay(
			() -> mainThreadExecutor.execute(
				() -> checkTaskManagerTimeouts()),  //看这里，具体检查逻辑
			0L,
			taskManagerTimeout.toMilliseconds(),
			TimeUnit.MILLISECONDS);

		//检查 slot request 是否超时
		slotRequestTimeoutCheck = scheduledExecutor.scheduleWithFixedDelay(
			() -> mainThreadExecutor.execute(
				() -> checkSlotRequestTimeouts()), //看这里，具体检查逻辑
			0L,
			slotRequestTimeout.toMilliseconds(),
			TimeUnit.MILLISECONDS);
	}

	/**
	 * Suspends the component. This clears the internal state of the slot manager.
	 */
	public void suspend() {
		LOG.info("Suspending the SlotManager.");

		// stop the timeout checks for the TaskManagers and the SlotRequests
		if (taskManagerTimeoutCheck != null) {
			taskManagerTimeoutCheck.cancel(false);
			taskManagerTimeoutCheck = null;
		}

		if (slotRequestTimeoutCheck != null) {
			slotRequestTimeoutCheck.cancel(false);
			slotRequestTimeoutCheck = null;
		}

		for (PendingSlotRequest pendingSlotRequest : pendingSlotRequests.values()) {
			cancelPendingSlotRequest(pendingSlotRequest);
		}

		pendingSlotRequests.clear();

		ArrayList<InstanceID> registeredTaskManagers = new ArrayList<>(taskManagerRegistrations.keySet());

		for (InstanceID registeredTaskManager : registeredTaskManagers) {
			unregisterTaskManager(registeredTaskManager);
		}

		resourceManagerId = null;
		resourceActions = null;
		started = false;
	}

	/**
	 * Closes the slot manager.
	 *
	 * @throws Exception if the close operation fails
	 */
	@Override
	public void close() throws Exception {
		LOG.info("Closing the SlotManager.");

		suspend();
	}

	// ---------------------------------------------------------------------------------------------
	// Public API
	// ---------------------------------------------------------------------------------------------

	/**
	 * Requests a slot with the respective resource profile.
	 *
	 * @param slotRequest specifying the requested slot specs
	 * @return true if the slot request was registered; false if the request is a duplicate
	 * @throws SlotManagerException if the slot request failed (e.g. not enough resources left)
	 *
	 * rm通过这个方法来请求slot；
	 * SlotRequest 中封装了请求的 JobId, AllocationID 以及请求的资源描述 ResourceProfile；
	 * SlotManager 会将 slot request 进一步封装为 PendingSlotRequest, 意为一个尚未被满足要求的 slot request。然后放进pending request 集合中；
	 */
	public boolean registerSlotRequest(SlotRequest slotRequest) throws SlotManagerException {
		checkInit();

		if (checkDuplicateRequest(slotRequest.getAllocationId())) {
			LOG.debug("Ignoring a duplicate slot request with allocation id {}.", slotRequest.getAllocationId());

			return false;
		} else {
			//将请求封装为 PendingSlotRequest
			PendingSlotRequest pendingSlotRequest = new PendingSlotRequest(slotRequest);

			pendingSlotRequests.put(slotRequest.getAllocationId(), pendingSlotRequest); //放进pendingSlotRequests 中；

			try {
				internalRequestSlot(pendingSlotRequest);  //看这里，执行请求分配slot的逻辑
			} catch (ResourceManagerException e) {
				// requesting the slot failed --> remove pending slot request
				pendingSlotRequests.remove(slotRequest.getAllocationId());

				throw new SlotManagerException("Could not fulfill slot request " + slotRequest.getAllocationId() + '.', e);
			}

			return true;
		}
	}

	/**
	 * Cancels and removes a pending slot request with the given allocation id. If there is no such
	 * pending request, then nothing is done.
	 *
	 * @param allocationId identifying the pending slot request
	 * @return True if a pending slot request was found; otherwise false
	 *
	 * 取消一个slot request
	 */
	public boolean unregisterSlotRequest(AllocationID allocationId) {
		checkInit();

		PendingSlotRequest pendingSlotRequest = pendingSlotRequests.remove(allocationId);  //从队列中移除

		if (null != pendingSlotRequest) {
			LOG.debug("Cancel slot request {}.", allocationId);
			//取消请求
			cancelPendingSlotRequest(pendingSlotRequest);

			return true;
		} else {
			LOG.debug("No pending slot request with allocation id {} found. Ignoring unregistration request.", allocationId);

			return false;
		}
	}

	/**
	 * Registers a new task manager at the slot manager. This will make the task managers slots
	 * known and, thus, available for allocation.
	 *
	 * @param taskExecutorConnection for the new task manager
	 * @param initialSlotReport for the new task manager
	 *
	 * 当一个新的 TaskManager  注册的时候，registerTaskManager 被调用：
	 */
	public void registerTaskManager(final TaskExecutorConnection taskExecutorConnection, SlotReport initialSlotReport) {
		checkInit();

		LOG.info("Registering TaskManager {} under {} at the SlotManager.", taskExecutorConnection.getResourceID(), taskExecutorConnection.getInstanceID());

		// we identify task managers by their instance id
		if (taskManagerRegistrations.containsKey(taskExecutorConnection.getInstanceID())) {
			reportSlotStatus(taskExecutorConnection.getInstanceID(), initialSlotReport);
		} else {
			// first register the TaskManager
			ArrayList<SlotID> reportedSlots = new ArrayList<>();

			for (SlotStatus slotStatus : initialSlotReport) {
				reportedSlots.add(slotStatus.getSlotID());
			}

			TaskManagerRegistration taskManagerRegistration = new TaskManagerRegistration(
				taskExecutorConnection,
				reportedSlots);

			taskManagerRegistrations.put(taskExecutorConnection.getInstanceID(), taskManagerRegistration);

			// 依次注册所有的 slot
			// next register the new slots
			for (SlotStatus slotStatus : initialSlotReport) {
				registerSlot(  //看这里，注册一个slot
					slotStatus.getSlotID(),
					slotStatus.getAllocationID(),
					slotStatus.getJobID(),
					slotStatus.getResourceProfile(),
					taskExecutorConnection);
			}
		}

	}

	/**
	 * Unregisters the task manager identified by the given instance id and its associated slots
	 * from the slot manager.
	 *
	 * @param instanceId identifying the task manager to unregister
	 * @return True if there existed a registered task manager with the given instance id
	 */
	public boolean unregisterTaskManager(InstanceID instanceId) {
		checkInit();

		LOG.info("Unregister TaskManager {} from the SlotManager.", instanceId);

		TaskManagerRegistration taskManagerRegistration = taskManagerRegistrations.remove(instanceId);

		if (null != taskManagerRegistration) {
			internalUnregisterTaskManager(taskManagerRegistration);

			return true;
		} else {
			LOG.debug("There is no task manager registered with instance ID {}. Ignoring this message.", instanceId);

			return false;
		}
	}

	/**
	 * Reports the current slot allocations for a task manager identified by the given instance id.
	 *
	 * @param instanceId identifying the task manager for which to report the slot status
	 * @param slotReport containing the status for all of its slots
	 * @return true if the slot status has been updated successfully, otherwise false
	 *
	 * tm通过心跳向rm汇报slot status，rm 调用 slotManager 的就是这个方法；
	 */
	public boolean reportSlotStatus(InstanceID instanceId, SlotReport slotReport) {
		checkInit();

		LOG.debug("Received slot report from instance {}.", instanceId);

		TaskManagerRegistration taskManagerRegistration = taskManagerRegistrations.get(instanceId);

		if (null != taskManagerRegistration) {

			for (SlotStatus slotStatus : slotReport) {
				updateSlot(slotStatus.getSlotID(), slotStatus.getAllocationID(), slotStatus.getJobID());  //这里需要更新slot
			}

			return true;
		} else {
			LOG.debug("Received slot report for unknown task manager with instance id {}. Ignoring this report.", instanceId);

			return false;
		}
	}

	/**
	 * Free the given slot from the given allocation. If the slot is still allocated by the given
	 * allocation id, then the slot will be marked as free and will be subject to new slot requests.
	 *
	 * @param slotId identifying the slot to free
	 * @param allocationId with which the slot is presumably allocated
	 */
	public void freeSlot(SlotID slotId, AllocationID allocationId) {
		checkInit();

		TaskManagerSlot slot = slots.get(slotId);

		if (null != slot) {
			if (slot.getState() == TaskManagerSlot.State.ALLOCATED) {
				if (Objects.equals(allocationId, slot.getAllocationId())) {

					TaskManagerRegistration taskManagerRegistration = taskManagerRegistrations.get(slot.getInstanceId());

					if (taskManagerRegistration == null) {
						throw new IllegalStateException("Trying to free a slot from a TaskManager " +
							slot.getInstanceId() + " which has not been registered.");
					}

					updateSlotState(slot, taskManagerRegistration, null, null);
				} else {
					LOG.debug("Received request to free slot {} with expected allocation id {}, " +
						"but actual allocation id {} differs. Ignoring the request.", slotId, allocationId, slot.getAllocationId());
				}
			} else {
				LOG.debug("Slot {} has not been allocated.", allocationId);
			}
		} else {
			LOG.debug("Trying to free a slot {} which has not been registered. Ignoring this message.", slotId);
		}
	}

	// ---------------------------------------------------------------------------------------------
	// Behaviour methods
	// ---------------------------------------------------------------------------------------------

	/**
	 * Finds a matching slot request for a given resource profile. If there is no such request,
	 * the method returns null.
	 *
	 * <p>Note: If you want to change the behaviour of the slot manager wrt slot allocation and
	 * request fulfillment, then you should override this method.
	 *
	 * @param slotResourceProfile defining the resources of an available slot
	 * @return A matching slot request which can be deployed in a slot with the given resource
	 * profile. Null if there is no such slot request pending.
	 */
	protected PendingSlotRequest findMatchingRequest(ResourceProfile slotResourceProfile) {

		for (PendingSlotRequest pendingSlotRequest : pendingSlotRequests.values()) {
			if (!pendingSlotRequest.isAssigned() && slotResourceProfile.isMatching(pendingSlotRequest.getResourceProfile())) {
				return pendingSlotRequest;
			}
		}

		return null;
	}

	/**
	 * Finds a matching slot for a given resource profile. A matching slot has at least as many
	 * resources available as the given resource profile. If there is no such slot available, then
	 * the method returns null.
	 *
	 * <p>Note: If you want to change the behaviour of the slot manager wrt slot allocation and
	 * request fulfillment, then you should override this method.
	 *
	 * @param requestResourceProfile specifying the resource requirements for the a slot request
	 * @return A matching slot which fulfills the given resource profile. Null if there is no such
	 * slot available.
	 *
	 * 根据资源请求，找到合适的slot (free)，
	 * 注意：有可能找不到；
	 */
	protected TaskManagerSlot findMatchingSlot(ResourceProfile requestResourceProfile) {
		Iterator<Map.Entry<SlotID, TaskManagerSlot>> iterator = freeSlots.entrySet().iterator();  //只找free状态的；

		while (iterator.hasNext()) {
			TaskManagerSlot taskManagerSlot = iterator.next().getValue(); 	//具体slot

			// sanity check
			Preconditions.checkState(
				taskManagerSlot.getState() == TaskManagerSlot.State.FREE,
				"TaskManagerSlot %s is not in state FREE but %s.",
				taskManagerSlot.getSlotId(), taskManagerSlot.getState());

			if (taskManagerSlot.getResourceProfile().isMatching(requestResourceProfile)) {  //匹配方式：当前这个slot的各个资源都比需要的资源要大就可以；
				iterator.remove();
				return taskManagerSlot;
			}
		}

		return null;
	}

	// ---------------------------------------------------------------------------------------------
	// Internal slot operations
	// ---------------------------------------------------------------------------------------------

	/**
	 * Registers a slot for the given task manager at the slot manager. The slot is identified by
	 * the given slot id. The given resource profile defines the available resources for the slot.
	 * The task manager connection can be used to communicate with the task manager.
	 *
	 * @param slotId identifying the slot on the task manager
	 * @param allocationId which is currently deployed in the slot
	 * @param resourceProfile of the slot
	 * @param taskManagerConnection to communicate with the remote task manager
	 *
	 * 注册一个slot
	 */
	private void registerSlot(
			SlotID slotId,
			AllocationID allocationId,
			JobID jobId,
			ResourceProfile resourceProfile,
			TaskExecutorConnection taskManagerConnection) {

		if (slots.containsKey(slotId)) {
			// remove the old slot first
			removeSlot(slotId);
		}

		TaskManagerSlot slot = new TaskManagerSlot(
			slotId,
			resourceProfile,
			taskManagerConnection);

		slots.put(slotId, slot); //创建一个 TaskManagerSlot 对象，并加入 slots 中

		//更新slot，主要的逻辑：看是否有pending request，如果有的话，看是否能匹配，可以匹配，则把slot分配给request，没有匹配的或者根本没有pending request，就放进freeSlot中。
		updateSlot(slotId, allocationId, jobId);
	}

	/**
	 * Updates a slot with the given allocation id.
	 *
	 * @param slotId to update
	 * @param allocationId specifying the current allocation of the slot
	 * @param jobId specifying the job to which the slot is allocated
	 * @return True if the slot could be updated; otherwise false
	 */
	private boolean updateSlot(SlotID slotId, AllocationID allocationId, JobID jobId) {
		final TaskManagerSlot slot = slots.get(slotId);

		if (slot != null) {
			final TaskManagerRegistration taskManagerRegistration = taskManagerRegistrations.get(slot.getInstanceId());

			if (taskManagerRegistration != null) {
				updateSlotState(slot, taskManagerRegistration, allocationId, jobId);

				return true;
			} else {
				throw new IllegalStateException("Trying to update a slot from a TaskManager " +
					slot.getInstanceId() + " which has not been registered.");
			}
		} else {
			LOG.debug("Trying to update unknown slot with slot id {}.", slotId);

			return false;
		}
	}

	private void updateSlotState(
			TaskManagerSlot slot,
			TaskManagerRegistration taskManagerRegistration,
			@Nullable AllocationID allocationId,
			@Nullable JobID jobId) {
		if (null != allocationId) {
			switch (slot.getState()) {
				case PENDING:
					// we have a pending slot request --> check whether we have to reject it
					PendingSlotRequest pendingSlotRequest = slot.getAssignedSlotRequest();

					if (Objects.equals(pendingSlotRequest.getAllocationId(), allocationId)) {
						// we can cancel the slot request because it has been fulfilled
						cancelPendingSlotRequest(pendingSlotRequest);

						// remove the pending slot request, since it has been completed
						pendingSlotRequests.remove(pendingSlotRequest.getAllocationId());

						slot.completeAllocation(allocationId, jobId);
					} else {
						// we first have to free the slot in order to set a new allocationId
						slot.clearPendingSlotRequest();
						// set the allocation id such that the slot won't be considered for the pending slot request
						slot.updateAllocation(allocationId, jobId);

						// this will try to find a new slot for the request
						rejectPendingSlotRequest(
							pendingSlotRequest,
							new Exception("Task manager reported slot " + slot.getSlotId() + " being already allocated."));
					}

					taskManagerRegistration.occupySlot();
					break;
				case ALLOCATED:
					if (!Objects.equals(allocationId, slot.getAllocationId())) {
						slot.freeSlot();
						slot.updateAllocation(allocationId, jobId);
					}
					break;
				case FREE:
					// the slot is currently free --> it is stored in freeSlots
					freeSlots.remove(slot.getSlotId());
					slot.updateAllocation(allocationId, jobId);
					taskManagerRegistration.occupySlot();
					break;
			}

			fulfilledSlotRequests.put(allocationId, slot.getSlotId());
		} else {
			// no allocation reported
			switch (slot.getState()) {
				case FREE:
					handleFreeSlot(slot);
					break;
				case PENDING:
					// don't do anything because we still have a pending slot request
					break;
				case ALLOCATED:
					AllocationID oldAllocation = slot.getAllocationId();
					slot.freeSlot();
					fulfilledSlotRequests.remove(oldAllocation);
					taskManagerRegistration.freeSlot();

					handleFreeSlot(slot);
					break;
			}
		}
	}

	/**
	 * Tries to allocate a slot for the given slot request. If there is no slot available, the
	 * resource manager is informed to allocate more resources and a timeout for the request is
	 * registered.
	 *
	 * @param pendingSlotRequest to allocate a slot for
	 * @throws ResourceManagerException if the resource manager cannot allocate more resource
	 * 重点：
	 * 执行 请求分配slot 的逻辑
	 */
	private void internalRequestSlot(PendingSlotRequest pendingSlotRequest) throws ResourceManagerException {
		//首先从 FREE 状态的已注册的 slot 中选择符合要求的 slot
		TaskManagerSlot taskManagerSlot = findMatchingSlot(pendingSlotRequest.getResourceProfile());

		if (taskManagerSlot != null) {  //说明找到了，
			allocateSlot(taskManagerSlot, pendingSlotRequest);  //看这里，开始分配
		} else {
			//没有找到合适的资源，直接向rm申请，调用 resourceActions#allocateResource 分配资源；  *** 动态资源管理的关键 ***
			resourceActions.allocateResource(pendingSlotRequest.getResourceProfile());  //没有找到，就会调用这个方法；
			//resourceActions的唯一实现在rm顶层：ResourceManager # ResourceActionsImpl，然后调用startNewWorker(), 模板方法，运行时调用具体实现，
			//对于YarnResourceManager来说，是新启一个container，对于standalone rm来说，无法提供实现；
		}
	}

	/**
	 * Allocates the given slot for the given slot request. This entails sending a registration
	 * message to the task manager and treating failures.
	 *
	 * @param taskManagerSlot to allocate for the given slot request
	 * @param pendingSlotRequest to allocate the given slot for
	 *
	 * 把一个 TaskManagerSlot 分配给 一个 PendingSlotRequest
	 * 具体是如何进行分配呢？主要就是rpc调用tm的方法，来分配slot；这个 RPC 调用就是在这里发生的；
	 */
	private void allocateSlot(TaskManagerSlot taskManagerSlot, PendingSlotRequest pendingSlotRequest) {
		Preconditions.checkState(taskManagerSlot.getState() == TaskManagerSlot.State.FREE);

		TaskExecutorConnection taskExecutorConnection = taskManagerSlot.getTaskManagerConnection();
		TaskExecutorGateway gateway = taskExecutorConnection.getTaskExecutorGateway();   //获取tm代理对象，因为需要rpc调用tm的方法；

		final CompletableFuture<Acknowledge> completableFuture = new CompletableFuture<>();   //PendingSlotRequest 需要回调的Future，
		final AllocationID allocationId = pendingSlotRequest.getAllocationId();
		final SlotID slotId = taskManagerSlot.getSlotId();  // slot id
		final InstanceID instanceID = taskManagerSlot.getInstanceId();

		//taskManagerSlot 状态变为 PENDING
		taskManagerSlot.assignPendingSlotRequest(pendingSlotRequest);
		pendingSlotRequest.setRequestFuture(completableFuture);

		TaskManagerRegistration taskManagerRegistration = taskManagerRegistrations.get(instanceID);

		if (taskManagerRegistration == null) {
			throw new IllegalStateException("Could not find a registered task manager for instance id " +
				instanceID + '.');
		}

		taskManagerRegistration.markUsed();

		// RPC call to the task manager  rpc 调用tm的 requestSlot 方法，分配slot
		CompletableFuture<Acknowledge> requestFuture = gateway.requestSlot(
			slotId,
			pendingSlotRequest.getJobId(),
			allocationId,
			pendingSlotRequest.getTargetAddress(),
			resourceManagerId,
			taskManagerRequestTimeout);

		requestFuture.whenComplete( //rpc tm -> requestSlot，调用完成
			(Acknowledge acknowledge, Throwable throwable) -> {
				if (acknowledge != null) {
					completableFuture.complete(acknowledge); //完成 slot request future
				} else {
					completableFuture.completeExceptionally(throwable);
				}
			});

		//PendingSlotRequest 请求完成的回调函数
		//PendingSlotRequest 请求完成可能是由于上面 RPC 调用完成，也可能是因为 PendingSlotRequest 被取消
		completableFuture.whenCompleteAsync(  //slot request future 调用完成；
			(Acknowledge acknowledge, Throwable throwable) -> {
				try {
					if (acknowledge != null) {
						//如果请求成功，则取消 pendingSlotRequest，并更新 slot 状态 PENDING -> ALLOCATED
						updateSlot(slotId, allocationId, pendingSlotRequest.getJobId()); //分配完成，需要更新slot status，
					} else {
						if (throwable instanceof SlotOccupiedException) {
							//这个 slot 已经被占用了，更新状态
							SlotOccupiedException exception = (SlotOccupiedException) throwable;
							updateSlot(slotId, exception.getAllocationId(), exception.getJobId());
						} else {
							//请求失败，将 pendingSlotRequest 从 TaskManagerSlot 中移除
							removeSlotRequestFromSlot(slotId, allocationId);
						}

						if (!(throwable instanceof CancellationException)) {
							//slot request 请求失败，会进行重试
							handleFailedSlotRequest(slotId, allocationId, throwable);
						} else {
							//主动取消
							LOG.debug("Slot allocation request {} has been cancelled.", allocationId, throwable);
						}
					}
				} catch (Exception e) {
					LOG.error("Error while completing the slot allocation.", e);
				}
			},
			mainThreadExecutor);
	}

	/**
	 * Handles a free slot. It first tries to find a pending slot request which can be fulfilled.
	 * If there is no such request, then it will add the slot to the set of free slots.
	 *
	 * @param freeSlot to find a new slot request for
	 */
	private void handleFreeSlot(TaskManagerSlot freeSlot) {
		Preconditions.checkState(freeSlot.getState() == TaskManagerSlot.State.FREE);

		PendingSlotRequest pendingSlotRequest = findMatchingRequest(freeSlot.getResourceProfile());

		if (null != pendingSlotRequest) {
			allocateSlot(freeSlot, pendingSlotRequest);
		} else {
			freeSlots.put(freeSlot.getSlotId(), freeSlot);
		}
	}

	/**
	 * Removes the given set of slots from the slot manager.
	 *
	 * @param slotsToRemove identifying the slots to remove from the slot manager
	 */
	private void removeSlots(Iterable<SlotID> slotsToRemove) {
		for (SlotID slotId : slotsToRemove) {
			removeSlot(slotId);
		}
	}

	/**
	 * Removes the given slot from the slot manager.
	 *
	 * @param slotId identifying the slot to remove
	 */
	private void removeSlot(SlotID slotId) {
		TaskManagerSlot slot = slots.remove(slotId);

		if (null != slot) {
			freeSlots.remove(slotId);

			if (slot.getState() == TaskManagerSlot.State.PENDING) {
				// reject the pending slot request --> triggering a new allocation attempt
				rejectPendingSlotRequest(
					slot.getAssignedSlotRequest(),
					new Exception("The assigned slot " + slot.getSlotId() + " was removed."));
			}

			AllocationID oldAllocationId = slot.getAllocationId();

			if (oldAllocationId != null) {
				fulfilledSlotRequests.remove(oldAllocationId);

				resourceActions.notifyAllocationFailure(
					slot.getJobId(),
					oldAllocationId,
					new FlinkException("The assigned slot " + slot.getSlotId() + " was removed."));
			}
		} else {
			LOG.debug("There was no slot registered with slot id {}.", slotId);
		}
	}

	// ---------------------------------------------------------------------------------------------
	// Internal request handling methods
	// ---------------------------------------------------------------------------------------------

	/**
	 * Removes a pending slot request identified by the given allocation id from a slot identified
	 * by the given slot id.
	 *
	 * @param slotId identifying the slot
	 * @param allocationId identifying the presumable assigned pending slot request
	 */
	private void removeSlotRequestFromSlot(SlotID slotId, AllocationID allocationId) {
		TaskManagerSlot taskManagerSlot = slots.get(slotId);

		if (null != taskManagerSlot) {
			if (taskManagerSlot.getState() == TaskManagerSlot.State.PENDING && Objects.equals(allocationId, taskManagerSlot.getAssignedSlotRequest().getAllocationId())) {

				TaskManagerRegistration taskManagerRegistration = taskManagerRegistrations.get(taskManagerSlot.getInstanceId());

				if (taskManagerRegistration == null) {
					throw new IllegalStateException("Trying to remove slot request from slot for which there is no TaskManager " + taskManagerSlot.getInstanceId() + " is registered.");
				}

				// clear the pending slot request
				taskManagerSlot.clearPendingSlotRequest();

				updateSlotState(taskManagerSlot, taskManagerRegistration, null, null);
			} else {
				LOG.debug("Ignore slot request removal for slot {}.", slotId);
			}
		} else {
			LOG.debug("There was no slot with {} registered. Probably this slot has been already freed.", slotId);
		}
	}

	/**
	 * Handles a failed slot request. The slot manager tries to find a new slot fulfilling
	 * the resource requirements for the failed slot request.
	 *
	 * @param slotId identifying the slot which was assigned to the slot request before
	 * @param allocationId identifying the failed slot request
	 * @param cause of the failure
	 *
	 * 处理失败的slot request，
	 */
	private void handleFailedSlotRequest(SlotID slotId, AllocationID allocationId, Throwable cause) {
		PendingSlotRequest pendingSlotRequest = pendingSlotRequests.get(allocationId);

		LOG.debug("Slot request with allocation id {} failed for slot {}.", allocationId, slotId, cause);

		if (null != pendingSlotRequest) {
			pendingSlotRequest.setRequestFuture(null);

			try {
				internalRequestSlot(pendingSlotRequest);  //重试的逻辑：重新请求分配slot
			} catch (ResourceManagerException e) {
				pendingSlotRequests.remove(allocationId);

				resourceActions.notifyAllocationFailure(
					pendingSlotRequest.getJobId(),
					allocationId,
					e);
			}
		} else {
			LOG.debug("There was not pending slot request with allocation id {}. Probably the request has been fulfilled or cancelled.", allocationId);
		}
	}

	/**
	 * Rejects the pending slot request by failing the request future with a
	 * {@link SlotAllocationException}.
	 *
	 * @param pendingSlotRequest to reject
	 * @param cause of the rejection
	 */
	private void rejectPendingSlotRequest(PendingSlotRequest pendingSlotRequest, Exception cause) {
		CompletableFuture<Acknowledge> request = pendingSlotRequest.getRequestFuture();

		if (null != request) {
			request.completeExceptionally(new SlotAllocationException(cause));
		} else {
			LOG.debug("Cannot reject pending slot request {}, since no request has been sent.", pendingSlotRequest.getAllocationId());
		}
	}

	/**
	 * Cancels the given slot request.
	 *
	 * @param pendingSlotRequest to cancel
	 *
	 * 主动取消一个slot request
	 */
	private void cancelPendingSlotRequest(PendingSlotRequest pendingSlotRequest) {
		CompletableFuture<Acknowledge> request = pendingSlotRequest.getRequestFuture();  //slot request 的future，那就是说可能有两种完成结果，一种是分配成功，另外一种是主动取消了slot request

		if (null != request) {
			request.cancel(false); //主动取消
		}
	}

	// ---------------------------------------------------------------------------------------------
	// Internal timeout methods
	// ---------------------------------------------------------------------------------------------

	//检查tm是否长期处于空闲状态；
	private void checkTaskManagerTimeouts() {
		if (!taskManagerRegistrations.isEmpty()) {
			long currentTime = System.currentTimeMillis();

			ArrayList<InstanceID> timedOutTaskManagerIds = new ArrayList<>(taskManagerRegistrations.size()); //InstanceID 标记一个tm

			// first retrieve the timed out TaskManagers
			for (TaskManagerRegistration taskManagerRegistration : taskManagerRegistrations.values()) {    //已经注册的所有tm
				if (currentTime - taskManagerRegistration.getIdleSince() >= taskManagerTimeout.toMilliseconds()) {  //空闲时间  > timeout时间
					// we collect the instance ids first in order to avoid concurrent modifications by the
					// ResourceActions.releaseResource call
					timedOutTaskManagerIds.add(taskManagerRegistration.getInstanceId());  //需要释放的tm
				}
			}

			// second we trigger the release resource callback which can decide upon the resource release
			for (InstanceID timedOutTaskManagerId : timedOutTaskManagerIds) {
				LOG.debug("Release TaskExecutor {} because it exceeded the idle timeout.", timedOutTaskManagerId);
				//一旦 TaskExecutor 长时间处于空闲状态，则会通过 ResourceActions#releaseResource() 回调函数释放资源：
				resourceActions.releaseResource(timedOutTaskManagerId, new FlinkException("TaskExecutor exceeded the idle timeout.")); //释放资源；
			}
		}
	}

	//检查 slot request 是否超时的具体逻辑
	private void checkSlotRequestTimeouts() {
		if (!pendingSlotRequests.isEmpty()) {
			long currentTime = System.currentTimeMillis();

			Iterator<Map.Entry<AllocationID, PendingSlotRequest>> slotRequestIterator = pendingSlotRequests.entrySet().iterator();

			while (slotRequestIterator.hasNext()) {
				PendingSlotRequest slotRequest = slotRequestIterator.next().getValue();

				if (currentTime - slotRequest.getCreationTimestamp() >= slotRequestTimeout.toMilliseconds()) {   //当前时间 - request的创建时间 > 设置的超时时间
					slotRequestIterator.remove(); //移除

					if (slotRequest.isAssigned()) {
						cancelPendingSlotRequest(slotRequest);  //cancel掉已经 assign 的slot
					}

					resourceActions.notifyAllocationFailure(  //资源分配失败；通过 ResourceActions#notifyAllocationFailure() 告知 ResourceManager
						slotRequest.getJobId(),
						slotRequest.getAllocationId(),
						new TimeoutException("The allocation could not be fulfilled in time."));
				}
			}
		}
	}

	// ---------------------------------------------------------------------------------------------
	// Internal utility methods
	// ---------------------------------------------------------------------------------------------

	private void internalUnregisterTaskManager(TaskManagerRegistration taskManagerRegistration) {
		Preconditions.checkNotNull(taskManagerRegistration);

		removeSlots(taskManagerRegistration.getSlots());
	}

	private boolean checkDuplicateRequest(AllocationID allocationId) {
		return pendingSlotRequests.containsKey(allocationId) || fulfilledSlotRequests.containsKey(allocationId);
	}

	private void checkInit() {
		Preconditions.checkState(started, "The slot manager has not been started.");
	}

	// ---------------------------------------------------------------------------------------------
	// Testing methods
	// ---------------------------------------------------------------------------------------------

	@VisibleForTesting
	TaskManagerSlot getSlot(SlotID slotId) {
		return slots.get(slotId);
	}

	@VisibleForTesting
	PendingSlotRequest getSlotRequest(AllocationID allocationId) {
		return pendingSlotRequests.get(allocationId);
	}

	@VisibleForTesting
	boolean isTaskManagerIdle(InstanceID instanceId) {
		TaskManagerRegistration taskManagerRegistration = taskManagerRegistrations.get(instanceId);

		if (null != taskManagerRegistration) {
			return taskManagerRegistration.isIdle();
		} else {
			return false;
		}
	}

	@VisibleForTesting
	public void unregisterTaskManagersAndReleaseResources() {
		Iterator<Map.Entry<InstanceID, TaskManagerRegistration>> taskManagerRegistrationIterator =
				taskManagerRegistrations.entrySet().iterator();

		while (taskManagerRegistrationIterator.hasNext()) {
			TaskManagerRegistration taskManagerRegistration =
					taskManagerRegistrationIterator.next().getValue();

			taskManagerRegistrationIterator.remove();

			internalUnregisterTaskManager(taskManagerRegistration);

			resourceActions.releaseResource(taskManagerRegistration.getInstanceId(), new FlinkException("Triggering of SlotManager#unregisterTaskManagersAndReleaseResources."));
		}
	}
}

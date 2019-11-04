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

package org.apache.flink.runtime.highavailability.nonha.embedded;

import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A simple leader election service, which selects a leader among contenders and notifies listeners.
 * 一个简单的leader选举服务，在候选者中选择一个leader，然后通知 listener
 * <p>An election service for contenders can be created via {@link #createLeaderElectionService()},
 * a listener service for leader observers can be created via {@link #createLeaderRetrievalService()}.
 *
 * 这个是一个顶级父类，有两个内部类，EmbeddedLeaderElectionService(用于leader选举)  和  EmbeddedLeaderRetrievalService(用于leader检索)
 * 这个类的主要作用就是实现了 leader检索 和 leader选举，
 *
 * 实现的原理也比较简答，适用于 ResourceMangaer， TaksManager，JobManager 等所有组件都运行在同一个进程的情况。也就是永远只有一个leader
 * 1.首先，ResourceMangaer，dispatcher 等组件启动，会各自创建一个自己的 EmbeddedLeaderService，进行自己的leader选举和leader检索服务；
 * 2.启动时，通过各自的 EmbeddedLeaderService 获取各自的leader选举服务，传入候选者，然后自动成为leader（通过回调）
 * 3.当tm需要获取rm / jm 的leader地址时，只需要获取对应的 rm / jm 的 EmbeddedLeaderService 的leader检索服务，然后注入一个监听器，然后被对应组件的EmbeddedLeaderService回调listener方法，拿到leader地址
 */
public class EmbeddedLeaderService {

	private static final Logger LOG = LoggerFactory.getLogger(EmbeddedLeaderService.class);

	private final Object lock = new Object();

	private final Executor notificationExecutor;

	private final Set<EmbeddedLeaderElectionService> allLeaderContenders;  //所有 参与竞选的候选者

	private final Set<EmbeddedLeaderRetrievalService> listeners;		//所有 监听者

	/** proposed leader, which has been notified of leadership grant, but has not confirmed. */
	private EmbeddedLeaderElectionService currentLeaderProposed;     //提议人

	/** actual leader that has confirmed leadership and of which listeners have been notified. */
	private EmbeddedLeaderElectionService currentLeaderConfirmed;   //确定人

	/** fencing UID for the current leader (or proposed leader). */
	private volatile UUID currentLeaderSessionId;

	/** the cached address of the current leader. */
	private String currentLeaderAddress;

	/** flag marking the service as terminated. */
	private boolean shutdown;

	// ------------------------------------------------------------------------

	public EmbeddedLeaderService(Executor notificationsDispatcher) {
		this.notificationExecutor = checkNotNull(notificationsDispatcher);
		this.allLeaderContenders = new HashSet<>();
		this.listeners = new HashSet<>();
	}

	// ------------------------------------------------------------------------
	//  shutdown and errors
	// ------------------------------------------------------------------------

	/**
	 * Shuts down this leader election service.
	 *
	 * <p>This method does not perform a clean revocation of the leader status and
	 * no notification to any leader listeners. It simply notifies all contenders
	 * and listeners that the service is no longer available.
	 */
	public void shutdown() {
		synchronized (lock) {
			shutdownInternally(new Exception("Leader election service is shutting down"));
		}
	}

	private void fatalError(Throwable error) {
		LOG.error("Embedded leader election service encountered a fatal error. Shutting down service.", error);

		synchronized (lock) {
			shutdownInternally(new Exception("Leader election service is shutting down after a fatal error", error));
		}
	}

	@GuardedBy("lock")
	private void shutdownInternally(Exception exceptionForHandlers) {
		assert Thread.holdsLock(lock);

		if (!shutdown) {
			// clear all leader status
			currentLeaderProposed = null;
			currentLeaderConfirmed = null;
			currentLeaderSessionId = null;
			currentLeaderAddress = null;

			// fail all registered listeners
			for (EmbeddedLeaderElectionService service : allLeaderContenders) {
				service.shutdown(exceptionForHandlers);
			}
			allLeaderContenders.clear();

			// fail all registered listeners
			for (EmbeddedLeaderRetrievalService service : listeners) {
				service.shutdown(exceptionForHandlers);
			}
			listeners.clear();

			shutdown = true;
		}
	}

	// ------------------------------------------------------------------------
	//  创建 leader检索服务   和   leader选举服务
	// ------------------------------------------------------------------------

	public LeaderElectionService createLeaderElectionService() {
		checkState(!shutdown, "leader election service is shut down");
		return new EmbeddedLeaderElectionService();
	}

	public LeaderRetrievalService createLeaderRetrievalService() {
		checkState(!shutdown, "leader election service is shut down");
		return new EmbeddedLeaderRetrievalService();
	}

	// ------------------------------------------------------------------------
	//  adding and removing contenders & listeners
	// ------------------------------------------------------------------------

	/**
	 * Callback from leader contenders when they start their service.
	 * 新增一个候选者，
	 */
	void addContender(EmbeddedLeaderElectionService service, LeaderContender contender) {
		synchronized (lock) {
			checkState(!shutdown, "leader election service is shut down");
			checkState(!service.running, "leader election service is already started");

			try {
				if (!allLeaderContenders.add(service)) {  	//加到所有候选者中； 不允许重复加入 （为什么要加入这个Service而不加contender呢？）
					throw new IllegalStateException("leader election service was added to this service multiple times");
				}

				service.contender = contender;
				service.running = true;

				updateLeader();  //更新一下当前leader，回调，触发leader授权回调；
			}
			catch (Throwable t) {
				fatalError(t);
			}
		}
	}

	/**
	 * Callback from leader contenders when they stop their service.
	 */
	void removeContender(EmbeddedLeaderElectionService service) {
		synchronized (lock) {
			// if the service was not even started, simply do nothing
			if (!service.running || shutdown) {
				return;
			}

			try {
				if (!allLeaderContenders.remove(service)) {
					throw new IllegalStateException("leader election service does not belong to this service");
				}

				// stop the service
				service.contender = null;
				service.running = false;
				service.isLeader = false;

				// if that was the current leader, unset its status
				if (currentLeaderConfirmed == service) {
					currentLeaderConfirmed = null;
					currentLeaderSessionId = null;
					currentLeaderAddress = null;
				}
				if (currentLeaderProposed == service) {
					currentLeaderProposed = null;
					currentLeaderSessionId = null;
				}

				updateLeader();
			}
			catch (Throwable t) {
				fatalError(t);
			}
		}
	}

	/**
	 * Callback from leader contenders when they confirm a leader grant.
	 */
	void confirmLeader(final EmbeddedLeaderElectionService service, final UUID leaderSessionId) {
		synchronized (lock) {
			// if the service was shut down in the meantime, ignore this confirmation
			if (!service.running || shutdown) {
				return;
			}

			try {
				// check if the confirmation is for the same grant, or whether it is a stale grant
				if (service == currentLeaderProposed && currentLeaderSessionId.equals(leaderSessionId)) {
					final String address = service.contender.getAddress();
					LOG.info("Received confirmation of leadership for leader {} , session={}", address, leaderSessionId);

					// mark leadership
					currentLeaderConfirmed = service;
					currentLeaderAddress = address;
					currentLeaderProposed = null;

					// notify all listeners
					for (EmbeddedLeaderRetrievalService listener : listeners) {
						notificationExecutor.execute(
								new NotifyOfLeaderCall(address, leaderSessionId, listener.listener, LOG));
					}
				}
				else {
					LOG.debug("Received confirmation of leadership for a stale leadership grant. Ignoring.");
				}
			}
			catch (Throwable t) {
				fatalError(t);
			}
		}
	}

	//更新当前leader，需要回调leader 的 GrantLeadership() 方法进行授权操作；
	@GuardedBy("lock")
	private void updateLeader() {
		// this must be called under the lock
		assert Thread.holdsLock(lock);

		if (currentLeaderConfirmed == null && currentLeaderProposed == null) {
			// we need a new leader
			if (allLeaderContenders.isEmpty()) {   //没有leader
				// no new leader available, tell everyone that there is no leader currently
				for (EmbeddedLeaderRetrievalService listener : listeners) {
					notificationExecutor.execute(
							new NotifyOfLeaderCall(null, null, listener.listener, LOG));
				}
			}
			else {
				// propose a leader and ask it     但是这里好像没有触发 对listener的回调？ 这是个bug吗？
				final UUID leaderSessionId = UUID.randomUUID();
				EmbeddedLeaderElectionService leaderService = allLeaderContenders.iterator().next(); //直接从所有候选者中选一个，授予其leader权限；

				currentLeaderSessionId = leaderSessionId;
				currentLeaderProposed = leaderService;

				LOG.info("Proposing leadership to contender {} @ {}",
						leaderService.contender, leaderService.contender.getAddress());

				notificationExecutor.execute(
						new GrantLeadershipCall(leaderService, leaderSessionId, LOG));
			}
		}
	}

	//增加监听者； 在这里如果已经有leader了，就触发listener的回调，也就是说，必须先选举出leader，然后注册listener，才能成功触发回调，
	//因为选举出leader之后，没有触发listener的回调的操作，这好像是个bug？！
	void addListener(EmbeddedLeaderRetrievalService service, LeaderRetrievalListener listener) {
		synchronized (lock) {
			checkState(!shutdown, "leader election service is shut down");
			checkState(!service.running, "leader retrieval service is already started");

			try {
				if (!listeners.add(service)) {
					throw new IllegalStateException("leader retrieval service was added to this service multiple times");
				}

				service.listener = listener;
				service.running = true;

				// if we already have a leader, immediately notify this new listener
				if (currentLeaderConfirmed != null) {
					notificationExecutor.execute(
							new NotifyOfLeaderCall(currentLeaderAddress, currentLeaderSessionId, listener, LOG));
				}
			}
			catch (Throwable t) {
				fatalError(t);
			}
		}
	}

	void removeListener(EmbeddedLeaderRetrievalService service) {
		synchronized (lock) {
			// if the service was not even started, simply do nothing
			if (!service.running || shutdown) {
				return;
			}

			try {
				if (!listeners.remove(service)) {
					throw new IllegalStateException("leader retrieval service does not belong to this service");
				}

				// stop the service
				service.listener = null;
				service.running = false;
			}
			catch (Throwable t) {
				fatalError(t);
			}
		}
	}

	// ------------------------------------------------------------------------
	//  election and retrieval service implementations
	// ------------------------------------------------------------------------

	private class EmbeddedLeaderElectionService implements LeaderElectionService {

		volatile LeaderContender contender;   //leader 竞选者

		volatile boolean isLeader;

		volatile boolean running;

		@Override
		public void start(LeaderContender contender) throws Exception {
			checkNotNull(contender);
			addContender(this, contender); //新增一个候选人，然后updateLeader()回调触发leader的授权；但是对leader授完权却没有回调listener，？
		}

		@Override
		public void stop() throws Exception {
			removeContender(this);
		}

		@Override
		public void confirmLeaderSessionID(UUID leaderSessionID) {
			checkNotNull(leaderSessionID);
			confirmLeader(this, leaderSessionID);
		}

		@Override
		public boolean hasLeadership(@Nonnull UUID leaderSessionId) {
			return isLeader && leaderSessionId.equals(currentLeaderSessionId);
		}

		void shutdown(Exception cause) {
			if (running) {
				running = false;
				isLeader = false;
				contender.revokeLeadership();
				contender = null;
			}
		}
	}

	// ------------------------------------------------------------------------

	private class EmbeddedLeaderRetrievalService implements LeaderRetrievalService {

		volatile LeaderRetrievalListener listener;

		volatile boolean running;

		@Override
		public void start(LeaderRetrievalListener listener) throws Exception {
			checkNotNull(listener);
			addListener(this, listener);
		}

		@Override
		public void stop() throws Exception {
			removeListener(this);
		}

		public void shutdown(Exception cause) {
			if (running) {
				running = false;
				listener = null;
			}
		}
	}

	// ------------------------------------------------------------------------
	//  asynchronous notifications
	//  异步通知
	// ------------------------------------------------------------------------

	private static class NotifyOfLeaderCall implements Runnable {

		@Nullable
		private final String address;       // null if leader revoked without new leader
		@Nullable
		private final UUID leaderSessionId; // null if leader revoked without new leader

		private final LeaderRetrievalListener listener;
		private final Logger logger;

		NotifyOfLeaderCall(
				@Nullable String address,
				@Nullable UUID leaderSessionId,
				LeaderRetrievalListener listener,
				Logger logger) {

			this.address = address;
			this.leaderSessionId = leaderSessionId;
			this.listener = checkNotNull(listener);
			this.logger = checkNotNull(logger);
		}

		@Override
		public void run() {
			try {
				listener.notifyLeaderAddress(address, leaderSessionId);  //回调listener的方法，通知它新leader地址；
			}
			catch (Throwable t) {
				logger.warn("Error notifying leader listener about new leader", t);
				listener.handleError(t instanceof Exception ? (Exception) t : new Exception(t));
			}
		}
	}

	// ------------------------------------------------------------------------

	private static class GrantLeadershipCall implements Runnable {

		private final EmbeddedLeaderElectionService leaderElectionService;
		private final UUID leaderSessionId;
		private final Logger logger;

		GrantLeadershipCall(
				EmbeddedLeaderElectionService leaderElectionService,
				UUID leaderSessionId,
				Logger logger) {

			this.leaderElectionService = checkNotNull(leaderElectionService);
			this.leaderSessionId = checkNotNull(leaderSessionId);
			this.logger = checkNotNull(logger);
		}

		@Override
		public void run() {
			leaderElectionService.isLeader = true;

			final LeaderContender contender = leaderElectionService.contender;

			try {
				contender.grantLeadership(leaderSessionId); //给候选者授权，让其成为leader
			}
			catch (Throwable t) {
				logger.warn("Error granting leadership to contender", t);
				contender.handleError(t instanceof Exception ? (Exception) t : new Exception(t));
				leaderElectionService.isLeader = false;
			}
		}
	}
}

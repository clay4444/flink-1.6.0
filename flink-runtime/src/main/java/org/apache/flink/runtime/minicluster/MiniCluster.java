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

package org.apache.flink.runtime.minicluster;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.blob.BlobCacheService;
import org.apache.flink.runtime.blob.BlobClient;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.client.ClientUtils;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.clusterframework.FlinkResourceManager;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.dispatcher.DispatcherId;
import org.apache.flink.runtime.dispatcher.DispatcherRestEndpoint;
import org.apache.flink.runtime.dispatcher.HistoryServerArchivist;
import org.apache.flink.runtime.dispatcher.MemoryArchivedExecutionGraphStore;
import org.apache.flink.runtime.dispatcher.StandaloneDispatcher;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalException;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;
import org.apache.flink.runtime.metrics.util.MetricUtils;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.ResourceManagerRunner;
import org.apache.flink.runtime.rest.RestServerEndpointConfiguration;
import org.apache.flink.runtime.rest.handler.RestHandlerConfiguration;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.akka.AkkaRpcService;
import org.apache.flink.runtime.taskexecutor.TaskExecutor;
import org.apache.flink.runtime.taskexecutor.TaskManagerRunner;
import org.apache.flink.runtime.webmonitor.retriever.impl.AkkaQueryServiceRetriever;
import org.apache.flink.runtime.webmonitor.retriever.impl.RpcGatewayRetriever;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;

import akka.actor.ActorSystem;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * MiniCluster to execute Flink jobs locally.
 */
public class MiniCluster implements JobExecutorService, AutoCloseableAsync {

	private static final Logger LOG = LoggerFactory.getLogger(MiniCluster.class);

	/** The lock to guard startup / shutdown / manipulation methods. */
	private final Object lock = new Object();

	/** The configuration for this mini cluster. */
	private final MiniClusterConfiguration miniClusterConfiguration;

	private final Time rpcTimeout;

	private CompletableFuture<Void> terminationFuture;

	@GuardedBy("lock")
	private MetricRegistryImpl metricRegistry;

	@GuardedBy("lock")
	private RpcService commonRpcService;

	@GuardedBy("lock")
	private RpcService jobManagerRpcService;

	@GuardedBy("lock")
	private RpcService[] taskManagerRpcServices;

	@GuardedBy("lock")
	private RpcService resourceManagerRpcService;

	@GuardedBy("lock")
	private HighAvailabilityServices haServices;

	@GuardedBy("lock")
	private BlobServer blobServer;

	@GuardedBy("lock")
	private HeartbeatServices heartbeatServices;

	@GuardedBy("lock")
	private BlobCacheService blobCacheService;

	@GuardedBy("lock")
	private ResourceManagerRunner resourceManagerRunner;

	private volatile TaskExecutor[] taskManagers;  //TaskExecutor

	@GuardedBy("lock")
	private DispatcherRestEndpoint dispatcherRestEndpoint;

	@GuardedBy("lock")
	private URI restAddressURI;

	@GuardedBy("lock")
	private LeaderRetrievalService resourceManagerLeaderRetriever;

	@GuardedBy("lock")
	private LeaderRetrievalService dispatcherLeaderRetriever;

	@GuardedBy("lock")
	private StandaloneDispatcher dispatcher;

	@GuardedBy("lock")
	private JobManagerMetricGroup jobManagerMetricGroup;

	@GuardedBy("lock")
	private RpcGatewayRetriever<DispatcherId, DispatcherGateway> dispatcherGatewayRetriever;  //通过它的createGateWay可以获取到 DispatcherGateway 的远程代理对象

	/** Flag marking the mini cluster as started/running. */
	private volatile boolean running;

	// ------------------------------------------------------------------------

	/**
	 * Creates a new Flink mini cluster based on the given configuration.
	 *
	 * @param miniClusterConfiguration The configuration for the mini cluster
	 */
	public MiniCluster(MiniClusterConfiguration miniClusterConfiguration) {
		this.miniClusterConfiguration = checkNotNull(miniClusterConfiguration, "config may not be null");

		this.rpcTimeout = Time.seconds(10L);
		this.terminationFuture = CompletableFuture.completedFuture(null);
		running = false;
	}

	public URI getRestAddress() {
		synchronized (lock) {
			checkState(running, "MiniCluster is not yet running.");
			return restAddressURI;
		}
	}

	public HighAvailabilityServices getHighAvailabilityServices() {
		synchronized (lock) {
			checkState(running, "MiniCluster is not yet running.");
			return haServices;
		}
	}

	// ------------------------------------------------------------------------
	//  life cycle  声明周期
	// ------------------------------------------------------------------------

	/**
	 * Checks if the mini cluster was started and is running.
	 */
	public boolean isRunning() {
		return running;
	}

	/**
	 * Starts the mini cluster, based on the configured properties.
	 * @throws Exception This method passes on any exception that occurs during the startup of
	 *                   the mini cluster.
	 *
	 * 大致分为三个阶段
	 * 	1.创建一些辅助的服务，如 RpcService， HighAvailabilityServices, BlobServer 等
	 * 	2.启动 TaskManager
	 * 	2.启动 Dispatcher， ResourceManager 等
	 *
	 * 基于 configutation 配置，启动 mini cluster，
	 *  1. 创建RpcService，RpcService是RpcEndPoint的运行时环境，RpcEndPoint创建的时候就是通过 RpcService 来启动，并返回代理对象的；主要有三个，jobmanager，resourcemanager，taskmanager
	 *  2. 创建 HighAvailabilityServices，它提供了获取 HA 相关所有服务的方法，包括：ResourceManager 选举服务及 Leader 获取 、Dispatcher 选举服务及 Leader 获取、任务状态的注册表、checkpoint recovery、blob store 等相关的服务
	 * minicluster环境下，返回的是EmbeddedHaServices；细节看代码处的注释
	 *  3. 启动ResourceManager，然后直接start启动，可以看到选举的过程；
	 *  4. 启动tm，具体是创建了 TaskExecutor，然后start启动，可以看到和rm通信，并rpc汇报当前slot的过程；细节跟代码看注释
	 *  5. 启动DispatcherRestEndpoint
	 *  6. 启动Dispatcher(具体是StandaloneDispatcher)，然后start启动，也看到参与选举的过程；
	 *
	 */
	public void start() throws Exception {
		synchronized (lock) {
			checkState(!running, "FlinkMiniCluster is already running");

			LOG.info("Starting Flink Mini Cluster");
			LOG.debug("Using configuration {}", miniClusterConfiguration);

			final Configuration configuration = miniClusterConfiguration.getConfiguration();   //configuration
			final Time rpcTimeout = miniClusterConfiguration.getRpcTimeout();
			final int numTaskManagers = miniClusterConfiguration.getNumTaskManagers();  // tm个数， 1
			final boolean useSingleRpcService = miniClusterConfiguration.getRpcServiceSharing() == RpcServiceSharing.SHARED;  //true

			try {
				initializeIOFormatClasses(configuration);

				LOG.info("Starting Metrics Registry");
				metricRegistry = createMetricRegistry(configuration);

				//1. 创建RpcService，
				final RpcService jobManagerRpcService;
				final RpcService resourceManagerRpcService;
				final RpcService[] taskManagerRpcServices = new RpcService[numTaskManagers];

				// bring up all the RPC services
				LOG.info("Starting RPC Service(s)");

				// we always need the 'commonRpcService' for auxiliary calls
				commonRpcService = createRpcService(configuration, rpcTimeout, false, null);  //创建一个共享的 AkkaRpcService

				// TODO: Temporary hack until the metric query service is ported to the RpcEndpoint
				final ActorSystem actorSystem = ((AkkaRpcService) commonRpcService).getActorSystem();
				metricRegistry.startQueryService(actorSystem, null);

				if (useSingleRpcService) {    // 默认所有组件(tm,jm,rm)都使用一个共享的rpcService；
					for (int i = 0; i < numTaskManagers; i++) {
						taskManagerRpcServices[i] = commonRpcService;
					}

					jobManagerRpcService = commonRpcService;
					resourceManagerRpcService = commonRpcService;

					this.resourceManagerRpcService = null;
					this.jobManagerRpcService = null;
					this.taskManagerRpcServices = null;
				}
				else {
					// start a new service per component, possibly with custom bind addresses
					final String jobManagerBindAddress = miniClusterConfiguration.getJobManagerBindAddress();
					final String taskManagerBindAddress = miniClusterConfiguration.getTaskManagerBindAddress();
					final String resourceManagerBindAddress = miniClusterConfiguration.getResourceManagerBindAddress();

					jobManagerRpcService = createRpcService(configuration, rpcTimeout, true, jobManagerBindAddress);
					resourceManagerRpcService = createRpcService(configuration, rpcTimeout, true, resourceManagerBindAddress);

					for (int i = 0; i < numTaskManagers; i++) {
						taskManagerRpcServices[i] = createRpcService(
								configuration, rpcTimeout, true, taskManagerBindAddress);
					}

					this.jobManagerRpcService = jobManagerRpcService;
					this.taskManagerRpcServices = taskManagerRpcServices;
					this.resourceManagerRpcService = resourceManagerRpcService;
				}

				// create the high-availability services   创建 高可用 服务
				LOG.info("Starting high-availability services");

				/**
				 * 创建HighAvailabilityServices，返回的是 EmbeddedHaServices，
				 * HighAvailabilityServicesUtils 是创建 HighAvailabilityServices 的工具类，在没有配置 HA 的情况下，会创建 EmbeddedHaServices。
				 * EmbeddedHaServices 不具备高可用的特性，适用于 ResourceMangaer， TaksManager，JobManager 等所有组件都运行在同一个进程的情况。
				 * EmbeddedHaService 为各组件创建的选举服务为 EmbeddedLeaderElectionService, 一旦有参与选举的 LeaderContender 加入，该 contender 就被选择为 leader。
				 */
				haServices = HighAvailabilityServicesUtils.createAvailableOrEmbeddedServices(
					configuration,
					commonRpcService.getExecutor());

				blobServer = new BlobServer(configuration, haServices.createBlobStore());  //mini cluster 情况下不做任何事
				blobServer.start();

				heartbeatServices = HeartbeatServices.fromConfiguration(configuration);

				// bring up the ResourceManager(s)
				LOG.info("Starting ResourceManger");

				/**
				 * 在启动tm之前创建resourceManager，然后直接调用start启动，会看到参与选举的过程
				 */
				resourceManagerRunner = startResourceManager(
					configuration,
					haServices,
					heartbeatServices,
					metricRegistry,
					resourceManagerRpcService,
					new ClusterInformation("localhost", blobServer.getPort()));

				blobCacheService = new BlobCacheService(
					configuration, haServices.createBlobStore(), new InetSocketAddress(InetAddress.getLocalHost(), blobServer.getPort())
				);

				// bring up the TaskManager(s) for the mini cluster
				LOG.info("Starting {} TaskManger(s)", numTaskManagers);

				/**
				 * 启动所有的tm，也即是启动TaskExecutor(实现了Endpoint和TaskExecutorGateway)，
				 * 稍微具体点的过程：
				 * TaskManagerRunner#startTaskManager 会创建一个 TaskExecutor, TaskExecutor 实现了 RpcEndpoint 接口。TaskExecutor创建完成之后，就直接调用了它的start方法，start方法中，
				 * 会从 HighAvailabilityServices(EmbeddedHaServices) 中获取到 ResourceManagerLeaderRetriever，然后通过监听回调的方式，获取rm的地址，和rm建立链接，建立链接之后可以获取到rm的RpcGateway，
				 * 然后通过rpc的方式，向rm注册自己(ResourceManager#registerTaskExecutor)，然后把当前tm的slotReport汇报给rm(ResourceManager#sendSlotReport)。
				 */
				taskManagers = startTaskManagers(
					configuration, //config
					haServices,    // EmbeddedHaServices
					heartbeatServices, //心跳
					metricRegistry,
					blobCacheService,
					numTaskManagers,  //tm 个数
					taskManagerRpcServices);  //tm 的  RpcService

				// starting the dispatcher rest endpoint
				LOG.info("Starting dispatcher rest endpoint.");


				dispatcherGatewayRetriever = new RpcGatewayRetriever<>(   //通过它的createGateway方法可以获取到 DispatcherGateway 的远程代理对象
					jobManagerRpcService,
					DispatcherGateway.class, //要获取 DispatcherGateway的远程代理对象
					DispatcherId::fromUuid,
					20,
					Time.milliseconds(20L));

				//通过它的createGateway方法可以获取到 ResourceManagerGateway 的远程代理对象
				final RpcGatewayRetriever<ResourceManagerId, ResourceManagerGateway> resourceManagerGatewayRetriever = new RpcGatewayRetriever<>(
					jobManagerRpcService,
					ResourceManagerGateway.class, //要获取 ResourceManagerGateway 的远程代理对象
					ResourceManagerId::fromUuid,
					20,
					Time.milliseconds(20L));

				/**
				 *  client与JobManager在以前(Version 1.4及以前）也是通过AKKA(实现的RPCService)通讯的，但Version1.5及以后版本的JobManager里引入DispatcherRestEndPoint (目的是使Client请求可以在穿过Firewall ？)，
				 *  从此client端与JobManager提供的 DispatcherRestEndpoint 通讯。
				 */
				this.dispatcherRestEndpoint = new DispatcherRestEndpoint(  //rest方式接收并处理JobGraph；
					RestServerEndpointConfiguration.fromConfiguration(configuration),
					dispatcherGatewayRetriever,
					configuration,
					RestHandlerConfiguration.fromConfiguration(configuration),
					resourceManagerGatewayRetriever,
					blobServer.getTransientBlobService(),
					commonRpcService.getExecutor(),
					new AkkaQueryServiceRetriever(
						actorSystem,
						Time.milliseconds(configuration.getLong(WebOptions.TIMEOUT))),
					haServices.getWebMonitorLeaderElectionService(),
					new ShutDownFatalErrorHandler());

				dispatcherRestEndpoint.start();

				restAddressURI = new URI(dispatcherRestEndpoint.getRestBaseUrl());

				// bring up the dispatcher that launches JobManagers when jobs submitted
				LOG.info("Starting job dispatcher(s) for JobManger");

				this.jobManagerMetricGroup = MetricUtils.instantiateJobManagerMetricGroup(metricRegistry, "localhost");

				final HistoryServerArchivist historyServerArchivist = HistoryServerArchivist.createHistoryServerArchivist(configuration, dispatcherRestEndpoint);

				/**
				 * 创建 StandaloneDispatcher
				 */
				dispatcher = new StandaloneDispatcher(
					jobManagerRpcService,
					Dispatcher.DISPATCHER_NAME + UUID.randomUUID(),
					configuration,
					haServices,
					resourceManagerRunner.getResourceManageGateway(),
					blobServer,
					heartbeatServices,
					jobManagerMetricGroup,
					metricRegistry.getMetricQueryServicePath(),
					new MemoryArchivedExecutionGraphStore(),
					Dispatcher.DefaultJobManagerRunnerFactory.INSTANCE,
					new ShutDownFatalErrorHandler(),
					dispatcherRestEndpoint.getRestBaseUrl(),
					historyServerArchivist);

				dispatcher.start(); //直接启动，也会看到参与选举的过程；

				resourceManagerLeaderRetriever = haServices.getResourceManagerLeaderRetriever();  //获取rm的leader检索的服务；
				dispatcherLeaderRetriever = haServices.getDispatcherLeaderRetriever(); 			//获取dispatcher的leader检索的服务；

				resourceManagerLeaderRetriever.start(resourceManagerGatewayRetriever); //向rm的leader检索服务注册resourceManagerGatewayRetriever这个监听器，使其可以通过createGateway获取到ResourceManagerGateway的远程代理对象
				dispatcherLeaderRetriever.start(dispatcherGatewayRetriever);   //同理
			}
			catch (Exception e) {
				// cleanup everything
				try {
					close();
				} catch (Exception ee) {
					e.addSuppressed(ee);
				}
				throw e;
			}

			// create a new termination future
			terminationFuture = new CompletableFuture<>();

			// now officially mark this as running
			running = true;

			LOG.info("Flink Mini Cluster started successfully");
		}
	}

	/**
	 * Shuts down the mini cluster, failing all currently executing jobs.
	 * The mini cluster can be started again by calling the {@link #start()} method again.
	 *
	 * <p>This method shuts down all started services and components,
	 * even if an exception occurs in the process of shutting down some component.
	 *
	 * @return Future which is completed once the MiniCluster has been completely shut down
	 */
	@Override
	public CompletableFuture<Void> closeAsync() {
		synchronized (lock) {
			if (running) {
				LOG.info("Shutting down Flink Mini Cluster");
				try {
					final int numComponents = 2 + miniClusterConfiguration.getNumTaskManagers();
					final Collection<CompletableFuture<Void>> componentTerminationFutures = new ArrayList<>(numComponents);

					if (taskManagers != null) {
						for (TaskExecutor tm : taskManagers) {
							if (tm != null) {
								tm.shutDown();
								componentTerminationFutures.add(tm.getTerminationFuture());
							}
						}
						taskManagers = null;
					}

					componentTerminationFutures.add(shutDownDispatcher());

					if (resourceManagerRunner != null) {
						componentTerminationFutures.add(resourceManagerRunner.closeAsync());
						resourceManagerRunner = null;
					}

					final FutureUtils.ConjunctFuture<Void> componentsTerminationFuture = FutureUtils.completeAll(componentTerminationFutures);

					final CompletableFuture<Void> metricRegistryTerminationFuture = FutureUtils.runAfterwards(
						componentsTerminationFuture,
						() -> {
							synchronized (lock) {
								if (jobManagerMetricGroup != null) {
									jobManagerMetricGroup.close();
									jobManagerMetricGroup = null;
								}
								// metrics shutdown
								if (metricRegistry != null) {
									metricRegistry.shutdown();
									metricRegistry = null;
								}
							}
						});

					// shut down the RpcServices
					final CompletableFuture<Void> rpcServicesTerminationFuture = metricRegistryTerminationFuture
						.thenCompose((Void ignored) -> terminateRpcServices());

					final CompletableFuture<Void> remainingServicesTerminationFuture = FutureUtils.runAfterwards(
						rpcServicesTerminationFuture,
						this::terminateMiniClusterServices);

						remainingServicesTerminationFuture.whenComplete(
							(Void ignored, Throwable throwable) -> {
								if (throwable != null) {
									terminationFuture.completeExceptionally(ExceptionUtils.stripCompletionException(throwable));
								} else {
									terminationFuture.complete(null);
								}
							});
				} finally {
					running = false;
				}
			}

			return terminationFuture;
		}
	}

	// ------------------------------------------------------------------------
	//  Accessing jobs
	// ------------------------------------------------------------------------

	public CompletableFuture<Collection<JobStatusMessage>> listJobs() {
		try {
			return getDispatcherGateway().requestMultipleJobDetails(rpcTimeout)
				.thenApply(jobs -> jobs.getJobs().stream()
					.map(details -> new JobStatusMessage(details.getJobId(), details.getJobName(), details.getStatus(), details.getStartTime()))
					.collect(Collectors.toList()));
		} catch (LeaderRetrievalException | InterruptedException e) {
			return FutureUtils.completedExceptionally(
				new FlinkException(
					"Could not retrieve job list.",
					e));
		}
	}

	public CompletableFuture<JobStatus> getJobStatus(JobID jobId) {
		try {
			return getDispatcherGateway().requestJobStatus(jobId, rpcTimeout);
		} catch (LeaderRetrievalException | InterruptedException e) {
			return FutureUtils.completedExceptionally(
				new FlinkException(
					String.format("Could not retrieve job status for job %s.", jobId),
					e));
		}
	}

	public CompletableFuture<Acknowledge> cancelJob(JobID jobId) {
		try {
			return getDispatcherGateway().cancelJob(jobId, rpcTimeout);
		} catch (LeaderRetrievalException | InterruptedException e) {
			return FutureUtils.completedExceptionally(
				new FlinkException(
					String.format("Could not cancel job %s.", jobId),
					e));
		}
	}

	public CompletableFuture<Acknowledge> stopJob(JobID jobId) {
		try {
			return getDispatcherGateway().stopJob(jobId, rpcTimeout);
		} catch (LeaderRetrievalException | InterruptedException e) {
			return FutureUtils.completedExceptionally(
				new FlinkException(
					String.format("Could not stop job %s.", jobId),
					e));
		}
	}

	public CompletableFuture<String> triggerSavepoint(JobID jobId, String targetDirectory, boolean cancelJob) {
		try {
			return getDispatcherGateway().triggerSavepoint(jobId, targetDirectory, cancelJob, rpcTimeout);
		} catch (LeaderRetrievalException | InterruptedException e) {
			return FutureUtils.completedExceptionally(
				new FlinkException(
					String.format("Could not trigger savepoint for job %s.", jobId),
					e));
		}
	}

	public CompletableFuture<Acknowledge> disposeSavepoint(String savepointPath) {
		try {
			return getDispatcherGateway().disposeSavepoint(savepointPath, rpcTimeout);
		} catch (LeaderRetrievalException | InterruptedException e) {
			ExceptionUtils.checkInterrupted(e);
			return FutureUtils.completedExceptionally(
				new FlinkException(
					String.format("Could not dispose savepoint %s.", savepointPath),
					e));
		}
	}

	public CompletableFuture<? extends AccessExecutionGraph> getExecutionGraph(JobID jobId) {
		try {
			return getDispatcherGateway().requestJob(jobId, rpcTimeout);
		} catch (LeaderRetrievalException | InterruptedException e) {
			return FutureUtils.completedExceptionally(
				new FlinkException(
					String.format("Could not retrieve job job %s.", jobId),
					e));
		}
	}

	// ------------------------------------------------------------------------
	//  running jobs
	// ------------------------------------------------------------------------

	/**
	 * This method executes a job in detached mode. The method returns immediately after the job
	 * has been added to the
	 *
	 * @param job  The Flink job to execute
	 *
	 * @throws JobExecutionException Thrown if anything went amiss during initial job launch,
	 *         or if the job terminally failed.
	 */
	public void runDetached(JobGraph job) throws JobExecutionException, InterruptedException {
		checkNotNull(job, "job is null");

		final CompletableFuture<JobSubmissionResult> submissionFuture = submitJob(job);

		try {
			submissionFuture.get();
		} catch (ExecutionException e) {
			throw new JobExecutionException(job.getJobID(), ExceptionUtils.stripExecutionException(e));
		}
	}

	/**
	 * This method runs a job in blocking mode. The method returns only after the job
	 * completed successfully, or after it failed terminally.
	 *
	 * @param job  The Flink job to execute
	 * @return The result of the job execution
	 *
	 * @throws JobExecutionException Thrown if anything went amiss during initial job launch,
	 *         or if the job terminally failed.
	 */
	/**
	 * 阻塞的方式提交一个 job
	 * 本地模式的入口方法
	 */
	@Override
	public JobExecutionResult executeJobBlocking(JobGraph job) throws JobExecutionException, InterruptedException {
		checkNotNull(job, "job is null");


		//在这里，最终把job提交给了jobMaster,这段代码核心逻辑就是调用这个 submitJob 方法。
		final CompletableFuture<JobSubmissionResult> submissionFuture = submitJob(job);

		final CompletableFuture<JobResult> jobResultFuture = submissionFuture.thenCompose(
			(JobSubmissionResult ignored) -> requestJobResult(job.getJobID()));

		final JobResult jobResult;

		try {
			jobResult = jobResultFuture.get();
		} catch (ExecutionException e) {
			throw new JobExecutionException(job.getJobID(), "Could not retrieve JobResult.", ExceptionUtils.stripExecutionException(e));
		}

		try {
			return jobResult.toJobExecutionResult(Thread.currentThread().getContextClassLoader());
		} catch (JobResult.WrappedJobException e) {
			throw new JobExecutionException(job.getJobID(), e.getCause());
		} catch (IOException | ClassNotFoundException e) {
			throw new JobExecutionException(job.getJobID(), e);
		}
	}

	/**
	 * 提交job的核心方法
	 * @param jobGraph
	 * @return
	 */
	public CompletableFuture<JobSubmissionResult> submitJob(JobGraph jobGraph) {
		final DispatcherGateway dispatcherGateway;
		try {
			//通过 Dispatcher 的 gateway retriever 获取 DispatcherGateway
			dispatcherGateway = getDispatcherGateway();  //远程代理对象；
		} catch (LeaderRetrievalException | InterruptedException e) {
			ExceptionUtils.checkInterrupted(e);
			return FutureUtils.completedExceptionally(e);
		}

		// we have to allow queued scheduling in Flip-6 mode because we need to request slots
		// from the ResourceManager
		jobGraph.setAllowQueuedScheduling(true);

		final CompletableFuture<InetSocketAddress> blobServerAddressFuture = createBlobServerAddress(dispatcherGateway);

		final CompletableFuture<Void> jarUploadFuture = uploadAndSetJobFiles(blobServerAddressFuture, jobGraph);

		//通过 RPC 调用向 Dispatcher 提交 JobGraph
		final CompletableFuture<Acknowledge> acknowledgeCompletableFuture = jarUploadFuture.thenCompose(
			//在这里执行了真正的submit操作
			/**
			 * 这里的 Dispatcher 是一个接收job，然后指派JobMaster去启动任务的类,
			 * 我们可以看看它的 类结构，有两个实现。在本地环境下启动的是 MiniDispatcher ，
			 * 在集群上提交任务时，集群 上启动的是 StandaloneDispatcher
			 *
			 * 提交之后 Dispatcher的后续的处理的过程可以看 Dispatcher类的submitJob方法；
			 */
			(Void ack) -> dispatcherGateway.submitJob(jobGraph, rpcTimeout));

		return acknowledgeCompletableFuture.thenApply(
			(Acknowledge ignored) -> new JobSubmissionResult(jobGraph.getJobID()));
	}

	public CompletableFuture<JobResult> requestJobResult(JobID jobId) {
		final DispatcherGateway dispatcherGateway;
		try {
			dispatcherGateway = getDispatcherGateway();
		} catch (LeaderRetrievalException | InterruptedException e) {
			ExceptionUtils.checkInterrupted(e);
			return FutureUtils.completedExceptionally(e);
		}

		return dispatcherGateway.requestJobResult(jobId, RpcUtils.INF_TIMEOUT);
	}

	/**
	 * 获取disPatcherGateWay的远程代理对象
	 */
	private DispatcherGateway getDispatcherGateway() throws LeaderRetrievalException, InterruptedException {
		synchronized (lock) {
			checkState(running, "MiniCluster is not yet running.");
			try {
				return dispatcherGatewayRetriever.getFuture().get();
			} catch (ExecutionException e) {
				throw new LeaderRetrievalException("Could not retrieve the leading dispatcher.", ExceptionUtils.stripExecutionException(e));
			}
		}
	}

	private CompletableFuture<Void> uploadAndSetJobFiles(final CompletableFuture<InetSocketAddress> blobServerAddressFuture, final JobGraph job) {
		return blobServerAddressFuture.thenAccept(blobServerAddress -> {
			try {
				ClientUtils.extractAndUploadJobGraphFiles(job, () -> new BlobClient(blobServerAddress, miniClusterConfiguration.getConfiguration()));
			} catch (FlinkException e) {
				throw new CompletionException(e);
			}
		});
	}

	private CompletableFuture<InetSocketAddress> createBlobServerAddress(final DispatcherGateway currentDispatcherGateway) {
		return currentDispatcherGateway.getBlobServerPort(rpcTimeout)
			.thenApply(blobServerPort -> new InetSocketAddress(currentDispatcherGateway.getHostname(), blobServerPort));
	}

	// ------------------------------------------------------------------------
	//  factories - can be overridden by subclasses to alter behavior
	// ------------------------------------------------------------------------

	/**
	 * Factory method to create the metric registry for the mini cluster.
	 *
	 * @param config The configuration of the mini cluster
	 */
	protected MetricRegistryImpl createMetricRegistry(Configuration config) {
		return new MetricRegistryImpl(MetricRegistryConfiguration.fromConfiguration(config));
	}

	/**
	 * Factory method to instantiate the RPC service.
	 *
	 * @param configuration
	 *            The configuration of the mini cluster
	 * @param askTimeout
	 *            The default RPC timeout for asynchronous "ask" requests.
	 * @param remoteEnabled
	 *            True, if the RPC service should be reachable from other (remote) RPC services.
	 * @param bindAddress
	 *            The address to bind the RPC service to. Only relevant when "remoteEnabled" is true.
	 *
	 * @return The instantiated RPC service
	 */
	protected RpcService createRpcService(
			Configuration configuration,
			Time askTimeout,
			boolean remoteEnabled,
			String bindAddress) {

		final Config akkaConfig;

		if (remoteEnabled) {
			akkaConfig = AkkaUtils.getAkkaConfig(configuration, bindAddress, 0);
		} else {
			akkaConfig = AkkaUtils.getAkkaConfig(configuration);
		}

		final Config effectiveAkkaConfig = AkkaUtils.testDispatcherConfig().withFallback(akkaConfig);

		final ActorSystem actorSystem = AkkaUtils.createActorSystem(effectiveAkkaConfig);

		return new AkkaRpcService(actorSystem, askTimeout);
	}

	protected ResourceManagerRunner startResourceManager(
			Configuration configuration,
			HighAvailabilityServices haServices,
			HeartbeatServices heartbeatServices,
			MetricRegistry metricRegistry,
			RpcService resourceManagerRpcService,
			ClusterInformation clusterInformation) throws Exception {

		final ResourceManagerRunner resourceManagerRunner = new ResourceManagerRunner(
			ResourceID.generate(),
			FlinkResourceManager.RESOURCE_MANAGER_NAME + '_' + UUID.randomUUID(),
			configuration,
			resourceManagerRpcService,
			haServices,
			heartbeatServices,
			metricRegistry,
			clusterInformation);

			resourceManagerRunner.start();

		return resourceManagerRunner;
	}

	//启动 tm
	protected TaskExecutor[] startTaskManagers(
			Configuration configuration,
			HighAvailabilityServices haServices,
			HeartbeatServices heartbeatServices,
			MetricRegistry metricRegistry,
			BlobCacheService blobCacheService,
			int numTaskManagers,
			RpcService[] taskManagerRpcServices) throws Exception {

		final TaskExecutor[] taskExecutors = new TaskExecutor[numTaskManagers];  //TaskExecutor是具体的tm需要代理的接口的实现，它实现了 RpcEndpoint 和 TaskExecutorGateway(定义服务接口方法)
		final boolean localCommunication = numTaskManagers == 1;

		for (int i = 0; i < numTaskManagers; i++) {
			taskExecutors[i] = TaskManagerRunner.startTaskManager(    //创建具体的 TaskExecutor
				configuration,
				new ResourceID(UUID.randomUUID().toString()),
				taskManagerRpcServices[i],
				haServices,
				heartbeatServices,
				metricRegistry,
				blobCacheService,
				localCommunication,
				new TerminatingFatalErrorHandler(i));

			taskExecutors[i].start();  //life cycle  start方法；
		}

		return taskExecutors;
	}

	// ------------------------------------------------------------------------
	//  Internal methods
	// ------------------------------------------------------------------------

	@GuardedBy("lock")
	private CompletableFuture<Void> shutDownDispatcher() {

		final Collection<CompletableFuture<Void>> terminationFutures = new ArrayList<>(2);

		// cancel all jobs and shut down the job dispatcher
		if (dispatcher != null) {
			dispatcher.shutDown();
			terminationFutures.add(dispatcher.getTerminationFuture());

			dispatcher = null;
		}

		if (dispatcherRestEndpoint != null) {
			terminationFutures.add(dispatcherRestEndpoint.closeAsync());

			dispatcherRestEndpoint = null;
		}

		final FutureUtils.ConjunctFuture<Void> dispatcherTerminationFuture = FutureUtils.completeAll(terminationFutures);

		return FutureUtils.runAfterwards(
			dispatcherTerminationFuture,
			() -> {
				Exception exception = null;

				synchronized (lock) {
					if (resourceManagerLeaderRetriever != null) {
						try {
							resourceManagerLeaderRetriever.stop();
						} catch (Exception e) {
							exception = ExceptionUtils.firstOrSuppressed(e, exception);
						}

						resourceManagerLeaderRetriever = null;
					}

					if (dispatcherLeaderRetriever != null) {
						try {
							dispatcherLeaderRetriever.stop();
						} catch (Exception e) {
							exception = ExceptionUtils.firstOrSuppressed(e, exception);
						}

						dispatcherLeaderRetriever = null;
					}
				}

				if (exception != null) {
					throw exception;
				}
			});
	}

	private void terminateMiniClusterServices() throws Exception {
		// collect the first exception, but continue and add all successive
		// exceptions as suppressed
		Exception exception = null;

		synchronized (lock) {
			if (blobCacheService != null) {
				try {
					blobCacheService.close();
				} catch (Exception e) {
					exception = ExceptionUtils.firstOrSuppressed(e, exception);
				}
				blobCacheService = null;
			}

			// shut down the blob server
			if (blobServer != null) {
				try {
					blobServer.close();
				} catch (Exception e) {
					exception = ExceptionUtils.firstOrSuppressed(e, exception);
				}
				blobServer = null;
			}

			// shut down high-availability services
			if (haServices != null) {
				try {
					haServices.closeAndCleanupAllData();
				} catch (Exception e) {
					exception = ExceptionUtils.firstOrSuppressed(e, exception);
				}
				haServices = null;
			}

			if (exception != null) {
				throw exception;
			}
		}
	}

	@Nonnull
	private CompletionStage<Void> terminateRpcServices() {
		final int numRpcServices;
		if (miniClusterConfiguration.getRpcServiceSharing() == RpcServiceSharing.SHARED) {
			numRpcServices = 1;
		} else {
			numRpcServices = 1 + 2 + miniClusterConfiguration.getNumTaskManagers(); // common, JM, RM, TMs
		}

		final Collection<CompletableFuture<?>> rpcTerminationFutures = new ArrayList<>(numRpcServices);

		synchronized (lock) {
			rpcTerminationFutures.add(commonRpcService.stopService());

			if (miniClusterConfiguration.getRpcServiceSharing() != RpcServiceSharing.SHARED) {
				rpcTerminationFutures.add(jobManagerRpcService.stopService());
				rpcTerminationFutures.add(resourceManagerRpcService.stopService());

				for (RpcService taskManagerRpcService : taskManagerRpcServices) {
					rpcTerminationFutures.add(taskManagerRpcService.stopService());
				}
			}

			commonRpcService = null;
			jobManagerRpcService = null;
			taskManagerRpcServices = null;
			resourceManagerRpcService = null;
		}

		return FutureUtils.completeAll(rpcTerminationFutures);
	}

	// ------------------------------------------------------------------------
	//  miscellaneous utilities
	// ------------------------------------------------------------------------

	private void initializeIOFormatClasses(Configuration configuration) {
		// TODO: That we still have to call something like this is a crime against humanity
		FileOutputFormat.initDefaultsFromConfiguration(configuration);
	}

	private static Throwable shutDownRpc(RpcService rpcService, Throwable priorException) {
		if (rpcService != null) {
			try {
				rpcService.stopService().get();
			}
			catch (Throwable t) {
				return ExceptionUtils.firstOrSuppressed(t, priorException);
			}
		}

		return priorException;
	}

	private static Throwable shutDownRpcs(RpcService[] rpcServices, Throwable priorException) {
		if (rpcServices != null) {
			Throwable exception = priorException;

			for (RpcService service : rpcServices) {
				try {
					if (service != null) {
						service.stopService().get();
					}
				}
				catch (Throwable t) {
					exception = ExceptionUtils.firstOrSuppressed(t, exception);
				}
			}
		}
		return priorException;
	}

	private class TerminatingFatalErrorHandler implements FatalErrorHandler {

		private final int index;

		private TerminatingFatalErrorHandler(int index) {
			this.index = index;
		}

		@Override
		public void onFatalError(Throwable exception) {
			// first check if we are still running
			if (running) {
				LOG.error("TaskManager #{} failed.", index, exception);

				// let's check if there are still TaskManagers because there could be a concurrent
				// shut down operation taking place
				TaskExecutor[] currentTaskManagers = taskManagers;

				if (currentTaskManagers != null) {
					// the shutDown is asynchronous
					currentTaskManagers[index].shutDown();
				}
			}
		}
	}

	private class ShutDownFatalErrorHandler implements FatalErrorHandler {

		@Override
		public void onFatalError(Throwable exception) {
			LOG.warn("Error in MiniCluster. Shutting the MiniCluster down.", exception);
			closeAsync();
		}
	}
}

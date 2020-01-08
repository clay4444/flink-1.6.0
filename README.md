
### 大致流程
1. 启rm       -> leader 竞选
2. 启tm，tm汇报slot给rm(刚启动/心跳)         -> 没有leader 竞选
3. 启dispatcher，         -> leader 竞选
4. 开始调用阻塞执行方法，dispatcher#submitJob jobGraph,  (rpc)
5. dispatcher#submitJob:  创建 JobManagerRunner(用来启jobMaster), 然后start启动，       -> leader竞选
6. JobManagerRunner 拿到授权，回调时，启动jobMaster，然后和 rm 建立连接 (需要向rm申请资源)
7. executionGraph.scheduleForExecution()  真正执行；
8. 注意：jobMaster(jobManagerRunner) 是在dispatcher接收到用户作业的时候创建的，而不是在集群启动的时候创建的；

**具体的源码解析需要查看 MiniCluster 的 start() 方法**


### 所有涉及到选举服务的组件
1. rm
2. dispatcher
3. jobMaster (JobManagerRunner)


### 资源的管理 (slot)

#### tm 中 slot的管理

TaskSlot 是在 TaskExecutor 中对 slot 的抽象，可能处于 Free, Releasing, Allocated, Active 这四种状态之中;
TaskExecutor 主要通过 TaskSlotTable 来管理它所拥有的所有 slot;
TaskSlotTable 的 allocateSlot() 方法就是用来将指定 index 的 slot 分配给 AllocationID 对应的请求，(注意这里超时时间的作用)

大致的过程是这样的，rm先启动，然后tm启动，tm启动时，会和rm建立连接，然后tm会把当前所有slot的情况汇报给rm，
主要的代码逻辑在TaskExecutor类的 establishResourceManagerConnection() 方法中；

在之后的心跳过程中，也会把所有slot情况汇报给rm，也就是说rm拥有所有slot的status；
主要的代码逻辑在TaskExecutor类的 ResourceManagerHeartbeatListener 这个内部类中；

程序启动时，jobMaster接收jobGraph，转化为ExecutionGraph，然后开始执行，然后会向rm申请资源，rm包含着所有SlotStatus，所以会向
具体某个tm发起requestSlot rpc调用，传一个jobId，
主要的代码逻辑在TaskExecutor.requestSlot()方法中，这个方法是被rm通过rpc调用的；

然后tm会rpc调用 JobMaster.offerSlots(); 把slot分配给具体的jm；
主要的代码逻辑在TaskExecutor.offerSlotsToJobManager()方法中，这个方法会rpc调用 jobMasterGateway.offerSlots() 方法；

总结一下，tm对slot的管理是比较简单的，主要通过 TaskSlotTable 来维护所有的slot，rm向tm请求资源，然后tm向jm分配资源；


#### rm 中 slot的管理
rm需要对所有的tm的slot进行管理，jobMaster都是向rm申请资源的；然后rm把请求 "转发" 给tm；tm offer slot 给jm；
rm主要通过 SlotManager 来对 所有的slot进行管理；SlotManager 主要维护的就是所有slot的状态 和 所有 pending slot request 的状态；
SlotManager start时，会启动两个线程，一个检测tm是否长期idle状态，是则释放这个tm。另一个检测request是否长时间不被满足，是则取消这个request

SlotManager的主要功能有：
1. 注册slot，从 registerTaskManager 开始，注册一个tm的所有slot，每次注册一个slot，都会检查是否满足某个request，是则触发分配slot逻辑
2. 请求slot，从 registerSlotRequest 开始，jm发起，如果有free且满足条件的slot，直接分配，否则进等待队列
3. 具体分配逻辑，rpc调用tm的方法，进行分配，tm会把具体的slot offer 给 jm；
3. 取消slot请求，unregisterSlotRequest，直接cancel掉 slot request 对应的future；

rm的rpc方法中，主要有四个用于管理slot
1. requestSlot： 供jm调用
2. cancelSlotRequest： 供jm调用
3. sendSlotReport: 供tm调用
4. notifySlotAvailable:  供tm调用

动态资源管理：standAlone无法实现，yarn可以；
核心思想大概是这样的：jm发起request slot，但是找不到match的slot，此时会调用resourceActions#allocateResource 申请新资源，ResourceActions
的唯一实现是 ResourceManager#ResourceActionsImpl，所以调用到这里，然后又调用抽象方法startNewWorker()，这是个模板方法，交给具体的rm
去实现，比如yarn rm； 这样就实现了资源的动态申请；资源的动态释放(比如tm被检测到长期idle) 同理，ResourceActionsImpl#releaseResource()，
最终调用到模板(抽象)方法：stopWorker();
以yarn为例，动态申请可能就是申请一个container，动态释放可能就是释放一个container

#### jm 中 slot 的管理
比较复杂

- 概念区分：
1. PhysicalSlot：
表征的是物理意义上TaskExecutor上的一个slot，(注意1.6版本没有这个类，只有它的子类 AllocatedSlot)，
AllocatedSlot.Payload 接口代表的是可以分配给具体的slot(AllocatedSlot) 的负载(任务)；实现类：
    1.SingleLogicalSlot：这个类实现的是：一个 LogicalSlot 映射到一个 PhysicalSlot 上，
    2.SlotSharingManager.MultiTaskSlot: **flink资源共享实现多个LogicalSlot映射到同一个 PhysicalSlot 上的核心**


2. LogicalSlot
表征逻辑上的一个slot，一个task是部署到一个LogicalSlot上的，但它和物理上一个具体的slot并不是一一对应的。
由于资源共享等机制的存在，多个LogicalSlot可能被映射到同一个PhysicalSlot上。
实现类SingleLogicalSlot，


- 如何实现资源共享？
核心主要是SlotSharingManager.TaskSlot内部类，及其两个子类 MultiTaskSlot 和 SingleTaskSlot，他们组成一棵多叉树，这棵树公用一个物理slot
其中MultiTaskSlot用来代表内部(中间)节点，或者根节点。作为根节点时会有一个 SlotContext，代表一个物理slot
SingleTaskSlot只能代表叶子节点，


- 核心：SlotPool (它只用来管理所有的物理 slot)
JobManager使用SlotPool来向ResourceManager申请slot，并管理所有分配给该JobManager的slots。这里所说的slot指的都是 physical slot。
内部有一些容器用来保存所有的slot情况，比如allocatedSlots代表所有分配给当前JobManager的slots；availableSlots代表所有可用的slots(还没装载payload的) ....

下面的流程第一句有点问题，开始调度执行task的时候，不是直接向SlotPool申请资源的，而是向Scheduler申请logical slot的，Scheduler再根据slotShareingGroup的设置，决定何时向slot pool 申请physicalslot；

大致流程其实可以总结一下，首先executionGraph在jm开始调度执行所有task，首先就需要先申请计算资源，也就是slot，这里会调用 requestAllocatedSlot() 方法；
方法首先尝试从当前可用的 slot 中获取，没有获取到，就调用requestNewAllocatedSlot()方法来向rm申请新的slot，(rpc调用resourceManagerGateway.requestSlot,上面看了)
然后tm会通过rpc回调offerSlots()方法，为当前jm分配slot，如果有request在等待这个slot，会直接分配，如果没有，会调用tryFulfillSlotRequestOrMakeAvailable()方法
来尝试满足其他的slot request，满足的话，也会直接分配，否则，会放进availableSlots容器中；

slotPool启动的时候会开启一个定时调度的任务，周期性地检查空闲的slot，如果slot空闲时间过长，会将该slot归还给 TaskManager: checkIdleSlot()方法

- Scheduler(核心，任务调度时LogicalSlot资源的申请就是通过它来做的) 和 SlotSharingManager
SlotPool主要负责的是分配给当前JobMaster的PhysicalSlot的管理。但是，具体到每一个Task所需要的计算资源的调度和管理，是按照LogicalSlot进行组织的，
不同的Task所分配的LogicalSlot各不相同，但它们底层的 PhysicalSlot 可能是同一个。主要的逻辑都封装在 SlotSharingManager 和 Scheduler 中。

前面已经提到过，通过构造一个由TaskSlot构成的树形结构可以实现SlotSharingGroup内的资源共享以及CoLocationGroup的强制约束，这主要就是通过SlotSharingManager来完成的。
**每一个SlotSharingGroup都会有一个与其对应的 SlotSharingManager**

任务调度时 LogicalSlot 资源的申请通过 Scheduler 接口进行管理，Scheduler 接口继承了 SlotProvider 接口；(这里的源码解析以1.9的为主，1.9和1.6的差别有点大)
Scheduler主要有两个变量；
    1.SlotPool
    2.slotSharingManagers
所以主要的设计思想也很明显，借助SlotPool来申请PhysicalSlot，借助SlotSharingManager实现slot共享。

Scheduler最核心的方法也就几个
    1.allocateSlot (job开始调度执行时，调用这个方法来申请 LogicalSlot )
    。。。。。。

这里主要看一下allocateSlot这个方法的执行过程：
先看有没有指定SlotSharingGroupId，如果没有指定，说明这个任务不运行slot共享，要独占一个slot，此时会调用allocateSingleSlot()方法，直接从SlotPool中获取PhysicalSlot，然后创建一个LogicalSlot即可：
如果允许资源共享，此时会调用allocateSharedSlot()方法，这个方法的核心就在于构造TaskSlot 构成的树；细节先跳过把；



### 内存管理
why: 1.大量的数据保存在堆内存，很容易导致OOM；  2.GC严重影响性能   3.对象头、对象填充 浪费空间
how: 将对象序列化(二进制) 到一个个预先分配的 MemorySegment 中；MemorySegment 是一段固定长度的内存（默认32KB），也是 Flink 中最小的内存分配单元；

#### tm的三部分内存：
1. Network Buffers，用于网络传输
2. Managed Memory，由 MemoryManager 管理的一组 MemorySegment 集合；主要是在 Batch 模式下使用，在 Streaming 模式下这一部分内存并不会预分配
3. Remaining JVM heap，主要给用户代码使用

#### 具体序列化 的机制：
为了性能考虑，自己实现了一套序列化机制；
Flink 可以处理任意的 Java 或 Scala 对象，而不必实现特定的接口。对于 Java 实现的 Flink 程序，Flink 会通过反射框架获取用户自定义函数返回的类型；而对于 Scala 实现的 Flink 程序，则通过 Scala Compiler 分析用户自定义函数返回的类型。
每一种数据类型都对应一个 TypeInformation。大致分为下面几种：
1. BasicTypeInfo: 基本类型（装箱的）或 String 类型      -> flink提供具体的序列化器
2. BasicArrayTypeInfo: 基本类型数组（装箱的）或 String 数组        -> flink提供具体的序列化器
3. WritableTypeInfo: 任意 Hadoop Writable 接口的实现类      -> 序列化/反序列化 委托给Hadoop 
4. TupleTypeInfo: 任意的 Flink Tuple 类型 (支持Tuple1 to Tuple25)       -> 下面三种都是组合类型
5. CaseClassTypeInfo: 任意的 Scala CaseClass (包括 Scala tuples)
6. PojoTypeInfo: 任意的 POJO (Java or Scala)，Java对象的所有成员变量，要么是 public 修饰符定义，要么有 getter/setter 方法
7. GenericTypeInfo: 任意无法匹配之前几种类型的类      ->  默认使用 Kyro 

#### 好处
1. 保证内存安全：由于分配的 MemorySegment 的数量是固定的，因而可以准确地追踪 MemorySegment 的使用情况。在 Batch 模式下，如果 MemorySegment 资源不足，会将一批 MemorySegment 写入磁盘，需要时再重新读取。这样有效地减少了 OOM 的情况。
2. 减少了 GC 的压力：因为分配的 MemorySegment 是长生命周期的对象，数据都以二进制形式存放，且 MemorySegment 可以回收重用，所以 MemorySegment 会一直保留在老年代不会被 GC；而由用户代码生成的对象基本都是短生命周期的，Minor GC 可以快速回收这部分对象，尽可能减少 Major GC 的频率。此外，MemorySegment 还可以配置为使用堆外内存，进而避免 GC。
3. 节省内存空间：数据对象序列化后以二进制形式保存在 MemorySegment 中，减少了对象存储的开销。
4. 高效的二进制操作和缓存友好的计算：可以直接基于二进制数据进行比较等操作，避免了反复进行序列化于反序列；另外，二进制形式可以把相关的值，以及 hash 值，键值和指针等相邻地放进内存中，这使得数据结构可以对高速缓存更友好。


### task之间的数据传输
具体的分析在 InputGate(数据输入) 和 ResultPartition(数据输出)

### Task的生命周期

问题1：上游算子处理之后的记录如何传递给下游算子？  主要是通过 OutPut 这个接口，这个接口的作用就是为了实现往下游发送数据；

-  一个 Task 运行期间的主要处理逻辑对应一个 OperatorChain，这个 OperatorChain 可能包含多个 Operator，也可能只有一个 Operator。
-  operator算子处理完的数据收集主要靠 OutPut 接口，每一个 StreamOperator 都有一个 Output 成员，用于收集当前算子处理完的记录，比如在StreamMap / StreamFilter / StreamFlatMap 等
-  在OperatorChain内部，定义了很多的内部类，并且实现了OutPut接口，用来给下游算子传输数据，这里要分几种情况来具体探讨；
    1. ChainingOutput类，用来处理一个OperatorChain中间的算子，通过在ChainingOutput类中保存一个下游算子的引用，来实现直接把数据传输给下游算子，
    2. BroadcastingOutputCollector类，原理和第一个一样，但ExecutionConfig中默认会禁止对象重用，所以会把一条数据复制一遍，再传给下游算子
    3. BroadcastingOutputCollector类，主要用在当前算子有多个下游算子的情况下，接收到一条数据，会发给下游所有的算子
    4. DirectedOutput类，主要是在 split/select 的情况下使用
    5. RecordWriterOutput类，主要用来处理位于 OperatorChain 末尾的算子，这种算子需要把数据写入对应的ResultPartition，核心在recordWriter.emit()，这里会和ResultPartition(数据输出)的源码联系在一起，具体可以到对应的源码文件看解析
        
任务从开始执行到最终调度的逻辑：
集群启动的流程这里就忽略了，最上面已经介绍过了，这里从 ExecutionGraph.scheduleForExecution()开始，首先流作业的调度模式是scheduleEager() 模式，需要预先为所有task分配slot，
具体的过程最终还是通过具体的 ExecutionVertex 的 Execution去申请slot(通过Scheduler类申请，上面已经介绍过了)，然后等待所有的Execution的资源分配完毕，最后对每个Execution调用 execution.deploy() 方法；
这个方法的核心主要是生成一个 TaskDeploymentDescription，然后通过rpc调用taskManagerGateway的 submitTask() 方法，来部署任务；下面的逻辑就是 tm的 submitTask() 具体执行任务；

在submitTask方法中，当TaskDeploymentDescription被提交到TaskExecutor后，TaskExecutor会据此创建一个Task对象，并在构造函数中完成一些初始化操作，然后直接启动这个Task(就是一个Runnable，一个线程)，所以直接看Task的Run()方法逻辑即可；
总结一下，先确定状态，然后创建一个用户加载用户代码的类加载器，然后调用network.registerTask(this)方法，向网络栈中注册 Task,为 ResultPartition 和 InputGate 分配缓冲池，和之前分析的 **task之间的数据传输** 的源码解析又联系在了一起，
然后创建一个执行环境Envrionment(非常重要)，这个Environment中封装着这个task的所有信息，最核心的是StreamConfig，这个配置类中封装着一个Task具体要执行的信息(用户代码operator)，
然后调用loadAndInstantiateInvokable()方法(重要，可以看一下代码备注)，在生成StreamGraph的时候，每一个StreamNode在添加的时候都会有一个jobVertexClass属性，这个属性的值就是StreamTask的子类(SourceStreamTask、OneInputStreamTask、TwoInputStreamTask 等)的类名；
这个方法主要做的事就是反射生成 AbstractInvokable(StreamTask的父类) 实例(调用的是带Envrionment参数的构造器，所以把这个Envrionment参数传进去了)，
最终调用invokable.invoke()方法，开始执行这个具体的StreamTask，然后调用到StreamTask的invoke()方法，这里就是具体的执行逻辑了，
这里的重要点是创建OperatorChain这个链(构造器)，这个chain中保存着这个Task所有要执行的operator，创建OperatorChain的时候，就通过一个递归函数，创建了chain中所有operator的OutPut(具体的过程有些复杂，看代码注释吧)
需要额外注意的就是 headOperator.setup(....) 方法 和 chainedOperator.setup(....)方法，这个方法会为这个operator的container(StreamTask)和config(StreamConfig)和output(Output)赋值；
在StreamTask的invoke()方法中，真正执行用户代码，调用的是run()方法，模板方法，交给具体的子类去实现；
拿SourceStreamTask为例，看看具体实现的init()方法 和 run() 方法，就知道了，这里的问题是userFunction是什么时候传进去的，不要迷糊，是生成StreamGraph的时候，直接把用户代码封装进去的，这里tm执行是直接把operator反序列化出来的，userFunction序列化之前就赋好值了；
最终就是调用 userFunction.run() 方法； 
整个流程结束；

三层封装：StreamTransformation -> operator -> userFunction，最终执行具体的StreamTask，把反序列化出来的operator传进StreamTask中，最终还是执行operator中的userFunction


###  state的管理

##### Keyed State 和Operator State：
Flink中的状态分为两类，Keyed State和Operator State。Keyed State是和具体的Key相绑定的，只能在KeyedStream上的函数和算子中使用。 
Opeartor State 则是和 Operator 的一个特定的并行实例相绑定的，例如 Kafka Connector 中，每一个并行的Kafka Consumer都在 Operator State 中维护当前 Consumer 订阅的 partiton 和 offset。
由于 Flink 中的 keyBy 操作保证了每一个键相关联的所有消息都会送给下游算子的同一个并行实例处理，因此 Keyed State 也可以看作是 Operator State 的一种分区(partitioned)形式，每一个 key 都关联一个状态分区(state-partition)。

##### Managed State 和 Raw State：
从另一个角度来看，无论Operator State还是Keyed State，都有两种形式，Managed State和Raw State。Managed State的数据结构由Flink进行托管，而Raw State的数据结构对Flink是透明的。 
Flink的建议是尽量使用Managed State, 这样Flink可以在并行度改变等情况下重新分布状态，并且可以更好地进行内存管理。

##### 使用方法：
1. CheckpointedFunction 接口，既可以管理Operator State，也可以管理Keyed State，主要有两个方法，snapshotState()在创建检查点的时候调用，initializeState()在初始化/状态恢复时被调用；
2. RuntimeContext，对于Keyed State，通常都是通过RuntimeContext实例来获取
3. ListCheckpointed 接口，只能管理operator state 中的 list-state；

##### StateBackend：
StateBackend 定义了状态是如何存储的，不同的 State Backend 会采用不同的方式来存储状态，目前 Flink 提供了三种不同形式的存储后端，分别是
1. MemoryStateBackend：将工作状态存储在TaskManager的内存中，将检查点存储在JobManager的内存中；
2. FsStateBackend：将工作状态存储在TaskManager的内存中，将检查点存储在文件系统中（通常是分布式文件系统）
3. RocksDBStateBackend：状态存储在RocksDB中，将检查点存储在文件系统中（类似FsStateBackend）；这里不太准确，对于operator state来说，即使是使用这种backend，状态也是存储在tm内存中的；

StateBackend还负责创建OperatorStateBackend和AbstractKeyedStateBackend,分别负责存储Operator State和Keyed State，以及在需要的时候生成对应的Checkpoint。
所以，**实际上StateBackend可以看作是一个Factory，由它创建的具体的 OperatorStateBackend 和 AbstractKeyedStateBackend 才负责实际的状态存储和检查点生成的工作**

StateBackend 的另一个主要作用是和检查点相关，负责为作业创建检查点的存储（检查点写入）以及根据一个检查点的 pointer 获得检查点的存储位置(检查点读取)。

##### 状态的注册与获取
前面介绍如何使用状态的时候提到，通过CheckpointedFunction接口既可以获取Operator State，也可以获取Keyed State，这两类状态分别通过OperatorStateStore和KeyedStateStore这两个接口作为桥梁来进行管理。
比较详细的类图和底层实现画在纸上了，到时候再看吧；

##### 注意点
对于operator state来说，无论使用哪种backend，工作状态都是存储在tm内存中的；
对于keyed state来说，MemoryStateBackend和FsStateBackend也是将状态存储在tm内存中，RocksDBStateBackend会将状态存储在rocksdb中；



### checkpoint

##### 概述
Flink 分布式快照的核心在与stream barrier，barrier是一种特殊的标记消息，会和正常的消息记录一起在数据流中向前流动。Checkpoint Coordinator在需要触发检查点的时候要求数据源向数据流中注入barrie，
barrier和正常的数据流中的消息一起向前流动，相当于将数据流中的消息切分到了不同的检查点中。当一个operator从它所有的input channel中都收到了barrier，则会触发当前operator的快照操作，
并向其下游channel中发射barrier。当所有的sink都反馈完成了快照之后，Checkpoint Coordinator认为检查点创建完毕。

##### 整体流程梳理
CheckpointCoordinator是checkpoint过程的协调者，它主要负责
1. 发起 checkpoint 触发的消息，并接收不同 task 对 checkpoint 的响应信息（Ack）
2. 维护 Ack 中附带的状态句柄（state-handle）的全局视图

在生成ExecutionGraph的最后(ExecutionGraphBuilder的buildGraph()方法末尾)，会调用executionGraph.enableCheckpointing，在这个方法里，会创建CheckpointCoordinator，然后注册一个作业状态的
监听，当作业状态变为running的时候，会回调coordinator.startCheckpointScheduler()方法，开始checkpoint的流程，其实就是一个scheduler线程池，以用户配置的间隔来执行ScheduledTrigger这个Runnable，
这个Runnable内部就是调用triggerCheckpoint()方法，所以也就是固定频率调用这个方法，简单总结一下这个方法做的事；
1. 检查是否可以触发 checkpoint，包括是否需要强制进行 checkpoint，当前正在排队的并发 checkpoint 的数目是否超过阈值，距离上一次成功 checkpoint 的间隔时间是否过小等，如果这些条件不满足，则当前检查点的触发请求不会执行
2. 检查是否所有需要触发 checkpoint 的 Execution 都是 RUNNING 状态
3. 生成此次 checkpoint 的 checkpointID（id 是严格自增的），并初始化 CheckpointStorageLocation，CheckpointStorageLocation 是此次 checkpoint 存储位置的抽象，通过 CheckpointStorage.initializeLocationForCheckpoint() 创建
   （CheckpointStorage 目前有两个具体实现，分别为 FsCheckpointStorage 和 MemoryBackendCheckpointStorage），CheckpointStorage 则是从 StateBackend 中创建；
4. 生成 PendingCheckpoint，这表示一个处于中间状态的 checkpoint，并保存在 checkpointId -> PendingCheckpoint 这样的映射关系中
5. 注册一个调度任务，在 checkpoint 超时后取消此次 checkpoint，并重新触发一次新的 checkpoint
6. 调用Execution.triggerCheckpoint()方法向所有需要trigger的task发起checkpoint请求(最终内部还是调用taskManagerGateway.triggerCheckpoint()这个rpc方法)

**注意点：上述的第6步，调用taskManagerGateway.triggerCheckpoint()方法让具体的task触发checkpoint时，这里的task指的仅仅是SourceTask**

接下来的过程跳到TaskExecutor的triggerCheckpoint(...)方法，找到对应的Task，调用task的triggerCheckpointBarrier(...)方法，Task类又委托给具体的StreamTask去(异步)执行: invokable.triggerCheckpoint(...)方法；
方法调用进入到StreamTask.triggerCheckpoint(...)方法，然后调用到核心方法: performCheckpoint()

继续往下分析之前，需要了解一下Task处理输入数据的流程，之前我们说一个Task 通过循环调用 InputGate.getNextBufferOrEvent 方法获取输入数据，其实是不太准确的，其实是通过 StreamInputProcessor创建的CheckpointBarrierHandler处理的输入数据，
CheckpointBarrierHandler是对InputGate的又一层封装，也就是 StreamInputProcessor -> CheckpointBarrierHandler -> InputGate -> InputChannel 四层，
CheckpointBarrierHandler有两种具体实现:(BarrierTracker 对应 AT_LEAST_ONCE，BarrierBuffer 对应 EXACTLY_ONCE，不同点在下面介绍)，
Task在处理输入数据的时候，调用StreamInputProcessor的processInput()方法，内部会通过CheckpointBarrierHandler的getNextNonBlocked()方法获取输入数据，需要注意的是这个方法不会返回barrier，只返回用户数据，barrier在这个方法内部已经处理了；
怎么处理的呢？最终还是调用的StreamTask(toNotifyOnCheckpoint)的triggerCheckpointOnBarrier()方法，而这个方法最终还是调用的 StreamTask的performCheckpoint()方法
所以结论就是不管是SourceTask还是执行图内部的Task，最终执行checkpoint的方法都是StreamTask的performCheckpoint()方法

所以接下来的重点就是performCheckpoint()方法(主要的作用是把checkpoint存储在分布式文件系统或者jobManager内存)，这个方法主要做了如下几步
1. 先调用operatorChain.broadcastCheckpointBarrier(...)方法，向下游所有Task(物理外部边)发送barrier注意，此时上游所有inputChannel的barrier都已经到了；
2. 调用checkpointState()方法，进行状态快照，这个方法是比较核心的方法，需要仔细分析一下

在checkpointState()方法中，主要有两步
1. 创建一个CheckpointStorageLocation对象，它是对检查点状态存储位置的一个抽象，它能够提供获取检查点输出流的方法，通过输出流将状态和元数据写入到存储系统中。输出流关闭时可以获得状态句柄（StateHandle），后面可以使用句柄重新读取写入的状态。
而它是通过CheckpointStorage创建的，CheckpointStorage 是对状态存储系统的抽象，它有两个不同的实现，分别是 MemoryBackendCheckpointStorage 和 FsCheckpointStorage。CheckpointStorage则是从statebackend中生成的；
MemoryBackendCheckpointStorage 会将所有算子的检查点状态存储在 JobManager 的内存中，通常不适合在生产环境中使用；
而 FsCheckpointStorage 则会把所有算子的检查点状态持久化存储在文件系统中。
2. 将存储检查点的过程封装为CheckpointingOperation，然后调用executeCheckpointing()方法开始进行检查点存储操作；

在CheckpointingOperation的executeCheckpointing()方法中，也主要分为两步
1. 同步执行的部分，对当前Task的所有的operator调用checkpointStreamOperator()方法，返回一个future(OperatorSnapshotFutures)，代表这个operator存储分布式快照的结果，
    这个方法最终会调用到 DefaultOperatorStateBackend的snapshot(...)方法，细节过于复杂，这里不再介绍，直接看源码吧
2. 异步执行的部分，主要是异步执行AsyncCheckpointRunnable这个Runnable，
    主要就是异步完成上面所有算子的OperatorSnapshotFutures(如果之前的模式是同步的，那这里本身就是已经完成的)，然后向CheckpointCoordinator汇报ACK，此次checkpoint成功(rpc到jobMaster) 所以接下来的代码又跳回到CheckpointCoordinator的receiveAcknowledgeMessage(...)方法
    
在CheckpointCoordinator的receiveAcknowledgeMessage(...)方法中(简单点说就是协调者接到了具体的Task返回的checkpoint进行结果)，主要有如下几步
1. 根据 Ack 的 checkpointID 从 Map<Long, PendingCheckpoint> pendingCheckpoints 中查找对应的 PendingCheckpoint
2. 若存在对应的 PendingCheckpoint
    2.1 这个PendingCheckpoint没有被丢弃，调用 PendingCheckpoint.acknowledgeTask 方法处理 Ack(内部维护两个容器，一个是已经收到ack的Task，一个是未收到ack的Task)，根据处理结果的不同：
        2.1.1 SUCCESS：判断是否已经接受了所有需要响应的Ack(未收到ack的Task的容器为空了)，如果是，则调用 completePendingCheckpoint(...)方法 完成此次 checkpoint
        2.1.2 DUPLICATE：Ack 消息重复接收，直接忽略
        2.1.3 UNKNOWN：未知的 Ack 消息，清理上报的 Ack 中携带的状态句柄
        2.1.4 DISCARD：Checkpoint 已经被 discard，清理上报的 Ack 中携带的状态句柄
    2.2 这个 PendingCheckpoint 已经被丢弃，抛出异常
3.若不存在对应的PendingCheckpoint，则清理上报的Ack中携带的状态句柄；

接下来看completePendingCheckpoint(...)这个方法
1. 调用 PendingCheckpoint.finalizeCheckpoint() 将 PendingCheckpoint 转化为 CompletedCheckpoint
    1.1 获取 CheckpointMetadataOutputStream，将所有的状态句柄信息通过 CheckpointMetadataOutputStream 写入到存储系统中
	1.2 创建一个 CompletedCheckpoint 对象
2. 将 CompletedCheckpoint 保存到 CompletedCheckpointStore 中
    2.1 CompletedCheckpointStore 有两种实现，分别为 StandaloneCompletedCheckpointStore 和 ZooKeeperCompletedCheckpointStore
    2.2 StandaloneCompletedCheckpointStore 简单地将 CompletedCheckpointStore 存放在一个数组中
    2.3 ZooKeeperCompletedCheckpointStore 提供高可用实现：先将 CompletedCheckpointStore 写入到 RetrievableStateStorageHelper 中（通常是文件系统），然后将文件句柄存在 ZK 中
    2.4 保存的 CompletedCheckpointStore 数量是有限的，会删除旧的快照
3. 移除被越过的 PendingCheckpoint，因为 CheckpointID 是递增的，那么所有比当前完成的 CheckpointID 小的 PendingCheckpoint 都可以被丢弃了
4. 依次调用 Execution.notifyCheckpointComplete() 通知所有的 Task 当前 Checkpoint 已经完成
    4.1 通过 RPC 调用 TaskExecutor.confirmCheckpoint() 告知对应的 Task

然后流程又跳到 TaskExecutor.confirmCheckpoint(),


##### at_least_one 和 exactly_once 的区别
注意这两个模式都是在所有的inputChannel都收到barrier的时候，才会通知StreamTask触发checkpoint，不同点是at_least_one下不会阻塞用户流数据，而exactly_once在收到所有inputChannel的数据之前，barrier早到的内些channel的数据也不能向下游发送，
必须先缓存起来，必须等上游所有channel的barrier都到之后，才会通知触发checkpoint；
还有一点是checkpoint动作都是StreamTask做的，CheckpointBarrierHandler只是通知StreamTask，什么时候可以做；


##### 本地状态存储
所谓本地状态存储，即在存储检查点快照时，在 Task 所在的 TaskManager 本地文件系统中存储一份副本，这样在进行状态恢复时可以优先从本地状态进行恢复，从而减少网络数据传输的开销。本地状态存储仅针对 keyed state;


### 时间、定时器和窗口

##### Trigger
触发器（Trigger）提供了一种灵活的机制来决定窗口的计算结果在什么时候对外输出。理论上来说，只有两种类型的触发器，大部分的应用都是选择其一或组合使用：
1. Repeated update triggers：重复更新窗口的计算结果，更新可以是由新消息到达时触发，也可以是每隔一段时间（如1分钟）进行触发
2. Completeness triggers：在窗口结束时进行触发，这是更符合直觉的使用方法，也和批处理模式的计算结果相吻合。但是需要一种机制来衡量一个窗口的所有消息都已经被正确地处理了。

##### watermark
是事件时间域中衡量输入完成进度的一种时间概念；
生成一个消息流的 event time 和 watermark 的两种方式
1. 在数据源中直接生成 (SourceFunction函数中)
在 SourceFunction 中，可以通过 SourceContext 接口提供的 SourceContext.collectWithTimestamp(T element, long timestamp) 提交带有时间戳的消息，
通过 SourceContext.emitWatermark(Watermark mark) 提交 watermark。
SourceContext 有几种不同的实现，根据时间属性的设置，会自动选择不同的 SourceContext。
    1. TimeCharacteristic#ProcessingTime，那么 NonTimestampContext 会忽略掉时间戳和watermark；
    2. TimeCharacteristic#EventTime，那么通过 ManualWatermarkContext 提交的StreamRecord就会包含时间戳，watermark也会正常提交
    3. TimeCharacteristic#IngestionTime，AutomaticWatermarkContext 会使用系统当前时间作为StreamRecord的时间戳，并定期提交watermark，从而实现IngestionTime的效果

2. 通过 Timestamp Assigners / Watermark Generators   (一般是从消息中提取出时间字段)
通过 DataStream.assignTimestampsAndWatermarks(AssignerWithPeriodicWatermarks) 
和   DataStream.assignTimestampsAndWatermarks(AssignerWithPunctuatedWatermarks) 
方法来自定义提取 timstamp 和生成 watermark 的逻辑。
    1. AssignerWithPeriodicWatermarks 会定期生成watermark信息，会生成一个TimestampsAndPeriodicWatermarksOperator算子，会注册定时器，定期提交watermark到下游
    2. AssignerWithPunctuatedWatermarks 一般依赖于数据流中的特殊元素来生成watermark，会生成一个TimestampsAndPunctuatedWatermarksOperator算子，会针对每个元素判断是否需要提交watermark：

##### 窗口

1. Window
窗口在Flink内部就是使用抽象类Window来表示，每一个窗口都有一个绑定的最大timestamp，一旦时间超过这个值表明窗口结束了。
Window有两个具体实现类，分别为TimeWindow和GlobalWindow：TimeWindow就是时间窗口，每一个时间窗口都有开始时间和结束时间，可以对时间窗口进行合并操作（主要是在 Session Window 中）；
GlobalWindow 是一个全局窗口，所有数据都属于该窗口，其最大timestamp是Long.MAX_VALUE，使用单例模式。

2. WindowAssigner
WindowAssigner确定每一条消息属于哪些窗口，一条消息可能属于多个窗口（如在滑动窗口中，窗口之间可能有重叠）；
MergingWindowAssigner 是WindowAssigner的抽象子类，主要是提供了对时间窗口的合并功能。窗口合并的逻辑在TimeWindow提供的工具方法 mergeWindows(Collection<TimeWindow> windows, MergingWindowAssigner.MergeCallback<TimeWindow> c) 中，会对所有窗口按开始时间排序，存在重叠的窗口就可以进行合并。

根据窗口类型和时间属性的不同，有不同的WindowAssigner的具体实现，如TumblingEventTimeWindows, TumblingProcessingTimeWindows, SlidingEventTimeWindows, SlidingProcessingTimeWindows, EventTimeSessionWindows, ProcessingTimeSessionWindows, DynamicEventTimeSessionWindows, DynamicProcessingTimeSessionWindows, 以及GlobalWindows。具体的实现逻辑这里就不赘述了。

3. Trigger
Trigger用来确定一个窗口是否应该触发结果的计算，Trigger提供了一系列的回调函数，根据回调函数返回的结果来决定是否应该触发窗口的计算。
Flink 提供了一些内置的Trigger实现，这些Trigger内部往往配合timer定时器进行使用，例如 EventTimeTrigger 是所有事件时间窗口的默认触发器，ProcessingTimeTrigger 是所有处理时间窗口的默认触发器，ContinuousEventTimeTrigger 和 ContinuousProcessingTimeTrigger 定期进行触发，CountTrigger 按照窗口内元素个数进行触发，DeltaTrigger 按照 DeltaFunction 进行触发，NeverTrigger 主要在全局窗口中使用，永远不会触发。

4. ProcessWindowFunction 和  WindowFunction 的区别
他们的效果在某些场景下是一致的，但 ProcessWindowFunction 能够提供更多的窗口上下文信息，并且在之后的版本中可能会移除 WindowFunction 接口：

5. WindowOperator
Window 操作的主要处理逻辑在WindowOperator中。其中有几个比较核心的对象
WindowAssigner
Trigger
StateDescriptor    是窗口状态的描述符，窗口的状态必须是 AppendingState 的子类
InternalWindowFunction   是窗口的计算函数  (ProcessWindowFunction 和 WindowFunction 会被包装成 InternalWindowFunction 的子类): 

6. Window 使用的状态
ListState

7. 窗口的处理逻辑
    1. 通过 WindowAssigner 确定消息所在的窗口（可能属于多个窗口）
    2. 将消息加入到对应窗口的状态中
    3. 根据 Trigger.onElement 确定是否应该触发窗口结果的计算，如果使用 InternalWindowFunction 对窗口进行处理
    4. 注册一个定时器，在窗口结束时清理窗口状态
    5. 如果消息太晚到达，提交到 side output 中

8. 增量窗口聚合
在使用 ProcessWindowFunction 来对窗口进行操作的一个重要缺陷是，需要把整个窗口内的所有消息全部缓存在 ListState 中，这无疑会导致性能问题。
如果窗口的计算逻辑支持增量聚合操作，那么可以使用 ReduceFunction, AggregateFunction 或 FoldFunction 进行增量窗口聚合计算，这可以在很大程度上解决 ProcessWindowFunction 的性能问题。

如果使用了增量聚合函数，那么窗口的状态就不再是以 ListState 的形式保存窗口中的所有元素，而是 AggregatingState。
这样，每当窗口中新消息到达时，在将消息添加到状态中的同时就会触发聚合函数的计算，这样在状态中就只需要保存聚合后的状态即可。

在直接使用AggregateFunction的情况下，用户代码中无法访问窗口的上下文信息。为了解决这个问题，
可以将增量聚合函数和 ProcessWindowFunction 结合在一起使用，这样在提交窗口计算结果时也可以访问到窗口的上下文信息：


9. Evictor
Flink 的窗口操作还提供了一个可选的evitor，允许在调用InternalWindowFunction计算窗口结果之前或之后移除窗口中的元素。
在这种情况下，就不能对窗口进行增量聚合操作了，窗口内的所有元素必须保存在 ListState 中，因而对性能会有一定影响。
举例：CountEvictor


### 知识点总结

1. Flink分区策略 (最上层是ChannelSelector接口，源码解析对应到了数据输出RecordWriter中)
    1. GlobalPartitioner 数据会被分发到下游算子的第一个实例中进行处理。
    2. ShufflePartitioner 数据会被随机分发到下游算子的每一个实例中进行处理。
    3. RebalancePartitioner 数据会被循环发送到下游的每一个实例中进行处理。
    4. RescalePartitioner 这种分区器会根据上下游算子的并行度，循环的方式输出到下游算子的每个实例。这里有点难以理解，假设上游并行度为2，编号为A和B。下游并行度为4，编号为1，2，3，4。那么A则把数据循环发送给1和2，B则把数据循环发送给3和4。假设上游并行度为4，编号为A，B，C，D。下游并行度为2，编号为1，2。那么A和B则把数据发送给1，C和D则把数据发送给2。
    5. BroadcastPartitioner 广播分区会将上游数据输出到下游算子的每个实例中。适合于大数据集和小数据集做Join的场景。
    6. KeyGroupStreamPartitioner 是一个Hash分区器。会将数据按Key的Hash值输出到下游算子实例中。
    7. CustomPartitionerWrapper 用户自定义分区器。需要用户自己实现Partitioner接口，来定义自己的分区逻辑
    9. **ForwardPartitioner** 直接把元素转发给下游，在用户没有指定partitoner，且上下游算子的并行度一致的时候，数据的分区策略会被自动指定为 ForwardPartitioner

2. Slot和parallelism有什么区别？
    1. slot是指taskmanager的并发执行能力，假设我们将taskmanager.numberOfTaskSlots配置为3那么每一个tm中分配3个TaskSlot, 3个 taskmanager 一共有9个TaskSlot。
    2. parallelism是指tm实际使用的并发能力。假设我们把parallelism.default设置为1，那么9个TaskSlot只能用1个，有8个空闲。
    **注意这种说的是在没设置slotSharingGroup的情况下**

3. OperatorChain 的 chain 策略
    1. ALWAYS: 可以与上下游链接，map、flatmap、filter等默认是ALWAYS，也就是说每个算子都有自己的chain策略
    2. HEAD：只能与下游链接，不能与上游链接，Source默认是HEAD,
    
4. 两个operator可以chain在一起的条件
    1. 上下游的并行度一致
    2. 下游节点的入度为1 （也就是说下游节点没有来自其他节点的输入）
    3. 上下游节点都在同一个slot group中（下面会解释 slot group）
    4. 下游节点的chain策略为ALWAYS（可以与上下游链接，map、flatmap、filter等默认是ALWAYS）
    5. 上游节点的chain策略为ALWAYS或HEAD（只能与下游链接，不能与上游链接，Source默认是HEAD）
    6. 两个节点间数据分区方式是 forward
    7. 用户没有禁用 chain

5. slot共享(默认情况下，Flink允许subtasks共享slot，条件是它们都来自同一个Job的不同task的subtask。结果可能一个slot持有该job的整个pipeline)
    1. Flink 集群所需的task slots数与job中最高的并行度一致。也就是说我们不需要再去计算一个程序总共会起多少个slot了。
    2. 更容易获得更充分的资源利用。如果没有slot共享，那么非密集型操作source/flatmap就会占用同密集型操作 keyAggregation/sink 一样多的资源。如果有slot共享，将keyAggregation/sink的2个并行度增加到6个，能充分利用slot资源，同时保证每个TaskManager能平均分配到重的subtasks。

6. 重启策略
    1. 固定延迟重启策略（Fixed Delay Restart Strategy）
    2. 故障率重启策略（Failure Rate Restart Strategy）
    3. 没有重启策略（No Restart Strategy）
    4. Fallback重启策略（Fallback Restart Strategy）

7. 广播变量
    1. 广播出去的变量存在于每个节点的内存中，所以这个数据集不能太大。因为广播出去的数据，会常驻内存，除非程序执行结束
    2. 广播变量在初始化广播出去以后不支持修改，这样才能保证每个节点的数据都是一致的
    3. 实现的原理其实就和上面提到的分区策略中的 BroadcastPartitioner 相关
    
8. window出现数据倾斜的解决办法
    1. 在数据进入窗口前做预聚合
    2. 重新设计窗口聚合的key

9. Flink中在使用聚合函数GroupBy、Distinct、KeyBy等函数时出现数据热点该如何解决
    1. 在业务上规避这类问题，例如一个假设订单场景，北京和上海两个城市订单量增长几十倍，其余城市的数据量不变。这时候我们在进行聚合的时候，北京和上海就会出现数据堆积，我们可以单独处理北京和上海的数据。
    2. Key的设计上，把热key进行拆分，比如上个例子中的北京和上海，可以把北京和上海按照地区进行拆分聚合。
    3. 参数设置，MiniBatch，微批处理


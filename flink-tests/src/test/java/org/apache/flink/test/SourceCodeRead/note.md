
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








    






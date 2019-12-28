
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

大致的过程是这样的，rm先启动，然后tm启动，tm启动时，会和rm建立连接，然后tm会把当前所有slot的情况汇报给rm，在之后的心跳过程中，
也会把所有slot情况汇报给rm，也就是说rm拥有所有slot的status；

程序启动时，jobMaster接收jobGraph，转化为ExecutionGraph，然后开始执行，然后会向rm申请资源，rm包含着所有SlotStatus，所以会向
具体某个tm发起requestSlot rpc调用，传一个jobId，然后tm会rpc调用 JobMaster.offerSlots(); 把slot分配给具体的jm；

总结一下，tm对slot的管理是比较简单的，主要通过 TaskSlotTable 来维护所有的slot，rm向tm请求资源，然后tm向jm分配资源；


#### rm 中 slot的管理
rm需要对所有的tm的slot进行管理，jobMaster都是向rm申请资源的；然后rm把请求 "转发" 给tm；tm offer slot 给jm
rm主要通过 SlotManager 来对 所有的slot进行管理；SlotManager 主要维护的就是所有slot的状态 和 所有 pending slot request 的状态；
SlotManager start时，会启动两个线程，一个检测tm是否长期idle状态，是则释放这个tm。另一个检测request是否长时间不被满足，是则取消这个request

SlotManager的主要功能有：
1. 注册slot，从registerTaskManager开始，注册一个tm的所有slot，每次注册一个slot，都会检查是否满足某个request，是则触发分配slot逻辑
2. 请求slot，从registerSlotRequest开始，jm发起，如果有free且满足条件的slot，直接分配，否则进等待队列
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

#### jm 中 slot的管理


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
    






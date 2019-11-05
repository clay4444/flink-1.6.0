
### 大致流程
1. 启rm       -> leader 竞选
2. 启tm，tm汇报slot给rm(刚启动/心跳)         -> 没有leader 竞选
3. 启dispatcher，         -> leader 竞选
4. 开始调用阻塞执行方法，dispatcher#submitJob jobGraph,  (rpc)
5. dispatcher#submitJob:  创建 JobManagerRunner(用来启jobMaster), 然后start启动，       -> leader竞选
6. JobManagerRunner 拿到授权，回调时，启动jobMaster，然后和 rm 建立连接 (需要向rm申请资源)
7. executionGraph.scheduleForExecution()  真正执行；


### 所有涉及到选举服务的组件
1. rm
2. dispatcher
3. jobMaster (JobManagerRunner)


### 资源的管理

#### tm 中 slot的管理

大致的过程是这样的，rm先启动，然后tm启动，tm启动时，会和rm建立连接，然后tm会把当前所有slot的情况汇报给rm，在之后的心跳过程中，
也会把所有slot情况汇报给rm，也就是说rm拥有所有slot的status；

程序启动时，jobMaster接收jobGraph，转化为ExecutionGraph，然后开始执行，然后会向rm申请资源，rm包含着所有SlotStatus，所以会向
具体某个tm发起requestSlot rpc调用，传一个jobId，然后tm会rpc调用 JobMaster.offerSlots(); 把slot分配给具体的jm；

总结一下，tm对slot的管理是比较简单的，主要通过 TaskSlotTable 来维护所有的slot，rm向tm请求资源，然后tm向jm分配资源；


#### rm 中 slot的管理
rm需要对所有的tm的slot进行管理，jobMaster都是向rm申请资源的；然后rm把请求 "转发" 给tm；




#### jm 中 slot的管理

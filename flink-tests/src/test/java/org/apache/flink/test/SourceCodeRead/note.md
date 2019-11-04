
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




#### jm 中 slot的管理

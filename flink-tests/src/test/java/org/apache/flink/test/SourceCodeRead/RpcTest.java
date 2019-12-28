package org.apache.flink.test.SourceCodeRead;

import akka.actor.ActorSystem;
import akka.actor.Terminated;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcService;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * 使用流程就是1）定义协议，提供 RPC 方法的实现；2）获得服务对象的代理对象，调用 RPC 方法。
 *
 * 主要抽象：
 * RpcEndpoint：
 * 		是对 RPC 框架中提供具体服务的实体的抽象，所有提供远程调用方法的组件都需要继承该抽象类。另外，对于同一个 RpcEndpoint 的所有 RPC 调用都会在同一个线程（RpcEndpoint 的“主线程”）中执行，因此无需担心并发执行的线程安全问题。
 * RpcGateway ：
 * 		是用于远程调用的代理接口。 RpcGateway 提供了获取其所代理的 RpcEndpoint 的地址的方法。在实现一个提供 RPC 调用的组件时，通常需要先定一个接口，该接口继承 RpcGateway 并约定好提供的远程调用的方法。
 * RpcService:
 * 		是 RpcEndpoint 的运行时环境， RpcService 提供了启动 RpcEndpoint, 连接到远端 RpcEndpoint 并返回远端 RpcEndpoint 的代理对象等方法。此外， RpcService 还提供了某些异步任务或者周期性调度任务的方法。
 * RpcServer:
 * 		相当于 RpcEndpoint 自身的的代理对象（self gateway)。RpcServer 是 RpcService 在启动了 RpcEndpoint 之后返回的对象，每一个 RpcEndpoint 对象内部都有一个 RpcServer 的成员变量，通过 getSelfGateway 方法就可以获得自身的代理，然后调用该Endpoint 提供的服务。
 * 	    本质上RpcServer就是一个用 Proxy.newProxyInstance 方法创建的，并且实现了指定RpcEndpoint(某个具体子类)接口的代理对象，它是上面内个RpcService创建并返回的； 具体可以看 AkkaRpcService的startServer()方法；
 *
 *
 * 创建具体的RpcEndpoint，创建的过程中 调用RpcService.startServer方法(会先创建一个Actor[里面具体的RpcEndpoint])， 实例化这个RpcEndpoint对应的RpcServer，RpcServer是一个代理对象，以后方法的调用都通过RpcServer来调用(比如start方法、自定义协议的方法等)，具体则是对应的invocationHandler方法，
 * 这个handler创建的时候传进了刚刚创建的actor，
 * 当通过rpc调用自定义协议的方法的时候，先转到invocationHandler，然后handler中给对应的Actor发消息，Actor中使用反射调用RpcEndpoint实际方法，最后返回结果；
 */
public class RpcTest {
	private static final Time TIMEOUT = Time.seconds(10L);
	private static ActorSystem actorSystem = null;
	private static RpcService rpcService = null;

	// 定义通信协议
	public interface HelloGateway extends RpcGateway {
		String hello();
	}

	public interface HiGateway extends RpcGateway {
		String hi();
	}

	// 具体实现
	public static class HelloRpcEndpoint extends RpcEndpoint implements HelloGateway {
		protected HelloRpcEndpoint(RpcService rpcService) {
			super(rpcService);
		}

		@Override
		public CompletableFuture<Void> postStop() {
			return null;
		}

		@Override
		public String hello() {
			return "hello";
		}
	}

	public static class HiRpcEndpoint extends RpcEndpoint implements HiGateway {
		protected HiRpcEndpoint(RpcService rpcService) {
			super(rpcService);  //会调用 AkkaRpcService#startServer 方法来初始化服务，所谓的初始化服务就是返回了一个代理对象rpcServer；
		}

		@Override
		public CompletableFuture<Void> postStop() {
			return null;
		}

		@Override
		public String hi() {
			return "hi";
		}
	}

	@BeforeClass
	public static void setup() {
		actorSystem = AkkaUtils.createDefaultActorSystem();
		// 创建RpcService，基于 AKKA 的实现，AkkaRpcService 会启动 Akka actor 来接收来自 RpcGateway 的 RPC 调用。
		rpcService = new AkkaRpcService(actorSystem, TIMEOUT);
	}

	@AfterClass
	public static void teardown() throws Exception {

		final CompletableFuture<Void> rpcTerminationFuture = rpcService.stopService();
		final CompletableFuture<Terminated> actorSystemTerminationFuture = FutureUtils.toJava(actorSystem.terminate());

		FutureUtils
			.waitForAll(Arrays.asList(rpcTerminationFuture, actorSystemTerminationFuture))
			.get(TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS);
	}

	/**
	 * 总结一下流程就是分为以下几步
	 * 1. 创建具体的RpcEndpoint，创建的时候传入了rpcService，它是 RpcEndpoint 的运行时环境，创建的时候，在构造器中就调用了super(rpcService)方法[必须存在]，在这个父类方法中，rpcService.startServer(this) 这段代码就创建了一个代理对象，
	 * 	  这个代理对象就被称为RpcServer，它就是用Proxy.newProxyInstance 方法创建的，并且实现了指定RpcEndpoint(某个具体子类)接口的代理对象，
	 * 2. 调用具体的RpcEndpoint的start()方法，实际上就会调用代理对象RpcServer的start方法，最终调用 InvocationHandler 的start方法
	 *
	 * 3. 注意：真正的分布式环境下，服务端创建并返回的RpcServer肯定是不能直接被客户端用的，也就是没用的，这个RpcServer只能本地测试使用，真正起作用的是创建的具体的 Actor，比如AkkaRpcActor(一个endPoint对应一个Actor)，它用来接收并处理用户请求；
	 */
	@Test
	public void test() throws Exception {

		//提供具体服务的两个对象；
		HelloRpcEndpoint helloEndpoint = new HelloRpcEndpoint(rpcService);
		HiRpcEndpoint hiEndpoint = new HiRpcEndpoint(rpcService);

		helloEndpoint.start();  //这个方法实际上会调用代理对象RpcServer的start方法，最终调用 InvocationHandler 的start方法，所以启动 RpcEndpoint 实际上就是向当前 endpoint 绑定的 Actor 发送一条 START 消息，通知服务启动。

		//------------------------------------------------------------------------------------------------------------------------------------
		// 		这种是在服务端本地同一个jvm中的方法的调用方式，需要注意的就是此时构造的RpcServer代理对象和invocationHandler，只供本地同一个jvm中进行方法调用；
		//------------------------------------------------------------------------------------------------------------------------------------
		//获取 endpoint 的 self gateway
		HelloGateway helloGateway = helloEndpoint.getSelfGateway(HelloGateway.class); // getSelfGateway 返回一个代理对象； 这个代理对象就是 rpcServer 强转来的 ！
		String hello = helloGateway.hello(); //直接调用代理对象的方法，只适用于本地测试的情况；
		//assertEquals("hello", hello);

		System.out.println(hello);

		hiEndpoint.start();  //服务端启动的；

		//------------------------------------------------------------------------------
		// 		这种是在服务端启动之后，客户端去远程进行rpc调用；注意一定要和上面的区分来看；
		//------------------------------------------------------------------------------

		// 通过 endpoint 的地址获得一个远程对象的代理
		//connect方法内部会返回一个 **远程服务端的** 代理对象，但是这个对象只代理 HiGateway 这个接口的方法；
		//具体一点，根据address获取服务端启动的 ActorRef，然后构造一个AkkaInvocationHandler(含有服务端的actor)，然后返回一个Proxy代理对象，之后方法的调用结果通过AkkaInvocationHandler向服务端的actor发消息来获得；

		//@TODO HiRpcEndpoint hiEndpoint = new HiRpcEndpoint(rpcService); hiEndpoint.start(); 应该是在远程启动的，那客户端如何获得这个hiEndpoint，然后调用 hiEndpoint.getAddress() 呢？
		//大概理解了，这里使用hiEndpoint.getAddress()迷惑性太大，这个address地址可以通过LeaderRetrievalService等方法获得，真正的分布式环境下，客户端肯定是无法直接使用hiEndpoint.getAddress()来获取address地址的；

		HiGateway hiGateway = rpcService.connect(hiEndpoint.getAddress(),HiGateway.class).get();
		String hi = hiGateway.hi();
		//assertEquals("hi", hi);

		System.out.println(hi);
	}
}

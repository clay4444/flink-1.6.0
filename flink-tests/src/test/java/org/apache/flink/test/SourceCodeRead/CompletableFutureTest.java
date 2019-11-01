package org.apache.flink.test.SourceCodeRead;

import org.apache.flink.shaded.netty4.io.netty.util.concurrent.CompleteFuture;
import org.junit.Test;
import org.omg.PortableServer.THREAD_POLICY_ID;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class CompletableFutureTest {

	class Student {
		int id;
		String name;

		public Student(int id, String name) {
			this.id = id;
			this.name = name;
		}

		public void setName(String name) {
			this.name = name;
		}

		@Override
		public String toString() {
			return "Student{" +
				"id=" + id +
				", name='" + name + '\'' +
				'}';
		}
	}

	@Test
	public void test1() {

		Student student = new Student(1, "li");
		String[] list = {"a", "b", "c"};

		List<CompletableFuture<String>> resList = new ArrayList<>();

		for (String str : list) {
			resList.add(CompletableFuture.supplyAsync(() -> str).thenApply(e -> e.toUpperCase()));
		}

		CompletableFuture.allOf(resList.toArray(new CompletableFuture[resList.size()]))
			.whenComplete((r, e) -> {
				if (e == null) {
					student.setName("zhu");
				} else {
					throw new RuntimeException(e);
				}
			});

		System.out.println(student);
	}

	@Test
	public void test2() {

		String result = CompletableFuture.supplyAsync(() -> {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			if (true) {
				throw new RuntimeException("exception test!");
			}

			return "Hi Boy";
		}).exceptionally(e -> {
			System.out.println(e.getMessage());
			return "Hello world!";
		}).join();        //这种一个的join之后返回结果
		System.out.println(result);
	}

	//基本使用
	@Test
	public void test3() {
		CompletableFuture<String> completableFuture = new CompletableFuture<>();  //直接new，创建一个 Future

		new Thread(() -> {
			completableFuture.complete(Thread.currentThread().getName());  //完成这个Future
		}).start();

		//doSomethingElse

		try {
			System.out.println(completableFuture.get()); //直接从Future中get
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	//不同于原始的Future，CompletableFuture 可以返回异步线程中的错误信息，
	@Test
	public void test4() {
		CompletableFuture<String> completableFuture = new CompletableFuture<>();

		new Thread(() -> {
			completableFuture.completeExceptionally(new RuntimeException("run error")); //使用 completeExceptionally 抛出一个异常，来完成这个Future；
			completableFuture.complete(Thread.currentThread().getName());
		}).start();

		//doSomethingElse

		try {
			completableFuture.get();  //这里可以获取到异常
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	//上面的方法已经足够简单，但仍然可以用 CompletableFuture 提供的大量工厂方法来 更加简化
	@Test
	public void test5() {

		//使用 supplyAsync 工厂方法，创建(并完成)一个 Future，
		CompletableFuture<String> completableFuture = CompletableFuture.supplyAsync(() -> Thread.currentThread().getName());

		//doSomethingElse

		try {
			completableFuture.get();  //这里可以获取到异常
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	//计算结果完成时的处理
	/**
	 * public CompletableFuture<T> 	whenComplete(BiConsumer<? super T,? super Throwable> action)
	 * public CompletableFuture<T> 	whenCompleteAsync(BiConsumer<? super T,? super Throwable> action)
	 * public CompletableFuture<T> 	whenCompleteAsync(BiConsumer<? super T,? super Throwable> action, Executor executor)
	 * public CompletableFuture<T>  exceptionally(Function<Throwable,? extends T> fn)
	 * 以Async结尾的会在新的线程池中执行，没有以Async结尾的会在之前的CompletableFuture执行的线程中执行
	 */
	@Test
	public void test6() throws Exception{
		CompletableFuture<String> completableFuture = CompletableFuture.supplyAsync(() -> {
			System.out.println(Thread.currentThread().getName());   //ForkJoinPool.commonPool-worker-9
			return "res";
			//throw new RuntimeException("run error");
		});

		//这是个错误的示范
		/*Future<String> future = completableFuture.whenComplete((v,e)->{   //v: res,  e: exception
			System.out.println(Thread.currentThread().getName());   //main
			System.out.println(v);   //res
		}).exceptionally((t) -> {
			System.out.println(t);
			return "exception res";
		});*/

		//正确写法
		//exceptionally方法返回一个新的CompletableFuture，当原始的CompletableFuture抛出异常的时候，就会触发这个CompletableFuture的计算，
		// 调用function计算值，否则如果原始的CompletableFuture正常计算完后，这个新的CompletableFuture也计算完成，它的值和原始的CompletableFuture的计算的值相同。
		// 也就是这个exceptionally方法用来处理异常的情况。
		Future<String> future = completableFuture.exceptionally((t) -> {
			System.out.println(t);    // java.util.concurrent.CompletionException: java.lang.RuntimeException: run error
			return "exception res";
		}).whenComplete((v,e)->{   //v: res,  e: exception
			System.out.println(Thread.currentThread().getName());   //main
			System.out.println(v);   //res
		});

		System.out.println("Main: " + Thread.currentThread().getName());    //main
		System.out.println(future.get());   //res
	}

	//计算结果完成时的转换，转换为另外一种结果
	/**
	 * public <U> CompletableFuture<U> 	thenApply(Function<? super T,? extends U> fn)
	 * public <U> CompletableFuture<U> 	thenApplyAsync(Function<? super T,? extends U> fn)
	 * public <U> CompletableFuture<U> 	thenApplyAsync(Function<? super T,? extends U> fn, Executor executor)
	 */
	@Test
	public void test7() throws Exception{
		CompletableFuture<Integer> completableFuture = CompletableFuture.supplyAsync(() -> 10);

		CompletableFuture<String> future = completableFuture.thenApply((t) -> t+1).thenApply(t -> String.valueOf(t));

		System.out.println(future.get());  //  "11"
	}

	//计算结果完成时的消费，不产生额外的转换，只是消费
	/**
	 * public CompletableFuture<Void> 	thenAccept(Consumer<? super T> action)
	 * public CompletableFuture<Void> 	thenAcceptAsync(Consumer<? super T> action)
	 * public CompletableFuture<Void> 	thenAcceptAsync(Consumer<? super T> action, Executor executor)
	 */
	@Test
	public void test8(){
		CompletableFuture<Integer> completableFuture = CompletableFuture.supplyAsync(() -> 10);
		completableFuture.thenAccept(System.out::println);
	}

	//对计算结果的组合，也就是连接两个 Future
	/**
	 * public <U> CompletableFuture<U> 	thenCompose(Function<? super T,? extends CompletionStage<U>> fn)
	 * public <U> CompletableFuture<U> 	thenComposeAsync(Function<? super T,? extends CompletionStage<U>> fn)
	 * public <U> CompletableFuture<U> 	thenComposeAsync(Function<? super T,? extends CompletionStage<U>> fn, Executor executor)
	 *
	 * Compose可以连接两个CompletableFuture，其内部处理逻辑是当第一个CompletableFuture处理没有完成时会合并成一个CompletableFuture,如果处理完成，
	 * 第二个future会紧接上一个CompletableFuture进行处理。
	 *
	 * public <U,V> CompletableFuture<V> 	thenCombine(CompletionStage<? extends U> other, BiFunction<? super T,? super U,? extends V> fn)
	 * public <U,V> CompletableFuture<V> 	thenCombineAsync(CompletionStage<? extends U> other, BiFunction<? super T,? super U,? extends V> fn)
	 * public <U,V> CompletableFuture<V> 	thenCombineAsync(CompletionStage<? extends U> other, BiFunction<? super T,? super U,? extends V> fn, Executor executor)
	 *
	 * 和Compose的区别是：Compose第二个CompletableFuture是基于第一个的结果来做的（这样说有点类似于thenApply了啊...）
	 * 					Combine是真正的等两个Future都并行计算完成后，再合并两个结果，产生一个新的结果
	 *
	 * 上面介绍了两个future完成的时候应该完成的工作，接下来介绍任意一个future完成时需要执行的工作，方法如下：
	 *
	 * public CompletableFuture<Void> 	acceptEither(CompletionStage<? extends T> other, Consumer<? super T> action)
	 * public CompletableFuture<Void> 	acceptEitherAsync(CompletionStage<? extends T> other, Consumer<? super T> action)
	 * public CompletableFuture<Void> 	acceptEitherAsync(CompletionStage<? extends T> other, Consumer<? super T> action, Executor executor)
	 * public <U> CompletableFuture<U> 	applyToEither(CompletionStage<? extends T> other, Function<? super T,U> fn)
	 * public <U> CompletableFuture<U> 	applyToEitherAsync(CompletionStage<? extends T> other, Function<? super T,U> fn)
	 * public <U> CompletableFuture<U> 	applyToEitherAsync(CompletionStage<? extends T> other, Function<? super T,U> fn, Executor executor)
 	 */
	@Test
	public void test9() throws Exception{
		CompletableFuture<Integer> future = CompletableFuture.supplyAsync(() -> 10);

		future.thenCompose(t -> CompletableFuture.supplyAsync(() -> t+1)).thenAccept(System.out::println);

		//====
		future.thenCombine(CompletableFuture.supplyAsync(() -> 20),(x,y) -> "计算结果: " + Integer.valueOf(x + y)).thenAccept(System.out::println);
	}

	//其他方法
	/**
	 * public static CompletableFuture<Void> 	    allOf(CompletableFuture<?>... cfs)
	 * public static CompletableFuture<Object> 	anyOf(CompletableFuture<?>... cfs)
	 *
	 * allOf方法是当所有的CompletableFuture都执行完后执行计算。
	 * anyOf方法是当任意一个CompletableFuture执行完后就会执行计算，计算的结果相同。
	 */

}

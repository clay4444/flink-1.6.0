package org.apache.flink.test.SourceCodeRead;

import org.apache.flink.shaded.netty4.io.netty.util.concurrent.CompleteFuture;
import org.junit.Test;

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
		CompletableFuture<String> completableFuture = new CompletableFuture<>();

		new Thread(() -> {
			completableFuture.complete(Thread.currentThread().getName());
		}).start();

		//doSomethingElse

		try {
			System.out.println(completableFuture.get());
		}catch (Exception e){
			e.printStackTrace();
		}
	}


}

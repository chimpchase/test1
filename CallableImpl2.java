package com.multithreading;

import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.multithreading.Main.Task;

public class CallableImpl2 implements Main.TaskExecutor {
    private final ExecutorService executorService;
    private final Map<UUID, Object> taskGroupLocks = new ConcurrentHashMap<>();

    public CallableImpl2(int maxConcurrency) {
        this.executorService = Executors.newFixedThreadPool(maxConcurrency);
    }

    @Override
    public <T> Future<T> submitTask(Task<T> task) {
        Object lock = taskGroupLocks.computeIfAbsent(task.taskGroup().groupUUID(), k -> new Object());
        return executorService.submit(() -> {
            synchronized (lock) {
                return task.taskAction().call();
            }
        });
    }

    public static void main(String[] args) {
        CallableImpl2 taskExecutor = new CallableImpl2(4);

        Main.TaskGroup g1 = new Main.TaskGroup(UUID.randomUUID());
        Main.TaskGroup g2 = new Main.TaskGroup(UUID.randomUUID());

        Main.Task<String> task1 = new Main.Task<>(UUID.randomUUID(), g1, Main.TaskType.READ, new CallableZ(1));
        Main.Task<String> task2 = new Main.Task<>(UUID.randomUUID(), g2, Main.TaskType.WRITE, new CallableZ(2));
        Main.Task<String> task3 = new Main.Task<>(UUID.randomUUID(), g2, Main.TaskType.READ, new CallableZ(3));
        Main.Task<String> task4 = new Main.Task<>(UUID.randomUUID(), g1, Main.TaskType.READ, new CallableZ(4));
        Main.Task<String> task5 = new Main.Task<>(UUID.randomUUID(), g2, Main.TaskType.WRITE, new CallableZ(5));
        Main.Task<String> task6 = new Main.Task<>(UUID.randomUUID(), g2, Main.TaskType.READ, new CallableZ(6));


        Future<String> f1 = taskExecutor.submitTask(task1);
        Future<String> f2 = taskExecutor.submitTask(task2);
        Future<String> f3 = taskExecutor.submitTask(task3);
        Future<String> f4 = taskExecutor.submitTask(task4);
        Future<String> f5 = taskExecutor.submitTask(task5);
        Future<String> f6 = taskExecutor.submitTask(task6);

        try {
            System.out.println(f1.get());
            System.out.println(f2.get());
            System.out.println(f3.get());
            System.out.println(f4.get());
            System.out.println(f5.get());
            System.out.println(f6.get());
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

        taskExecutor.executorService.shutdown();
        System.out.println("End");
    }
}

class CallableZ implements Callable{

	int seq;
	public CallableZ(int seqance) {
		// TODO Auto-generated constructor stub
		this.seq = seqance;
	}
	@Override
	public Object call() throws Exception {
		// TODO Auto-generated method stub
		Random rand = new Random();
		int randomNum = rand.nextInt(100)+1;
		String evenOdd = "Odd";
		if(randomNum%2==0) {
			evenOdd = "Even";
		}
		Thread.sleep(1000);
		return "#"+this.seq +". "+ randomNum +" is "+evenOdd ;
	}
	
}
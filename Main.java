package com.dsalgo.opentext2;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

interface TaskExecutor {
    <T> Future<T> submitTask(Task<T> task) throws InterruptedException;
    void shutdown();
}

enum TaskType {
    READ,
    WRITE
}

class Task<T> {
    private final UUID taskUUID;
    private final TaskGroup taskGroup;
    private final TaskType taskType;
    private final Callable<T> taskAction;
    public int taskno;
    public Task(int taskNo, UUID taskUUID, TaskGroup taskGroup, TaskType taskType, Callable<T> taskAction) {
        this.taskno = taskNo;
        this.taskUUID = taskUUID;
        this.taskGroup = taskGroup;
        this.taskType = taskType;
        this.taskAction = taskAction;

    }

    public UUID getTaskUUID() {
        return taskUUID;
    }

    public TaskGroup getTaskGroup() {
        return taskGroup;
    }

    public TaskType getTaskType() {
        return taskType;
    }

    public Callable<T> getTaskAction() {
        return taskAction;
    }
}

class TaskGroup {
    private final UUID groupUUID;

    public TaskGroup(UUID groupUUID) {
        this.groupUUID = groupUUID;
    }

    public UUID getGroupUUID() {
        return groupUUID;
    }
}

class TaskExecutorImpl implements TaskExecutor {
    final ExecutorService executorService;
    private final BlockingQueue<Task<?>> taskQueue;
    private final AtomicInteger currentTaskIndex;
    private final ConcurrentHashMap<TaskGroup, Semaphore> groupSemaphores;

    public TaskExecutorImpl(int maxConcurrency) {
        executorService = Executors.newFixedThreadPool(maxConcurrency);
        taskQueue = new LinkedBlockingQueue<>();
        currentTaskIndex = new AtomicInteger(0);
        groupSemaphores = new ConcurrentHashMap<>();
    }
    @Override
    public <T> Future<T> submitTask(Task<T> task) throws InterruptedException {
        int sequenceNumber = currentTaskIndex.getAndIncrement();
        task = new TaskWithSequenceNumber<>(task.getTaskUUID(), task.getTaskGroup(), task.getTaskType(), task.getTaskAction(), sequenceNumber);

        groupSemaphores.computeIfAbsent(task.getTaskGroup(), k -> new Semaphore(1)).acquire();
        return executeTask(task);
        
    }
    
    public <T> Future<T> executeTask(Task<T> task) throws InterruptedException {        

        CompletableFuture<T> future = new CompletableFuture<>();
        executorService.submit(() -> {
            try {
                T result = task.getTaskAction().call();
                future.complete(result);
            } catch (Exception e) {
                future.completeExceptionally(e);
            } finally {
                groupSemaphores.get(task.getTaskGroup()).release();
            }
        });

        return future;
    }

    @Override
    public void shutdown() {
        executorService.shutdown();
    }

    private static class TaskWithSequenceNumber<T> extends Task<T> {
        private final int sequenceNumber;

        public TaskWithSequenceNumber(UUID taskUUID, TaskGroup taskGroup, TaskType taskType, Callable<T> taskAction, int sequenceNumber) {
            super(sequenceNumber, taskUUID, taskGroup, taskType, taskAction);
            this.sequenceNumber = sequenceNumber;
        }

        @SuppressWarnings("unused")
        public int getSequenceNumber() {
            return sequenceNumber;
        }
    
        
        
        public int compareTo(Task<T> other) {
            if (other instanceof TaskWithSequenceNumber) {
                return Integer.compare(this.sequenceNumber, ((TaskWithSequenceNumber) other).sequenceNumber);
            }
            return 0;
        }
    }
}

public class Main {
    public static void main(String[] args) throws InterruptedException {
        TaskExecutor taskExecutor = new TaskExecutorImpl(5); // Maximum 5 concurrent tasks

        // Create and submit multiple tasks
        for (int i = 0; i < 10; i++) {
            int  j = i;
            int q = i + 100;
            Task<String> task = new Task<>(i,UUID.randomUUID(), new TaskGroup(UUID.randomUUID()), TaskType.READ, () -> {
                Thread.sleep(1000); 
                return "Task" + j +" completed";
            });

            Task<String> task2 = new Task<>(q,UUID.randomUUID(), task.getTaskGroup(), TaskType.READ, () -> {
                Thread.sleep(1000); 
                return "Task" + q +" completed";
            });

            List<Task> tasklst = new ArrayList<Task>();
            tasklst.add(task);
            tasklst.add(task2);
            
            tasklst.forEach(t ->{
                try {
                    Future<String> future = taskExecutor.submitTask(task);
                    try {
                        String result = future.get();
                        System.out.println("Task result: " + result);
                    } catch (InterruptedException | ExecutionException e) {
                        e.printStackTrace();
                    }
                } catch (InterruptedException e) {                    
                    e.printStackTrace();
                }
            });
            
        }
        
        taskExecutor.shutdown();
    }
}

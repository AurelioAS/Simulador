/**
 * 
 */
package com.simulador.service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class RoundRobinExecutorPool {

  private final List<ExecutorService> executors = new ArrayList<>();
    private final AtomicInteger index = new AtomicInteger(0);

  // 🔹 Constructor que crea N ejecutores
  public RoundRobinExecutorPool(String name, int numExecutors) {

    final AtomicInteger count = new AtomicInteger(1);
    for (int i = 0; i < numExecutors; i++) {
      ExecutorService executor = Executors.newFixedThreadPool(1,
          new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
              Thread t = new Thread(r);
              t.setName(name + "-EXECUTOR-" + count.getAndIncrement());
              return t;
            }
          });
      executors.add(executor);
    }
  }

  // 🔹 También mantenemos el constructor con lista si lo necesitas en el futuro
  public RoundRobinExecutorPool(List<ExecutorService> executorList) {
    this.executors.addAll(executorList);
    }

    public void execute(Runnable task) {
        int idx = Math.abs(index.getAndIncrement() % executors.size());
        executors.get(idx).execute(task);
    }

    public void shutdown() {
        executors.forEach(ExecutorService::shutdown);
    }
}
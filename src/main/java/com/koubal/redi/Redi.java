package com.koubal.redi;

import com.koubal.redi.task.ObjectGetTask;
import com.koubal.redi.task.ObjectUpdateTask;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.JedisPool;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Redi {
	private static final int DEFAULT_PORT = 6379;
	private static final int DEFAULT_TIMEOUT = 2000;
	private static final String DEFAULT_PASSWORD = "";
	private static final int DEFAULT_INTERVAL = 100;
	private static final int DEFAULT_THREADS = 8;

	private final Lock lock = new ReentrantLock();
	private final Map<String, RediObject> objects = new ConcurrentHashMap<String, RediObject>();
	private final JedisPool jedisPool;
	private final ScheduledExecutorService objectUpdateExecutor = Executors.newSingleThreadScheduledExecutor();
	private final ExecutorService threadPool;

	public Redi(String host) {
		this(host, DEFAULT_PORT);
	}

	public Redi (String host, int port) {
		this(host, port, DEFAULT_TIMEOUT);
	}

	public Redi (String host, int port, int timeout) {
		this(host, port, timeout, DEFAULT_PASSWORD);
	}

	public Redi (String host, int port, int timeout, String password) {
		this(host, port, timeout, password, DEFAULT_INTERVAL);
	}

	public Redi (String host, int port, int timeout, String password, int interval) {
		this(host, port, timeout, password, interval, DEFAULT_THREADS);
	}

	public Redi (String host, int port, int timeout, String password, int interval, int threads) {
		jedisPool = new JedisPool(new GenericObjectPoolConfig(), host, port, timeout, password);

		if (threads == -1) {
			threadPool = Executors.newCachedThreadPool();
		} else {
			threadPool = Executors.newFixedThreadPool(threads);
		}

		objectUpdateExecutor.scheduleWithFixedDelay(new ObjectUpdateTask(this), 0, interval, TimeUnit.MILLISECONDS);
	}

	public void close() {
		objectUpdateExecutor.shutdownNow();
		threadPool.shutdownNow();

		jedisPool.destroy();
	}

	public void addObject(String name) {
		addObject(name, true);
	}

	public void addObject(String name, boolean async) {
		ObjectGetTask task = new ObjectGetTask(name, this);

		if (async) {
			threadPool.execute(task);
		} else {
			task.run();
		}
	}

	public RediObject getObject(String name) {
		return objects.get(name);
	}

	public void removeObject(String name) {
		objects.remove(name);
	}

	public void lock() {
		lock.lock();
	}

	public void unlock() {
		lock.unlock();
	}

	public Map<String, RediObject> getObjects() {
		return objects;
	}

	public JedisPool getJedisPool() {
		return jedisPool;
	}

	public ExecutorService getThreadPool() {
		return threadPool;
	}
}

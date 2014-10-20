package com.koubal.redi.task;

import com.koubal.redi.Redi;
import com.koubal.redi.RediObject;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.HashMap;

public class ObjectGetTask implements Runnable {
	private final Redi instance;
	private final String name;

	public ObjectGetTask (String name, Redi instance) {
		this.name = name;
		this.instance = instance;
	}

	public void run() {
		JedisPool jedisPool = instance.getJedisPool();
		Jedis jedis = null;

		try {
			jedis = jedisPool.getResource();
			HashMap<String, String> values = (HashMap<String, String>) jedis.hgetAll(name);
			RediObject object = new RediObject(name, values, instance);
			instance.getObjects().put(name, object);
		} catch (JedisConnectionException e) {
			jedisPool.returnBrokenResource(jedis);
			jedis = null;
			instance.setConnected(false);
		} finally {
			if (jedis != null) {
				jedisPool.returnResource(jedis);
				instance.setConnected(true);
			}
		}
	}
}

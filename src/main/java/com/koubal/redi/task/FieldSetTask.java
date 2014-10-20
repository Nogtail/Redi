package com.koubal.redi.task;

import com.koubal.redi.Redi;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class FieldSetTask implements Runnable {
	private Redi instance;
	private String name;
	private String key;
	private String value;

	public FieldSetTask(String name, String key, String value, Redi instance) {
		this.name = name;
		this.key = key;
		this.value = value;
		this.instance = instance;
	}

	public void run() {
		JedisPool jedisPool = instance.getJedisPool();
		Jedis jedis = jedisPool.getResource();

		try {
			jedis.hset(name, key, value);
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

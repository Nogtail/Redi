package com.koubal.redi.task;

import com.koubal.redi.Redi;

public class ObjectUpdateTask implements Runnable {
	private final Redi instance;

	public ObjectUpdateTask(Redi instance) {
		this.instance = instance;
	}

	public void run() {
		for (String key : instance.getObjects().keySet()) {
			ObjectGetTask task = new ObjectGetTask(key, instance);
			instance.getThreadPool().execute(task);
		}
	}
}

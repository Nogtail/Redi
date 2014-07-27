package com.koubal.redi;

import com.koubal.redi.task.FieldSetTask;

import java.util.HashMap;
import java.util.Map;

public class RediObject {
	private final Redi instance;
	private final String name;
	private final Map<String, String> values;

	public RediObject(String name, HashMap<String, String> values, Redi instance) {
		this.name = name;
		this.values = values;
		this.instance = instance;
	}

	public String getValue(String key) {
		return values.get(key);
	}

	public void setValue(String key, String value) {
		setValue(key, value, true);
	}

	public void setValue(String key, String value, boolean async) {
		FieldSetTask task = new FieldSetTask(name, key, value, instance);

		if (async) {
			instance.getThreadPool().execute(task);
		} else {
			task.run();
		}
	}

	public boolean isEmpty() {
		return values.isEmpty();
	}
}

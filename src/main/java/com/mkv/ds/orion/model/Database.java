package com.mkv.ds.orion.model;

import com.google.gson.Gson;

public class Database {

	private final Entity entity = Entity.DATABASE;

	private final String name;

	public Database(String name) {
		super();
		this.name = name;
	}

	public Entity getEntity() {
		return entity;
	}

	public String getName() {
		return name;
	}

	@Override
	public String toString() {
		return new Gson().toJson(this);
	}
}
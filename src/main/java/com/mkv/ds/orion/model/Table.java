package com.mkv.ds.orion.model;

import com.google.gson.Gson;

public class Table {

	private final Entity entity = Entity.TABLE;

	private final String database;

	private final String name;

	public Table(String database, String name) {
		super();
		this.database = database;
		this.name = name;
	}

	public Entity getEntity() {
		return entity;
	}

	public String getDatabase() {
		return database;
	}

	public String getName() {
		return name;
	}

	@Override
	public String toString() {
		return new Gson().toJson(this);
	}
}
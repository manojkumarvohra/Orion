package com.mkv.ds.orion.model;

import com.google.gson.Gson;

public class View {

	private final Entity entity = Entity.VIEW;

	private final String database;

	private final String name;

	public View(String database, String name) {
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
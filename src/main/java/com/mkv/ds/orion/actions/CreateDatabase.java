package com.mkv.ds.orion.actions;

import com.google.gson.Gson;
import com.mkv.ds.orion.model.Database;

public class CreateDatabase implements DatabaseEvent{

	private static final long serialVersionUID = 1L;

	private final Action action = Action.CREATE_DATABASE;

	private final Database target;

	public CreateDatabase(Database target) {
		super();
		this.target = target;
	}

	@Override
	public Action getAction() {
		return action;
	}

	public Database getTarget() {
		return target;
	}

	@Override
	public String toString() {
		return new Gson().toJson(this);
	}
}
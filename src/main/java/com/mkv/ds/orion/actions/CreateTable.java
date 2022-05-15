package com.mkv.ds.orion.actions;

import com.google.gson.Gson;
import com.mkv.ds.orion.model.Table;

public class CreateTable implements DatabaseEvent{

	private static final long serialVersionUID = 1L;

	private final Action action = Action.CREATE_TABLE;

	private final Table target;

	public CreateTable(Table target) {
		super();
		this.target = target;
	}

	@Override
	public Action getAction() {
		return action;
	}

	public Table getTarget() {
		return target;
	}

	@Override
	public String toString() {
		return new Gson().toJson(this);
	}
}
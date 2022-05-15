package com.mkv.ds.orion.actions;

import java.util.List;

import com.google.gson.Gson;
import com.mkv.ds.orion.model.Table;
import com.mkv.ds.orion.model.View;

public class LoadTable implements DatabaseEvent{

	private static final long serialVersionUID = 1L;

	private final Action action = Action.LOAD_TABLE;

	private final Table target;

	private final List<Table> sourceTables;

	private final List<View> sourceViews;

	public LoadTable(Table target, List<Table> sourceTables, List<View> sourceViews) {
		super();
		this.target = target;
		this.sourceTables = sourceTables;
		this.sourceViews = sourceViews;
	}

	@Override
	public Action getAction() {
		return action;
	}

	public Table getTarget() {
		return target;
	}

	public List<Table> getSourceTables() {
		return sourceTables;
	}

	public List<View> getSourceViews() {
		return sourceViews;
	}

	@Override
	public String toString() {
		return new Gson().toJson(this);
	}
}
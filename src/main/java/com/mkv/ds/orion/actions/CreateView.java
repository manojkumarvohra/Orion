package com.mkv.ds.orion.actions;

import com.google.gson.Gson;
import com.mkv.ds.orion.model.View;

public class CreateView implements DatabaseEvent  {

	private static final long serialVersionUID = 1L;

	private final Action action = Action.CREATE_VIEW;

	private final View target;

	public CreateView(View target) {
		super();
		this.target = target;
	}

	@Override
	public Action getAction() {
		return action;
	}

	public View getTarget() {
		return target;
	}

	@Override
	public String toString() {
		return new Gson().toJson(this);
	}
}
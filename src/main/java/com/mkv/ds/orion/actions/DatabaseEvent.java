package com.mkv.ds.orion.actions;

import java.io.Serializable;

public interface DatabaseEvent extends Serializable {
	public Action getAction();
}

package com.packtpub.storm.trident.state;

import java.util.Map;

import storm.trident.state.State;
import storm.trident.state.StateFactory;
import backtype.storm.task.IMetricsContext;

@SuppressWarnings("rawtypes")
//OutbreakTrendFactory 是我们的toplogy提供给Storm的工厂类
public class OutbreakTrendFactory implements StateFactory {

	private static final long serialVersionUID = 1L;

	@Override
	public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
		// 工厂类返回State对象，Storm用它类持久化存储信息
		return new OutbreakTrendState(new OutbreakTrendBackingMap());
	}

}

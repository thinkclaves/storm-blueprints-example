package com.packtpub.storm.trident.operator;

import java.util.ArrayList;
import java.util.List;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class OutbreakDetector extends BaseFunction {

	private static final long serialVersionUID = 1L;

	public static final int THRESHOLD = 10000;

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		// 这个key为聚合的唯一标识
		String key = (String) tuple.getValue(0);
		// 这个count是聚合后的数量
		Long count = (Long) tuple.getValue(1);
		if (count > THRESHOLD) {
			List<Object> values = new ArrayList<>();
			values.add("Outbreak detected for [" + key + "]!");
			collector.emit(values);
		}
		// 如果小于，就什么都不做
	}

}
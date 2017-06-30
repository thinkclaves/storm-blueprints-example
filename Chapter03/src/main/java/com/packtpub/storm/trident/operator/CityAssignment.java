package com.packtpub.storm.trident.operator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import com.packtpub.storm.trident.model.DiagnosisEvent;
// function发送数据时，将新字段添加在tuple中，并不会删除或者变更已有的字段
public class CityAssignment extends BaseFunction {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(CityAssignment.class);

	//静态化的方式建立了我们关心的城市的地图
	private static Map<String, double[]> CITIES = new HashMap<>();

	{
		// Initialize the cities we care about.
		double[] phl = { 39.875365, -75.249524 };
		CITIES.put("PHL", phl);
		double[] nyc = { 40.71448, -74.00598 };
		CITIES.put("NYC", nyc);
		double[] sf = { -31.4250142, -62.0841809 };
		CITIES.put("SF", sf);
		double[] la = { -34.05374, -118.24307 };
		CITIES.put("LA", la);
	}

	// function声明的字段数量必须和发射出去的字段数一直，如果不一致，Storm就会抛出IndexOutOfBoundsException异常
	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		// 计算事件与城市之间的距离
		DiagnosisEvent diagnosis = (DiagnosisEvent) tuple.getValue(0);
		double leastDistance = Double.MAX_VALUE;
		String closestCity = "NONE";
		// Entry这个很有意思
		for (Entry<String, double[]> city : CITIES.entrySet()) {
			//与每个城市都遍历一次，取最近的城市病返回
			double R = 6371; // km
			double x = (city.getValue()[0] - diagnosis.lng) * Math.cos((city.getValue()[0] + diagnosis.lng) / 2);
			double y = (city.getValue()[1] - diagnosis.lat);
			double d = Math.sqrt(x * x + y * y) * R;
			if (d < leastDistance) {
				leastDistance = d;
				closestCity = city.getKey();
			}
		}
		List<Object> values = new ArrayList<>();
		values.add(closestCity);
		LOG.debug("Closest city to lat=[" + diagnosis.lat + "], lng=[" + diagnosis.lng + "] == [" + closestCity
				+ "], d=[" + leastDistance + "]");
		collector.emit(values);
	}

}

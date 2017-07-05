package com.packtpub.storm.trident.topology;

import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

import com.packtpub.storm.trident.operator.CityAssignment;
import com.packtpub.storm.trident.operator.DiseaseFilter;
import com.packtpub.storm.trident.operator.DispatchAlert;
import com.packtpub.storm.trident.operator.HourAssignment;
import com.packtpub.storm.trident.operator.OutbreakDetector;
import com.packtpub.storm.trident.spout.DiagnosisEventSpout;
import com.packtpub.storm.trident.state.OutbreakTrendFactory;

public class OutbreakDetectionTopology {

	public static StormTopology buildTopology() {
		TridentTopology topology = new TridentTopology();
		DiagnosisEventSpout spout = new DiagnosisEventSpout();
		Stream inputStream = topology.newStream("event", spout);

		inputStream.each(new Fields("event"), new DiseaseFilter())
				// DiseaseFilter() 过滤到不关心的疾病事件
				// CityAssignment() 为疾病事件赋值一个对应的城市名
				.each(new Fields("event"), new CityAssignment(), new Fields("city"))
				// cityDiseaseHour 包括城市、小时和疾病代码
				.each(new Fields("event", "city"), new HourAssignment(), new Fields("hour", "cityDiseaseHour"))
				.groupBy(new Fields("cityDiseaseHour"))
				// 使用persistentAggregate 对统计量进行持久性存储
				// OutbreakTrendFactory 是我们的toplogy提供给Storm的工厂类
				.persistentAggregate(new OutbreakTrendFactory(), new Count(), new Fields("count")).newValuesStream()
				// 如果统计量超过阈值，OutbreakDetector向后发送一个告警信息
				.each(new Fields("cityDiseaseHour", "count"), new OutbreakDetector(), new Fields("alert"))
				// DispatchAlert接受到告警信息，记录日志
				.each(new Fields("alert"), new DispatchAlert(), new Fields());
		return topology.build();
	}

	public static void main(String[] args) throws Exception {
		Config conf = new Config();
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("cdc", conf, buildTopology());
		Thread.sleep(200000);
		cluster.shutdown();
	}

}

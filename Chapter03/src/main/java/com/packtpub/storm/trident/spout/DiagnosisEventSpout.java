package com.packtpub.storm.trident.spout;

import java.util.Map;

import storm.trident.spout.ITridentSpout;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
// @SuppressWarnings()表示不显示使用了不赞成使用的类或方法时的警告
@SuppressWarnings("rawtypes")
public class DiagnosisEventSpout implements ITridentSpout<Long> {

	private static final long serialVersionUID = 1L;
    // BatchCoordinator 负责管理批次和元数据
	BatchCoordinator<Long> coordinator = new DefaultCoordinator();
	// Emitter需要依靠元数据来恰当的进行批次的数据重放
	Emitter<Long> emitter = new DiagnosisEventEmitter();

	@Override
	public BatchCoordinator<Long> getCoordinator(String txStateId, Map conf, TopologyContext context) {
		return coordinator;
	}

	@Override
	public Emitter<Long> getEmitter(String txStateId, Map conf, TopologyContext context) {
		return emitter;
	}

	@Override
	public Map getComponentConfiguration() {
		return null;
	}

	@Override
	public Fields getOutputFields() {
		return new Fields("event");
	}

}
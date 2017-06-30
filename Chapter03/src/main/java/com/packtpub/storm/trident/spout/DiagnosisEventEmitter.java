package com.packtpub.storm.trident.spout;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import storm.trident.operation.TridentCollector;
import storm.trident.spout.ITridentSpout.Emitter;
import storm.trident.topology.TransactionAttempt;

import com.packtpub.storm.trident.model.DiagnosisEvent;

public class DiagnosisEventEmitter implements Emitter<Long>, Serializable {

	private static final long serialVersionUID = 1L;

	AtomicInteger successfulTransactions = new AtomicInteger(0);

	//Spout使用collector类发送tuple
	@Override
	// 参数包括batch元数据，事务信息和Emitter用来发送tuple的collector
	public void emitBatch(TransactionAttempt tx, Long coordinatorMeta, TridentCollector collector) {
		for (int i = 0; i < 10000; i++) {
			List<Object> events = new ArrayList<>();
			double lat = new Double(-30 + (int) (Math.random() * 75));
			double lng = new Double(-120 + (int) (Math.random() * 70));
			long time = System.currentTimeMillis();

			//ICD-9-CM 范围为：320-327
			String diag = new Integer(320 + (int) (Math.random() * 7)).toString();
			DiagnosisEvent event = new DiagnosisEvent(lat, lng, time, diag);
			// spout发送的是DiagnosisEvent类，字段为event
			events.add(event);
			collector.emit(events);
		}
	}

	@Override
	public void success(TransactionAttempt tx) {
		successfulTransactions.incrementAndGet();
	}

	@Override
	public void close() {
		//
	}

}

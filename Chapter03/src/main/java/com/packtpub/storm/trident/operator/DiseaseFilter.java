package com.packtpub.storm.trident.operator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

import com.packtpub.storm.trident.model.DiagnosisEvent;
// Trident提供BaseFilter类，我们仅需实现子类，就可以方便对tuple进行过滤
public class DiseaseFilter extends BaseFilter {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(DiseaseFilter.class);

	@Override
	public boolean isKeep(TridentTuple tuple) {
		DiagnosisEvent diagnosis = (DiagnosisEvent) tuple.getValue(0);
		Integer code = Integer.parseInt(diagnosis.diagnosisCode);
		if (code.intValue() <= 322) {
			LOG.debug("Emitting disease [" + diagnosis.diagnosisCode + "]");
			return true;
		} else {
			LOG.debug("Filtering disease [" + diagnosis.diagnosisCode + "]");
			// 如果返回false，tuple就不会发送到下游
			return false;
		}
	}

}
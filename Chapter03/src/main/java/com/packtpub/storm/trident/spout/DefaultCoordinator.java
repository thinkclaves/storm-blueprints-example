package com.packtpub.storm.trident.spout;

import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.spout.ITridentSpout.BatchCoordinator;
// BatchCoordinator是一个泛型类，重放一个batch所需要的元数据
// 实际项目中，元数据可能包含组成这个batch的消息或者对象的标识符
// BatchCoordinator 作为bolt，运行在单线程中
public class DefaultCoordinator implements BatchCoordinator<Long>, Serializable {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(DefaultCoordinator.class);

	@Override
	public boolean isReady(long txid) {
		return true;
	}

	@Override
	public void close() {
		//
	}

	@Override
	public Long initializeTransaction(long txid, Long prevMetadata, Long currMetadata) {
		LOG.info("Initializing Transaction [" + txid + "]");
		return null;
	}

	@Override
	public void success(long txid) {
		LOG.info("Successful Transaction [" + txid + "]");
	}

}
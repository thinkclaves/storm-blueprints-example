package com.packtpub.storm.trident.state;

import storm.trident.state.map.NonTransactionalMap;
// 由于没有事务性保证，所以选用NonTransactionalMap作为State对象
public class OutbreakTrendState extends NonTransactionalMap<Long> {

	protected OutbreakTrendState(OutbreakTrendBackingMap outbreakBackingMap) {
		super(outbreakBackingMap);
	}

}

package com.wankun.hbasetest.coprocessor;

import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;


public class TestNavigableMap {
	public static void main(String[] args) {
		try {

			/**
			 * NavigableMap Map
			 */

			NavigableMap<String, Integer> map = new TreeMap<String, Integer>();

			map.put("1", 11);
			map.put("2", 22);
			map.put("3", 33);
			map.put("4", 44);

			// 小于该key且离该key最近的一个key
			String lowerKey = (String) map.lowerKey("2");
			System.out.println("lowerKey:" + lowerKey);

			Map.Entry entry = map.higherEntry("2");
			System.out.println("entry.getKey():" + entry.getKey());
			System.out.println("entry.getValue():" + entry.getValue());

			// 小于等于该key且离该key最近的一个key
			String floorKey = map.floorKey("2");
			System.out.println("floorKey:" + floorKey);

			// 大于等于该key且离该key最近的一个key
			String ceilingKey = map.ceilingKey("2");
			System.out.println("ceilingKey:" + ceilingKey);

			// 小于
			SortedMap headMap = map.headMap("2");
			System.out.println("headMap:" + headMap);

			// 大于等于
			SortedMap tailMap = map.tailMap("2");
			System.out.println("tailMap:" + tailMap);

			/**
			 * NavigableSet Set
			 */
			NavigableSet<Integer> set = new TreeSet<Integer>();
			set.add(1001);
			set.add(1002);
			set.add(1003);
			set.add(1004);

			// 小于
			Integer lower = set.lower(1003);
			System.out.println("lower:" + lower);
			// 小于等于
			Integer floor = set.floor(1003);
			System.out.println("floor:" + floor);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

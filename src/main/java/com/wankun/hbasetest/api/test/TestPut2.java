package com.wankun.hbasetest.api.test;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import com.wankun.hbasetest.util.MakeData;
import com.wankun.hbasetest.util.SimpleData;

/**
 * 1. table.put时，put记录的长度越长，数据插入效率越低 ，无论是分批次还是单记录操作
 * 
 * 
 * 一次数据插入测试情况
 * <table>
 * <tr >
 * <td >插入方式</td>
 * <td>autoflash</td>
 * <td>flush commit</td>
 * <td>插入记录数</td>
 * <td>插入时间</td>
 * <td>插入效率</td>
 * </tr>
 * <tr >
 * <td>1000条批量</td>
 * <td>默认</td>
 * <td>开启</td>
 * <td>5000000</td>
 * <td>75</td>
 * <td>66667</td>
 * </tr>
 * <tr >
 * <td>1000条批量</td>
 * <td>默认</td>
 * <td>关闭</td>
 * <td>5000000</td>
 * <td>76</td>
 * <td>65789</td>
 * </tr>
 * <tr >
 * <td>1000条批量</td>
 * <td>FALSE</td>
 * <td>开启</td>
 * <td>5000000</td>
 * <td>74</td>
 * <td>67568</td>
 * </tr>
 * <tr >
 * <td>1000条批量</td>
 * <td>FALSE</td>
 * <td>关闭</td>
 * <td>5000000</td>
 * <td>68</td>
 * <td>73529</td>
 * </tr>
 * <tr >
 * <td>单记录</td>
 * <td>默认</td>
 * <td>开启</td>
 * <td>50000</td>
 * <td>56</td>
 * <td>893</td>
 * </tr>
 * <tr >
 * <td>单记录</td>
 * <td>默认</td>
 * <td>关闭</td>
 * <td>50000</td>
 * <td>55</td>
 * <td>909</td>
 * </tr>
 * <tr >
 * <td>单记录</td>
 * <td>FALSE</td>
 * <td>开启</td>
 * <td>50000</td>
 * <td>55</td>
 * <td>909</td>
 * </tr>
 * <tr >
 * <td>单记录</td>
 * <td>FALSE</td>
 * <td>关闭</td>
 * <td>5000000</td>
 * <td>66</td>
 * <td>75758</td>
 * </tr>
 * </table>
 * 
 * @author wankun
 * @date 2014年10月10日
 * @version 1.0
 */
public class TestPut2 extends HBaseTest {

	public static void main(String[] args) {
		TestPut2 test = new TestPut2();
		test.runTest();
	}

	public MakeData md = new MakeData();

	@Override
	public void init() {
		md.start();
	}

	@Override
	public void test(HTable table) throws Exception {
		testSingleLong(table);
	}

	/**
	 * 直接put数据效率： 670
	 * 
	 * @param table
	 * @throws Exception
	 */
	public void testSingleLong(HTable table) throws Exception {
		table.setAutoFlush(false);
		int ALLROWS = 700000;
		for (long i = 0; i < ALLROWS; i++) {
			String rowKey = rowKeyFormatter.format(i);
			Put put = MakeData.genPut(md, rowKey);
			table.put(put);
			rownum++;
		}
	}

	/**
	 * 直接put数据效率： 531
	 * 
	 * @param table
	 * @throws Exception
	 */
	public void testBatchLong(HTable table) throws Exception {
		table.setAutoFlush(false);

		List<Put> puts;
		for (long i = 0; i < 5; i++) {
			puts = new ArrayList<Put>();
			for (int j = 0; j < 1000; j++) {
				// Put put = genPut();
				String rowKey = rowKeyFormatter.format(i * 1000 + j);
				Put put = MakeData.genPut(md, rowKey);
				puts.add(put);
				rownum++;
			}
			table.put(puts);
			table.flushCommits();
		}
	}

	/**
	 * 小put数据单次效率： 23390
	 * 
	 * @param table
	 * @throws Exception
	 */
	public void testSingleShort(HTable table) throws Exception {
		table.setAutoFlush(false);
		int ALLROWS = 500000;
		long l = 308948191001l;
		for (long i = 0; i < ALLROWS; i++) {
			Put put = SimpleData.genPut(l++);
			table.put(put);
			rownum++;
		}
	}

	/**
	 * 小put数据批次效率： 23066
	 * 
	 * @param table
	 * @throws Exception
	 */
	public void testBatchShort(HTable table) throws Exception {
		table.setAutoFlush(false,false);

		List<Put> puts;
		long l = 308948191001l;
		for (long i = 0; i < 1000; i++) {
			puts = new ArrayList<Put>();
			for (int j = 0; j < 1000; j++) {
				Put put = SimpleData.genPut(l++);
				puts.add(put);
				rownum++;
			}
			table.put(puts);
			table.flushCommits();
		}
	}

}

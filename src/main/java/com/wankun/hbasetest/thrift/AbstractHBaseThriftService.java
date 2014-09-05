package com.wankun.hbasetest.thrift;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.thrift.generated.Hbase;
import org.apache.hadoop.hbase.thrift.generated.TRowResult;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * 	建立到Thrift服务的连接：open()
	 获取到HBase中的所有表名：getTables()
	更新HBase表记录：update()
	删除HBase表中一行的记录的数据（cell）：deleteCell()和deleCells()
	删除HBase表中一行记录：deleteRow()
	打开一个Scanner，返回id：scannerOpen()、scannerOpenWithPrefix()和scannerOpenTs()；然后用返回的id迭代记录：scannerGetList()和scannerGet()
	获取一行记录结果：getRow()、getRows()和getRowsWithColumns()
	关闭一个Scanner：scannerClose()
	迭代结果，用于调试：iterateResults()
 * @author wankun
 *
 */
public abstract class AbstractHBaseThriftService {

	protected static final String CHARSET = "UTF-8";
	private String host = "localhost";
	private int port = 9090;
	private final TTransport transport;
	protected final Hbase.Client client;

	public AbstractHBaseThriftService() {
		transport = new TSocket(host, port);
		TProtocol protocol = new TBinaryProtocol(transport, true, true);
		client = new Hbase.Client(protocol);
	}

	public AbstractHBaseThriftService(String host, int port) {
		super();
		transport = new TSocket(host, port);
		TProtocol protocol = new TBinaryProtocol(transport, true, true);
		client = new Hbase.Client(protocol);
	}

	public void open() throws TTransportException {
		if (transport != null) {
			transport.open();
		}
	}

	public void close() {
		if (transport != null) {
			transport.close();
		}
	}

	public abstract List<String> getTables() throws TException;

	public abstract void update(String table, String rowKey, boolean writeToWal, String fieldName, String fieldValue, Map<String, String> attributes) throws TException;

	public abstract void update(String table, String rowKey, boolean writeToWal, Map<String, String> fieldNameValues, Map<String, String> attributes) throws TException;

	public abstract void deleteCell(String table, String rowKey, boolean writeToWal, String column, Map<String, String> attributes) throws TException;

	public abstract void deleteCells(String table, String rowKey, boolean writeToWal, List<String> columns, Map<String, String> attributes) throws TException;

	public abstract void deleteRow(String table, String rowKey, Map<String, String> attributes) throws TException;

	public abstract int scannerOpen(String table, String startRow, List<String> columns, Map<String, String> attributes) throws TException;

	public abstract int scannerOpen(String table, String startRow, String stopRow, List<String> columns, Map<String, String> attributes) throws TException;

	public abstract int scannerOpenWithPrefix(String table, String startAndPrefix, List<String> columns, Map<String, String> attributes) throws TException;

	public abstract int scannerOpenTs(String table, String startRow, List<String> columns, long timestamp, Map<String, String> attributes) throws TException;

	public abstract int scannerOpenTs(String table, String startRow, String stopRow, List<String> columns, long timestamp, Map<String, String> attributes) throws TException;

	public abstract List<TRowResult> scannerGetList(int id, int nbRows) throws TException;

	public abstract List<TRowResult> scannerGet(int id) throws TException;

	public abstract List<TRowResult> getRow(String table, String row, Map<String, String> attributes) throws TException;

	public abstract List<TRowResult> getRows(String table, List<String> rows, Map<String, String> attributes) throws TException;

	public abstract List<TRowResult> getRowsWithColumns(String table, List<String> rows, List<String> columns, Map<String, String> attributes) throws TException;

	public abstract void scannerClose(int id) throws TException;

	/**
	 * Iterate result rows(just for test purpose)
	 * 
	 * @param result
	 */
	public abstract void iterateResults(TRowResult result);

}

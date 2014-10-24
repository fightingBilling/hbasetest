## hbase coprocessor

	1. hbase 0.96 中移除了AggregationClient 类，在0.98中又加入了该类
	
	2. 启用该协处理器
	2.1 首先将这两个类打成jar包，上传到hdfs上去，比如我上传到了 hdfs://mycluster/tmp/wankun/hbasetest-1.0.0.jar
	2.2 将该协处理器设置到某个具体的表上


3 写个测试类测试一下：
[java] view plaincopy
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.hbase.HBaseConfiguration;  
import org.apache.hadoop.hbase.TableName;  
import org.apache.hadoop.hbase.client.Scan;  
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;  
  
public class TestAggregationClient {  
    private static Configuration hbaseConfig = null;  
    static {  
  
        hbaseConfig = HBaseConfiguration.create();  
  
    }  
  
    public static void main(String[] args) throws Throwable {  
        AggregationClient client = new AggregationClient(hbaseConfig);  
        final Scan scan;  
        scan = new Scan();  
        Long count = client.rowCount(TableName.valueOf("userinfo"),  
                new LongColumnInterpreter(), scan);  
          
        System.out.println(count);  
    }  
  
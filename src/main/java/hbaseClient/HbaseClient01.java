package hbaseClient;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HbaseClient01 {
    private static Connection connection;
    public static void main(String[] args) throws Exception {
        System.out.println(exist("kgc1"));
        createtable("kgc1","cf1","cf2","cf3");
    }
    //静态代码块
    static {
        //获取连接
        HBaseConfiguration conf = new HBaseConfiguration();
        //有Hmaste的那台服务器IP
        conf.set("hbase.zokeeper.quorum","192.168.64.110");
        //zookeeper端口
        conf.set("hbase.zookeeper.property.clientPort","2181");
        //获取连接对象
        try {
            Connection connection = ConnectionFactory.createConnection(conf);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    //判断表是否存在
    public static boolean exist(String table) throws IOException {
        //拿到admin对象
        Admin admin = connection.getAdmin();
        //判断
        boolean b = admin.tableExists(TableName.valueOf(table));
        return b;
    }
    //创建表
    public static void createtable(String table,String...cf) throws IOException {
        Admin admin = connection.getAdmin();
        //new一个
        HTableDescriptor hTableDescriptor = new HTableDescriptor();
        //遍历cf
        for (String s : cf) {
            hTableDescriptor.addFamily(new HColumnDescriptor(s));
        }
        //创建
        admin.createTable(hTableDescriptor);
        System.out.println("创建成功");
    }
    //删除
    public static void delete(String table) throws IOException {
        Admin admin = connection.getAdmin();
        //
        if (exist(table)) {
            admin.disableTable(TableName.valueOf(table));
            admin.deleteTables(table);
        }
        System.out.println("删除成功");
    }
    //添加
    public static void addtable(String table,String rowkey,String columnfamily,String column,String value) throws IOException {
        //拿到table
        Table table1 = connection.getTable(TableName.valueOf(table));
        //new一个put
        Put put = new Put(Bytes.toBytes(rowkey));
        //接下来添加
        put.add(Bytes.toBytes(columnfamily),Bytes.toBytes(column),Bytes.toBytes(value));
        //接下来告知上边我有值了
        table1.put(put);
        System.out.println("添加成功");

    }
    public static void scan(String table) throws IOException {
        Table table1 = connection.getTable(TableName.valueOf(table));
        Scan scan = new Scan();
        ResultScanner results = table1.getScanner(scan);
        for (Result result : results) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                System.out.println(cell.getFamilyArray().toString());
                System.out.println(cell.getRow().toString());
                System.out.println(cell.getQualifierArray().toString());
                System.out.println(cell.getValue().toString());
            }
        }
    }
    public static void get(String table) throws IOException {
        Table table1 = connection.getTable(TableName.valueOf(table));
        Get get = new Get(Bytes.toBytes(table));
        Result result = table1.get(get);
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.println(cell.getFamilyArray().toString());
            System.out.println(cell.getRow().toString());
            System.out.println(cell.getQualifierArray().toString());
            System.out.println(cell.getValue().toString());
        }
    }
}

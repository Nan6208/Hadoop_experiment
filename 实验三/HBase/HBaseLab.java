import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseLab {

    // 读取 src/main/resources 中的 HBase、Hadoop 配置
    private static Configuration getConf() {
        return HBaseConfiguration.create();
    }

    // 检查表是否存在
    private static void checkTable(
            Admin admin, TableName tableName) throws IOException {
        if (!admin.tableExists(tableName)) {
            throw new IllegalArgumentException(
                    "表不存在：" + tableName.getNameAsString());
        }
    }

    /*
     * 1. 创建表
     * fields 表示列族名称数组，例如 {"Info"} 或 {"Score"}。
     *
     * 按实验要求：同名表已存在时，先删除再重建。
     * 注意：该操作会删除原表数据！
     */
    public static void createTable(
            String tableName, String[] fields) throws IOException {

        if (fields == null || fields.length == 0) {
            throw new IllegalArgumentException("列族数组不能为空");
        }

        TableName tn = TableName.valueOf(tableName);

        try (Connection connection =
                     ConnectionFactory.createConnection(getConf());
             Admin admin = connection.getAdmin()) {

            if (admin.tableExists(tn)) {
                if (admin.isTableEnabled(tn)) {
                    admin.disableTable(tn);
                }
                admin.deleteTable(tn);
                System.out.println("已删除原表：" + tableName);
            }

            TableDescriptorBuilder builder =
                    TableDescriptorBuilder.newBuilder(tn);

            for (String family : fields) {
                if (family == null || family.trim().isEmpty()
                        || family.contains(":")) {
                    throw new IllegalArgumentException(
                            "列族名称不能为空，也不能包含冒号：" + family);
                }

                builder.setColumnFamily(
                        ColumnFamilyDescriptorBuilder.of(family.trim()));
            }

            admin.createTable(builder.build());
            System.out.println("创建表成功：" + tableName);
        }
    }

    /*
     * 2. 添加记录
     * row 是 RowKey。
     * fields 示例：{"Info:S_No", "Info:S_Name"}
     * values 示例：{"2015001", "Zhangsan"}
     */
    public static void addRecord(
            String tableName,
            String row,
            String[] fields,
            String[] values) throws IOException {

        if (fields == null || values == null
                || fields.length != values.length) {
            throw new IllegalArgumentException(
                    "fields 和 values 必须非空且长度相同");
        }

        if (row == null || row.isEmpty()) {
            throw new IllegalArgumentException("RowKey 不能为空");
        }

        TableName tn = TableName.valueOf(tableName);

        try (Connection connection =
                     ConnectionFactory.createConnection(getConf());
             Admin admin = connection.getAdmin()) {

            checkTable(admin, tn);

            try (Table table = connection.getTable(tn)) {
                Put put = new Put(Bytes.toBytes(row));

                for (int i = 0; i < fields.length; i++) {
                    String[] parts = fields[i].split(":", 2);

                    if (parts.length != 2
                            || parts[0].isEmpty()
                            || parts[1].isEmpty()) {
                        throw new IllegalArgumentException(
                                "字段格式必须为 列族:列限定符：" + fields[i]);
                    }

                    put.addColumn(
                            Bytes.toBytes(parts[0]),
                            Bytes.toBytes(parts[1]),
                            Bytes.toBytes(values[i]));
                }

                table.put(put);
                System.out.println("写入成功：表=" + tableName
                        + "，RowKey=" + row);
            }
        }
    }

    /*
     * 3. 查询指定列族或列
     *
     * column = "Info"          查询整个列族
     * column = "Info:S_Name"   查询指定列
     *
     * 返回结构：
     * RowKey -> (列名 -> 值)
     * 对指定列不存在的行，将对应值设为 null。
     */
    public static Map<String, Map<String, String>> scanColumn(
            String tableName, String column) throws IOException {

        if (column == null || column.trim().isEmpty()) {
            throw new IllegalArgumentException("查询列不能为空");
        }

        String[] parts = column.split(":", 2);
        boolean exactColumn = parts.length == 2;

        String family = parts[0];
        String qualifier = exactColumn ? parts[1] : null;

        TableName tn = TableName.valueOf(tableName);

        Map<String, Map<String, String>> output =
                new LinkedHashMap<>();

        try (Connection connection =
                     ConnectionFactory.createConnection(getConf());
             Admin admin = connection.getAdmin()) {

            checkTable(admin, tn);

            try (Table table = connection.getTable(tn);
                 ResultScanner scanner =
                         table.getScanner(new Scan())) {

                for (Result result : scanner) {
                    String rowKey = Bytes.toString(result.getRow());
                    Map<String, String> cells =
                            new LinkedHashMap<>();

                    if (exactColumn) {
                        byte[] value = result.getValue(
                                Bytes.toBytes(family),
                                Bytes.toBytes(qualifier));

                        cells.put(column,
                                value == null ? null : Bytes.toString(value));
                    } else {
                        NavigableMap<byte[], byte[]> familyMap =
                                result.getFamilyMap(Bytes.toBytes(family));

                        if (familyMap == null || familyMap.isEmpty()) {
                            cells.put(family, null);
                        } else {
                            for (Map.Entry<byte[], byte[]> entry
                                    : familyMap.entrySet()) {
                                String q = Bytes.toString(entry.getKey());
                                String v = Bytes.toString(entry.getValue());
                                cells.put(family + ":" + q, v);
                            }
                        }
                    }

                    output.put(rowKey, cells);
                }
            }
        }

        for (Map.Entry<String, Map<String, String>> row
                : output.entrySet()) {
            System.out.println("RowKey = " + row.getKey());

            for (Map.Entry<String, String> cell
                    : row.getValue().entrySet()) {
                System.out.println("  " + cell.getKey()
                        + " = " + cell.getValue());
            }
        }

        return output;
    }

    /*
     * 4. 修改单元格
     * 实验文件未给出新值参数，因此补充 newValue。
     * HBase 的 put 会覆盖同一单元格的当前值，
     * 并在保留版本的配置下按 HBase 版本机制保存历史版本。
     */
    public static void modifyData(
            String tableName,
            String row,
            String column,
            String newValue) throws IOException {

        addRecord(
                tableName,
                row,
                new String[]{column},
                new String[]{newValue});

        System.out.println("修改成功");
    }

    /*
     * 5. 删除指定 RowKey 的整行记录
     */
    public static void deleteRow(
            String tableName, String row) throws IOException {

        TableName tn = TableName.valueOf(tableName);

        try (Connection connection =
                     ConnectionFactory.createConnection(getConf());
             Admin admin = connection.getAdmin()) {

            checkTable(admin, tn);

            try (Table table = connection.getTable(tn)) {
                Delete delete = new Delete(Bytes.toBytes(row));
                table.delete(delete);
                System.out.println("已删除行：" + row);
            }
        }
    }

    // 辅助功能：列出 HBase 中的所有表
    public static void listTables() throws IOException {
        try (Connection connection =
                     ConnectionFactory.createConnection(getConf());
             Admin admin = connection.getAdmin()) {

            for (TableName tn : admin.listTableNames()) {
                System.out.println(tn.getNameAsString());
            }
        }
    }

    // 辅助功能：扫描整张表
    public static void scanTable(String tableName) throws IOException {
        TableName tn = TableName.valueOf(tableName);

        try (Connection connection =
                     ConnectionFactory.createConnection(getConf());
             Admin admin = connection.getAdmin()) {

            checkTable(admin, tn);

            try (Table table = connection.getTable(tn);
                 ResultScanner scanner =
                         table.getScanner(new Scan())) {

                for (Result result : scanner) {
                    System.out.println(
                            "RowKey = " + Bytes.toString(result.getRow()));

                    for (Cell cell : result.rawCells()) {
                        System.out.println(
                                "  "
                                + Bytes.toString(CellUtil.cloneFamily(cell))
                                + ":"
                                + Bytes.toString(CellUtil.cloneQualifier(cell))
                                + " = "
                                + Bytes.toString(CellUtil.cloneValue(cell)));
                    }
                }
            }
        }
    }

    // 辅助功能：统计表的行数
    public static long countRows(String tableName) throws IOException {
        TableName tn = TableName.valueOf(tableName);
        long count = 0;

        try (Connection connection =
                     ConnectionFactory.createConnection(getConf());
             Admin admin = connection.getAdmin()) {

            checkTable(admin, tn);

            try (Table table = connection.getTable(tn);
                 ResultScanner scanner =
                         table.getScanner(new Scan())) {

                for (Result ignored : scanner) {
                    count++;
                }
            }
        }

        System.out.println(tableName + " 行数 = " + count);
        return count;
    }

    // 辅助功能：添加列族
    public static void addColumnFamily(
            String tableName, String family) throws IOException {

        TableName tn = TableName.valueOf(tableName);

        try (Connection connection =
                     ConnectionFactory.createConnection(getConf());
             Admin admin = connection.getAdmin()) {

            checkTable(admin, tn);

            TableDescriptor oldDescriptor = admin.getDescriptor(tn);
            TableDescriptorBuilder builder =
                    TableDescriptorBuilder.newBuilder(oldDescriptor);

            builder.setColumnFamily(
                    ColumnFamilyDescriptorBuilder.of(family));

            admin.modifyTable(builder.build());
            System.out.println("已添加列族：" + family);
        }
    }

    // 辅助功能：删除列族
    public static void deleteColumnFamily(
            String tableName, String family) throws IOException {

        TableName tn = TableName.valueOf(tableName);

        try (Connection connection =
                     ConnectionFactory.createConnection(getConf());
             Admin admin = connection.getAdmin()) {

            checkTable(admin, tn);

            TableDescriptor oldDescriptor = admin.getDescriptor(tn);

            if (!oldDescriptor.hasColumnFamily(Bytes.toBytes(family))) {
                throw new IllegalArgumentException(
                        "列族不存在：" + family);
            }

            TableDescriptorBuilder builder =
                    TableDescriptorBuilder.newBuilder(oldDescriptor);

            builder.removeColumnFamily(Bytes.toBytes(family));
            admin.modifyTable(builder.build());

            System.out.println("已删除列族：" + family);
        }
    }

    // 辅助功能：清空整张表的记录，但保留表结构
    public static void clearTable(String tableName) throws IOException {
        TableName tn = TableName.valueOf(tableName);

        try (Connection connection =
                     ConnectionFactory.createConnection(getConf());
             Admin admin = connection.getAdmin()) {

            checkTable(admin, tn);

            if (!admin.isTableEnabled(tn)) {
                admin.enableTable(tn);
            }

            admin.truncateTable(tn, true);
            System.out.println("已清空表：" + tableName);
        }
    }

    // 命令行入口
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            printUsage();
            return;
        }

        switch (args[0]) {
            case "list":
                listTables();
                break;

            case "scan":
                scanTable(args[1]);
                break;

            case "scanColumn":
                scanColumn(args[1], args[2]);
                break;

            case "count":
                countRows(args[1]);
                break;

            case "createTable":
                createTable(args[1], args[2].split(","));
                break;

            case "addRecord":
                addRecord(
                        args[1], args[2],
                        args[3].split(",", -1),
                        args[4].split(",", -1));
                break;

            case "modifyData":
                modifyData(args[1], args[2], args[3], args[4]);
                break;

            case "deleteRow":
                deleteRow(args[1], args[2]);
                break;

            case "addFamily":
                addColumnFamily(args[1], args[2]);
                break;

            case "deleteFamily":
                deleteColumnFamily(args[1], args[2]);
                break;

            case "clear":
                clearTable(args[1]);
                break;

            default:
                printUsage();
        }
    }

    private static void printUsage() {
        System.out.println("可用命令：");
        System.out.println("  list");
        System.out.println("  scan <table>");
        System.out.println("  scanColumn <table> <family|family:qualifier>");
        System.out.println("  count <table>");
        System.out.println("  createTable <table> <family1,family2>");
        System.out.println("  addRecord <table> <row> <family:qualifier,...> <value,...>");
        System.out.println("  modifyData <table> <row> <family:qualifier> <newValue>");
        System.out.println("  deleteRow <table> <row>");
        System.out.println("  addFamily <table> <family>");
        System.out.println("  deleteFamily <table> <family>");
        System.out.println("  clear <table>");
    }
}

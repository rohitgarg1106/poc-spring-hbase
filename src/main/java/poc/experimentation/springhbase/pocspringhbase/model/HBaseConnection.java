package poc.experimentation.springhbase.pocspringhbase.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import poc.experimentation.springhbase.pocspringhbase.constants.HBaseConstants;
import poc.experimentation.springhbase.pocspringhbase.exception.HBaseTableExistsException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Slf4j
public class HBaseConnection {

    private Connection connection;

    private Admin admin;


    private ObjectMapper mapper;

    public Connection getConnection() {
        return connection;
    }

    public Admin getAdmin() {
        return admin;
    }

    public HBaseConnection(String zkHost, Integer zkPort) throws IOException {
        this.connection = getHBaseConnection(zkHost, zkPort);
        this.admin = getConnectionAdmin();
        this.mapper = new ObjectMapper();
    }

    public synchronized Connection getHBaseConnection(String zkHost, Integer zkPort) throws IOException {
            org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
            config.set(HBaseConstants.ZK_HOST, zkHost);
            config.set(HBaseConstants.ZK_PORT, zkPort.toString());
            config.set("hbase.client.pause","1000");
            config.set("hbase.client.retries.number","3");
            config.set("zookeeper.recovery.retry","1");
            config.set("hbase.rpc.timeout","10000");
            config.set("hbase.client.scanner.timeout.period","10000");
            return ConnectionFactory.createConnection(config);
    }

    private Admin getConnectionAdmin(){
        Admin admin = null;
        try{
            admin = this.connection.getAdmin();
        }
        catch (Exception e){
            e.printStackTrace();
        }
        return admin;
    }

    public void destroy() {
        try{
            this.connection.close();
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

    // CRUD Operations
    /**
     * returns table object
     * */
    public Table getTable(String namespace, String tableName) throws IOException {
        TableName table = getTableName(namespace,tableName);
        return this.connection.getTable(table);

    }

    /**
     * returns TableName object
     * */
    public TableName getTableName(String namespace, String tableName) throws IOException {
        return TableName.valueOf(namespace, tableName);
    }

    /**
     * lists all the tables present as descriptors
     * */
    public List<TableDescriptor> listTables() throws IOException {
        List<TableDescriptor> tableDescriptorList = this.admin.listTableDescriptors();
        return tableDescriptorList;
    }

    /**
     * lists all the tables present as strings
     * */
    public List<String> listTableNames() throws IOException {
        List<TableDescriptor> tableDescriptorList = this.admin.listTableDescriptors();
        return tableDescriptorList.stream().map(td -> td.getTableName().getNameAsString()).collect(Collectors.toList());
    }

    /**
     * lists all the tables in a namespace present as strings
     * */
    public List<String> listTableNames(String namespace) throws IOException {
        byte[] namespaceBytes = Bytes.toBytes(namespace);
        List<TableDescriptor> tableNameList = this.admin.listTableDescriptorsByNamespace(namespaceBytes);
        return tableNameList.stream().map(td -> td.getTableName().getNameAsString()).collect(Collectors.toList());
    }

    /**
    * creates a table using
    * @param namespace
    * @param tableName
    * @param columnFamilies
    * */
    public void createTable(String namespace, String tableName, List<String> columnFamilies) throws IOException, HBaseTableExistsException {


        TableName table = getTableName(namespace, tableName);
        if(this.admin.tableExists(table)){
            throw new HBaseTableExistsException(String.format(tableName, " already exists."));
        }
        HTableDescriptor hTableDescriptor = new HTableDescriptor(table);
        columnFamilies.stream().forEach( cf -> {
            hTableDescriptor.addFamily(new HColumnDescriptor(cf));

        });
        this.admin.createTable(hTableDescriptor);
    }

    /**
     * Puts a column
     * */
    public void putData(HBaseData data) throws IOException {

        byte[] rowKeyBytes = Bytes.toBytes(data.getRow());
        byte[] familyBytes = Bytes.toBytes(data.getColumnFamily());
        byte[] qualifierBytes = Bytes.toBytes(data.getColumnQualifier());
        byte[] objectBytes = this.mapper.writeValueAsBytes(data.getData());

        //define put object on row
        Put put = new Put(rowKeyBytes);
        //add column to put object
        put.addColumn(familyBytes, qualifierBytes, objectBytes);
        //perform put on table
        Table table = getTable(data.getNamespace(), data.getTableName());
        table.put(put);
        log.info("Put operation is successful");
    }

    /**
     * Puts a list of columns(values)
     * */
    public void putData(List<HBaseData> dataList) throws IOException {

        if(dataList.isEmpty()){
            throw new RuntimeException("Empty input list");
        }

        List <Put> putList = new ArrayList<Put>();
        for (HBaseData data : dataList){
            byte[] rowKeyBytes = Bytes.toBytes(data.getRow());
            byte[] familyBytes = Bytes.toBytes(data.getColumnFamily());
            byte[] qualifierBytes = Bytes.toBytes(data.getColumnQualifier());
            byte[] objectBytes = this.mapper.writeValueAsBytes(data.getData());

            //define put object on row
            Put put = new Put(rowKeyBytes);
            //add column to put object
            put.addColumn(familyBytes, qualifierBytes, objectBytes);
            putList.add(put);

        }
        //perform put on table
        Table table = getTable(dataList.get(0).getNamespace(), dataList.get(0).getTableName());
        table.put(putList);
        log.info("Put operation on list is successful");
    }

    /**
     * Gets a column(value) as byte[]
     * */
    public byte[] getData(HBaseData data) throws IOException {
        byte[] rowKeyBytes = Bytes.toBytes(data.getRow());
        byte[] familyBytes = Bytes.toBytes(data.getColumnFamily());
        byte[] qualifierBytes = Bytes.toBytes(data.getColumnQualifier());

        //Get table
        Table table = getTable(data.getNamespace(), data.getTableName());

        //Make get object
        Get get = new Get(rowKeyBytes);
        get.addColumn(familyBytes, qualifierBytes);
        Result result = table.get(get);
        log.info(result.toString());
        byte[] value = result.getValue(familyBytes, qualifierBytes);
        log.info("Get column operation is successful");
        return value;

    }

    /**
     * Gets a row as Result
     * */
    public Result getRow(String namespace, String tableName, String row) throws IOException {

        //Fetch table
        Table table = getTable(namespace, tableName);
        byte[] rowKeyBytes = Bytes.toBytes(row);
        //Make get object
        Get get = new Get(rowKeyBytes);
        Result rowResult = table.get(get);
        log.info("Get row operation is successful");
        return rowResult;
    }

    /**
     * Scans and returns list of result objects using:
     * @param namespace
     * @param tableName
     * @param limit
     * */
    public List<Result> scanTable(String namespace, String tableName, Integer limit) throws IOException {
        return scanTable(namespace, tableName, limit, null, null);
    }

    /**
     * Scans and returns list of result objects using:
     * @param namespace
     * @param tableName
     * @param limit
     * @param startRow
     * @param endRow
     * */
    public List<Result> scanTable(String namespace, String tableName, Integer limit, String startRow, String endRow ) throws IOException {
        Scan scan = new Scan();
        if(limit != null){
            scan.setLimit(limit);
        }

        if(startRow != null){
            byte[] startRowBytes = Bytes.toBytes(startRow);
            scan.withStartRow(startRowBytes);
        }

        if(endRow != null){
            byte[] stopRowBytes = Bytes.toBytes(endRow);
            scan.withStopRow(stopRowBytes);
        }

        Table table = getTable(namespace, tableName);
        ResultScanner resultScanner = table.getScanner(scan);
        List<Result> resultList = StreamSupport.stream(resultScanner.spliterator(), false).collect(Collectors.toList());
        resultScanner.close();
        return resultList;
    }

    /**
     * Deletes a row given
     * @param namespace
     * @param tableName
     * @param row
     * */
    public void deleteRow(String namespace, String tableName, String row) throws IOException {
        Table table = getTable(namespace, tableName);
        byte[] rowKeyBytes = Bytes.toBytes(row);
        Delete del = new Delete(rowKeyBytes);
        table.delete(del);
        log.info("Deleted row successfully.");
    }

    /**
     * Deletes a value(latest value of column) given
     * @param namespace
     * @param tableName
     * @param row
     * @param columnFamily
     * @param columnQualifier
     * */
    public void deleteData(String namespace, String tableName, String row, String columnFamily, String columnQualifier) throws IOException {
        Table table = getTable(namespace, tableName);
        byte[] rowKeyBytes = Bytes.toBytes(row);
        byte[] familyBytes = Bytes.toBytes(columnFamily);
        byte[] qualifierBytes = Bytes.toBytes(columnQualifier);
        Delete del = new Delete(rowKeyBytes);
        del.addColumn(familyBytes, qualifierBytes);
        table.delete(del);
        log.info("Deleted column data successfully.");
    }

    /**
     * Deletes a list of HbaseData values(latest value of column) given list of
     * list <HbaseData> data
     * */
    public void deleteBulkData(List<HBaseData> dataList) throws IOException {

        if(dataList.isEmpty()){
            throw new RuntimeException("List of HbaseData is empty");
        }
        String tableName = dataList.get(0).getTableName();
        String namespace = dataList.get(0).getNamespace();
        Table table = getTable(namespace, tableName);
        List<Delete> deleteOps = new ArrayList<>();
        for (HBaseData data : dataList){
            byte[] rowKeyBytes = Bytes.toBytes(data.getRow());
            byte[] familyBytes = Bytes.toBytes(data.getColumnFamily());
            byte[] qualifierBytes = Bytes.toBytes(data.getColumnQualifier());
            Delete del = new Delete(rowKeyBytes);
            del.addColumn(familyBytes, qualifierBytes);
            deleteOps.add(del);
        }

        table.delete(deleteOps);
        log.info("Deleted bulk column data successfully.");
    }


    /**
     * Appends a value(column) to row
     * */
    public void appendData(HBaseData data) throws IOException {
        Table table = getTable(data.getNamespace(),data.getTableName());
        byte[] rowKeyBytes = Bytes.toBytes(data.getRow());
        Append append = new Append(rowKeyBytes);
        byte[] familyBytes = Bytes.toBytes(data.getColumnFamily());
        byte[] qualifierBytes = Bytes.toBytes(data.getColumnQualifier());
        byte[] objectBytes = this.mapper.writeValueAsBytes(data.getData());
        append.addColumn(familyBytes, qualifierBytes, objectBytes);
        table.append(append);

        log.info("Column value appended successfully");
    }



}

package poc.experimentation.springhbase.pocspringhbase.repository;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.Filter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import poc.experimentation.springhbase.pocspringhbase.exception.HBaseTableExistsException;
import poc.experimentation.springhbase.pocspringhbase.model.HBaseConnection;
import poc.experimentation.springhbase.pocspringhbase.model.HBaseData;
import poc.experimentation.springhbase.pocspringhbase.request.BulkPutDto;
import poc.experimentation.springhbase.pocspringhbase.request.ScanTableDto;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;

@Repository
@Slf4j
public class HBaseCrudRepository {

    @Autowired
    private HBaseConnection connection;

    @Autowired
    private ObjectMapper mapper;

    public void createTable(String namespace, String tableName, List<String> cf) throws IOException, HBaseTableExistsException {
        connection.createTable(namespace, tableName, cf);
    }

    public List<String> listTableNames(String namespace) throws IOException {
        return connection.listTableNames(namespace);
    }

    public void addData(HBaseData data) throws IOException {
        connection.putData(data);
    }

    public byte[] getData(HBaseData hbaseData) throws IOException {
//        return connection.scanTable("ab","audience_user_map", 1).get(0).getValue(Bytes.toBytes("data"), Bytes.toBytes("country"));
        return connection.getData(hbaseData);
    }

    public NavigableMap<byte[], NavigableMap<byte[],byte[]>> getRow(String namespace, String table, String row) throws IOException {
        Result result =  connection.getRow(namespace, table, row);
        return result.getNoVersionMap();

    }

    public List<Result> scanTable(String namespace, String tableName, int limit, Filter filter) throws IOException {
        return connection.scanTable(namespace, tableName,limit,null,null,filter);
    }

    public List<Result> scanTable(ScanTableDto scanTableDto) throws IOException {
        return connection.scanTable(scanTableDto.getNamespace(), scanTableDto.getTableName(),scanTableDto.getLimit(), scanTableDto.getStartRow(), scanTableDto.getEndRow(), scanTableDto.getFilter(), scanTableDto.isIncludeStartRow());
    }

    public boolean bulkPut(BulkPutDto dto) {
        List<Put> putOps = new ArrayList<>();
        if(dto.getRows() == null || dto.getRows().isEmpty()){
            throw new IllegalArgumentException("Expected at least 1 row in list of rows but 0 provided");
        }

        dto.getRows().stream().forEach(row -> {
                Put p = new Put(row.getRowKeyBytes());
                row.getColumns().forEach(col -> {
                    p.addColumn(col.getColumnFamily(), col.getColumnQualifier(), col.getData());
                });
                putOps.add(p);
            });
        return connection.bulkPut(dto.getNamespace(), dto.getTableName(), putOps);
    }

    public boolean containsColumns(String namespace, String tableName, String row, String columnFamily, String columnQualifier) throws IOException {
        Result result = connection.getRow(namespace, tableName, row);
        return result.containsColumn(columnFamily.getBytes(), columnFamily.getBytes());
    }
}



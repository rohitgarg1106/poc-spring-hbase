package poc.experimentation.springhbase.pocspringhbase.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import poc.experimentation.springhbase.pocspringhbase.request.ScanTableRequest;
import poc.experimentation.springhbase.pocspringhbase.exception.HBaseTableExistsException;
import poc.experimentation.springhbase.pocspringhbase.model.HBaseData;
import poc.experimentation.springhbase.pocspringhbase.repository.HBaseCrudRepository;
import poc.experimentation.springhbase.pocspringhbase.request.*;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;


@Service
public class HBaseCrudService {

    @Autowired
    private HBaseCrudRepository repository;

    @Autowired
    private ObjectMapper mapper;

    public List<String> listTables(String namespace) throws IOException {
       return repository.listTableNames(namespace);
    }

    public void createTable(CreateMapStoreRequest request) throws IOException, HBaseTableExistsException {

        repository.createTable(request.getNamespace(), request.getTableName(), request.getCf());
    }

    public void addData(PutDataRequest request) throws IOException {
        HBaseData data = request.getHBaseData();
        repository.addData(data);
    }


    public Object getData(GetDataRequest request) throws IOException, ClassNotFoundException {
        HBaseData hbaseData = request.getHBaseData();
        byte[] byteArray = repository.getData(hbaseData);
        return mapper.readValue(byteArray, Object.class);
    }

    public Object scanTable(ScanTableRequest request) throws IOException {
        Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(request.getRegexPattern()));
        List<Result> resultList = repository.scanTable(request.getNamespace(),request.getTableName(),request.getLimit(), filter);
        return resultList.stream().map(r -> {
           byte[] b = r.getValue(Bytes.toBytes(request.getColumnFamily()),Bytes.toBytes( request.getColumnQualifier()));
            try {
                return mapper.readValue(b, Object.class);
            } catch (IOException e) {
                return null;
            }
        }).filter(Objects::nonNull).collect(Collectors.toList());
    }

    public boolean bulkPut(BulkPutDto bulkPutDto){
        return repository.bulkPut(bulkPutDto);
    }

    private NavigableMap<byte[], NavigableMap<byte[],byte[]>> getRow(GetRowDto getRowDto) throws IOException {
        return repository.getRow(getRowDto.getNamespace(), getRowDto.getTableName(), getRowDto.getRow());

    }

    public Map<String, Map<String,Object> > getRowValue(GetRowDto getRowDto) throws IOException {
        NavigableMap<byte[], NavigableMap<byte[],byte[]>> nMap = getRow(getRowDto);
        Map<String, Map<String,Object> > resultMap = new HashMap<>();
        for(byte[] cf : nMap.keySet()){
            String columnFamily = Bytes.toString(cf);
            NavigableMap<byte[],byte[]> columnFamilyMap = nMap.get(cf);
            HashMap<String, Object> resultCfMap = new HashMap<>();
            for (byte[] cq : columnFamilyMap.keySet()){
                String columnQualifier = Bytes.toString(cq);
                byte[] objectBytes = columnFamilyMap.get(cq);
                Object obj = mapper.readValue(objectBytes, Object.class);
                resultCfMap.put(columnQualifier,obj);
            }
            resultMap.put(columnFamily, resultCfMap);
        }

        return resultMap;
    }

    public boolean containsColumns(ContainsColumnDto containsColumnDto) throws IOException {
        return repository.containsColumns(containsColumnDto.getNamespace(), containsColumnDto.getTableName(), containsColumnDto.getRow(), containsColumnDto.getColumnFamily(), containsColumnDto.getColumnQualifier());
    }

    public List<Result> scanTable(ScanTableDto scanTableDto) throws IOException {
        return repository.scanTable(scanTableDto);
    }

}
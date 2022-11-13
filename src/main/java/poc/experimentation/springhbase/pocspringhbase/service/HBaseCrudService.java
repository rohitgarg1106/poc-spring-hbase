package poc.experimentation.springhbase.pocspringhbase.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.shaded.org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import poc.experimentation.springhbase.pocspringhbase.ab.HbaseUtils;
import poc.experimentation.springhbase.pocspringhbase.ab.ScanTableRequest;
import poc.experimentation.springhbase.pocspringhbase.exception.HBaseTableExistsException;
import poc.experimentation.springhbase.pocspringhbase.model.HBaseData;
import poc.experimentation.springhbase.pocspringhbase.repository.HBaseCrudRepository;
import poc.experimentation.springhbase.pocspringhbase.request.BulkPutDto;
import poc.experimentation.springhbase.pocspringhbase.request.CreateMapStoreRequest;
import poc.experimentation.springhbase.pocspringhbase.request.GetDataRequest;
import poc.experimentation.springhbase.pocspringhbase.request.PutDataRequest;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
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

    public boolean bulkPut(BulkPutDto dto){
        return repository.bulkPut(dto);
    }

}

//getrow
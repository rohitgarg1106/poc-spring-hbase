package poc.experimentation.springhbase.pocspringhbase.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.shaded.org.apache.avro.reflect.AvroIgnore;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import poc.experimentation.springhbase.pocspringhbase.model.HBaseConnection;
import poc.experimentation.springhbase.pocspringhbase.repository.CRUDRepository;
import poc.experimentation.springhbase.pocspringhbase.repository.CRUDRepositoryImpl;
import poc.experimentation.springhbase.pocspringhbase.request.PutDataRequest;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;


@Service
public class CRUDService {

    @Autowired
    private CRUDRepositoryImpl crudRepository;

    @Autowired
    private ObjectMapper mapper;

    public List<String> listTables() throws IOException {
       return crudRepository.listTables();
    }

    public void createDefaultTable() throws IOException {
     crudRepository.createDefaultTable();

    }

    public void addData(PutDataRequest request) throws IOException {
       crudRepository.addData(request);
    }
}

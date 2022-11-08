package poc.experimentation.springhbase.pocspringhbase.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.hbase.thirdparty.com.google.gson.JsonObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import poc.experimentation.springhbase.pocspringhbase.exception.HBaseTableExistsException;
import poc.experimentation.springhbase.pocspringhbase.model.HBaseData;
import poc.experimentation.springhbase.pocspringhbase.repository.CRUDRepository;
import poc.experimentation.springhbase.pocspringhbase.request.CreateMapStoreRequest;
import poc.experimentation.springhbase.pocspringhbase.request.GetDataRequest;
import poc.experimentation.springhbase.pocspringhbase.request.PutDataRequest;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.List;


@Service
public class CRUDService {

    @Autowired
    private CRUDRepository crudRepository;

    @Autowired
    private ObjectMapper mapper;

    public List<String> listTables(String namespace) throws IOException {
       return crudRepository.listTableNames(namespace);
    }

    public void createTable(CreateMapStoreRequest request) throws IOException, HBaseTableExistsException {

        crudRepository.createTable(request.getNamespace(), request.getTableName(), request.getCf());
    }

    public void addData(PutDataRequest request) throws IOException {
        HBaseData data = request.getHBaseData();
        crudRepository.addData(data);
    }


    public Object getData(GetDataRequest request) throws IOException, ClassNotFoundException {
        HBaseData hbaseData = request.getHBaseData();
        byte[] byteArray = crudRepository.getData(hbaseData);
        return mapper.readValue(byteArray, Object.class);
    }
}
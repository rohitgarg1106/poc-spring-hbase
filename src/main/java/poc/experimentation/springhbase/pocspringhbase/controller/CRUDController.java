package poc.experimentation.springhbase.pocspringhbase.controller;

import org.apache.hadoop.hbase.client.Connection;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class CRUDController {

    @Autowired
    private Connection connection;

    @GetMapping(value = "/api/v1/getAllTables")
    public ResponseEntity getAllTables(){
        return ResponseEntity.ok(connection);
    }

}

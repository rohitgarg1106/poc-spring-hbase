package poc.experimentation.springhbase.pocspringhbase.controller;

import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import poc.experimentation.springhbase.pocspringhbase.request.ScanTableRequest;
import poc.experimentation.springhbase.pocspringhbase.request.CreateMapStoreRequest;
import poc.experimentation.springhbase.pocspringhbase.request.GetDataRequest;
import poc.experimentation.springhbase.pocspringhbase.request.PutDataRequest;
import poc.experimentation.springhbase.pocspringhbase.service.HBaseCrudService;

import java.io.IOException;

@RestController
public class HBaseCrudController {

    private HBaseCrudService service;

    public HBaseCrudController(HBaseCrudService service) {
        this.service = service;
    }

    @GetMapping(value = "/api/v1/getAllTables", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity listTables(
            @RequestParam String namespace
    ) {
        try {
            return ResponseEntity.ok().body(service.listTables(namespace));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().build();

        }
    }

    @PostMapping(value = "/api/v1/table/createTable", produces = MediaType.APPLICATION_JSON_VALUE,  consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity createTable(
        @RequestBody CreateMapStoreRequest request
    ) {
        try {
            service.createTable(request);
            return ResponseEntity.ok().build();
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(e.getMessage());

        }
    }

    @PostMapping(value = "/api/v1/data/putData", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity addData(
            @RequestBody PutDataRequest request
    ) {
        try {
            service.addData(request);
            return ResponseEntity.ok().build();
        } catch (Exception e) {
            return ResponseEntity.internalServerError().build();

        }
    }

    @GetMapping(value = "/api/v1/data/getData", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity getData(
            @RequestBody GetDataRequest request
    ) throws IOException, ClassNotFoundException {
        return ResponseEntity.ok().body(service.getData(request));
    }

    @GetMapping(value = "/api/v1/data/scanTable", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity scanTable(
            @RequestBody ScanTableRequest request
            ){
        try {
            return ResponseEntity.ok().body(service.scanTable(request));
        } catch (IOException e) {
            return ResponseEntity.internalServerError().build();
        }
    }


}

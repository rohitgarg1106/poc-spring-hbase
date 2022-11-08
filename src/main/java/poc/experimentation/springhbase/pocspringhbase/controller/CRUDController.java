package poc.experimentation.springhbase.pocspringhbase.controller;

import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import poc.experimentation.springhbase.pocspringhbase.request.CreateMapStoreRequest;
import poc.experimentation.springhbase.pocspringhbase.request.GetDataRequest;
import poc.experimentation.springhbase.pocspringhbase.request.PutDataRequest;
import poc.experimentation.springhbase.pocspringhbase.service.CRUDService;

import java.io.IOException;

@RestController
public class CRUDController {

    private CRUDService crudService;

    public CRUDController(CRUDService crudService) {
        this.crudService = crudService;
    }

    @GetMapping(value = "/api/v1/getAllTables", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity listTables(
            @RequestParam String namespace
    ) {
        try {
            return ResponseEntity.ok().body(crudService.listTables(namespace));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().build();

        }
    }

    @PostMapping(value = "/api/v1/table/createTable", produces = MediaType.APPLICATION_JSON_VALUE,  consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity createTable(
        @RequestBody CreateMapStoreRequest request
    ) {
        try {
            crudService.createTable(request);
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
            crudService.addData(request);
            return ResponseEntity.ok().build();
        } catch (Exception e) {
            return ResponseEntity.internalServerError().build();

        }
    }

    @GetMapping(value = "/api/v1/data/getData", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity getData(
            @RequestBody GetDataRequest request
    ) throws IOException, ClassNotFoundException {
        return ResponseEntity.ok().body(crudService.getData(request));
    }


}

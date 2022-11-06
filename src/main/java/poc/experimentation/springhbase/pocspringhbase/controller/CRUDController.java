package poc.experimentation.springhbase.pocspringhbase.controller;

import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
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
    public ResponseEntity listTables() {
        try {
            return ResponseEntity.ok().body(crudService.listTables());
        } catch (Exception e) {
            return ResponseEntity.internalServerError().build();

        }
    }

    @PostMapping(value = "/api/v1/createDefaultTable", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity createDefaultTable() {
        try {
            crudService.createDefaultTable();
            return ResponseEntity.ok().build();
        } catch (Exception e) {
            return ResponseEntity.internalServerError().build();

        }
    }

    @PostMapping(value = "/api/v1/data/addData", produces = MediaType.APPLICATION_JSON_VALUE)
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


}

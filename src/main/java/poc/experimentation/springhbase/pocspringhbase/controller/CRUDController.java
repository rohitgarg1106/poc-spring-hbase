package poc.experimentation.springhbase.pocspringhbase.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import poc.experimentation.springhbase.pocspringhbase.service.CRUDService;

import java.io.IOException;

@RestController
public class CRUDController {

    private CRUDService crudService;

    public CRUDController(CRUDService crudService) {
        this.crudService = crudService;
    }

    @GetMapping(value = "/api/v1/getAllTables")
    public ResponseEntity listTables() {
        try {
            crudService.listTables();
            return ResponseEntity.ok().build();
        }
        catch (Exception e){
            return ResponseEntity.internalServerError().build();

        }
    }

}

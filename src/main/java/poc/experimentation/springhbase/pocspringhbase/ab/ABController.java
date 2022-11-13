package poc.experimentation.springhbase.pocspringhbase.ab;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
public class ABController {

    @Autowired
    private ABService abService;

    @GetMapping(value = "/api/v2/ab/doesMappingExists", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity doesMappingExists(
            @RequestParam String audienceId,
            @RequestParam String entityId
    ) {
        boolean mappingExists = false;

        try {
            mappingExists = abService.doesMappingExists(audienceId, entityId);
            return ResponseEntity.ok().body(mappingExists);
        } catch (Exception exception) {
            return ResponseEntity.internalServerError().body(exception.getMessage());
        }
    }

    @PostMapping(value = "/api/v2/ab/createAudienceEntityMap", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity createAudienceEntityMap(
            @RequestBody CreateAudienceEntityRequest request
    ) {
        try {
            abService.createAudienceEntityMap(request);
            return ResponseEntity.ok().build();
        } catch (Exception exception) {
            return ResponseEntity.internalServerError().body(exception.getMessage());
        }
    }

    @GetMapping(value = "/api/v2/ab/entity/{entityId}/audiences", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity getAudiencesForEntity(
            @PathVariable String entityId,
            @RequestParam String entityType
    ) {
        try {
            return ResponseEntity.ok().body(abService.getAudiencesForEntity(entityId, entityType));
        } catch (Exception exception) {
            return ResponseEntity.internalServerError().body(exception.getMessage());
        }
    }

    @GetMapping(value = "/api/v2/ab/audience/{audienceId}/entities", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity getEntitiesForAudience(
            @PathVariable Integer audienceId,
            @RequestParam String entityType
    ) {
        try {
            return ResponseEntity.ok().body(abService.getEntitiesForAudience(audienceId, entityType));
        } catch (Exception exception) {
            return ResponseEntity.internalServerError().body(exception.getMessage());
        }
    }


}

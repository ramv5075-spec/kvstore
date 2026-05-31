package com.kvstore.api;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.Optional;

@RestController
@RequestMapping("/api")
@CrossOrigin(origins = "*")
public class KVController {

    private final KVStoreService service;

    public KVController(KVStoreService service) {
        this.service = service;
    }

    @PutMapping("/keys/{key}")
    public ResponseEntity<Map<String, Object>> put(
            @PathVariable String key,
            @RequestBody Map<String, String> body) {
        String value = body.get("value");
        if (value == null) {
            return ResponseEntity.badRequest()
                .body(Map.of("success", false, "error", "value is required"));
        }
        boolean success = service.put(key, value);
        return ResponseEntity.ok(Map.of(
            "success", success,
            "key", key,
            "value", value
        ));
    }

    @GetMapping("/keys/{key}")
    public ResponseEntity<Map<String, Object>> get(@PathVariable String key) {
        Optional<String> value = service.get(key);
        if (value.isEmpty()) {
            return ResponseEntity.status(404)
                .body(Map.of("success", false, "error", "key not found"));
        }
        return ResponseEntity.ok(Map.of(
            "success", true,
            "key", key,
            "value", value.get()
        ));
    }

    @DeleteMapping("/keys/{key}")
    public ResponseEntity<Map<String, Object>> delete(@PathVariable String key) {
        boolean success = service.delete(key);
        return ResponseEntity.ok(Map.of(
            "success", success,
            "key", key
        ));
    }

    @GetMapping("/cluster/status")
    public ResponseEntity<KVStoreService.ClusterStatus> status() {
        return ResponseEntity.ok(service.getClusterStatus());
    }

    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> health() {
        return ResponseEntity.ok(Map.of("status", "UP"));
    }
}

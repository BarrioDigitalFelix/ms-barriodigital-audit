package com.barriodigital.demo.controller;

import com.barriodigital.demo.model.AuditEvent;
import com.barriodigital.demo.repository.AuditEventRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/audit")
@CrossOrigin(origins = "*")
public class AuditController {

    @Autowired
    private AuditEventRepository repository;

    @GetMapping
    public ResponseEntity<List<AuditEvent>> getAllEvents() {
        return ResponseEntity.ok(repository.findAllByOrderByEventTimestampDesc());
    }

    @GetMapping("/request/{requestId}")
    public ResponseEntity<List<AuditEvent>> getEventsByRequestId(@PathVariable Long requestId) {
        return ResponseEntity.ok(repository.findByRequestIdOrderByEventTimestampDesc(requestId));
    }
}
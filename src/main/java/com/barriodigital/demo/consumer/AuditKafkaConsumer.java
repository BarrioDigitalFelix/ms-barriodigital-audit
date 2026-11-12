package com.barriodigital.demo.consumer;

import com.barriodigital.demo.dto.KafkaAuditEventDto;
import com.barriodigital.demo.model.AuditEvent;
import com.barriodigital.demo.repository.AuditEventRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

@Service
public class AuditKafkaConsumer {

    @Autowired
    private AuditEventRepository repository;

    @KafkaListener(topics = "audit.timeline", groupId = "audit-group")
    public void consumeAuditEvent(KafkaAuditEventDto event, Acknowledgment acknowledgment) {
        try {
            if (event.getEventId() != null && repository.existsByEventId(event.getEventId())) {
                System.out.println("[ms-audit] Evento duplicado ignorado (Idempotencia). EventId: " + event.getEventId());
                acknowledgment.acknowledge();
                return;
            }

            LocalDateTime eventTime = (event.getTimestamp() != null)
                    ? LocalDateTime.ofInstant(Instant.ofEpochMilli(event.getTimestamp()), ZoneId.systemDefault())
                    : LocalDateTime.now();

            String eventType = (event.getOldStatus() == null) ? "CREACIÓN_TRÁMITE" : "CAMBIO_ESTADO";

            AuditEvent audit = AuditEvent.builder()
                    .eventId(event.getEventId())
                    .requestId(event.getRequestId())
                    .oldStatus(event.getOldStatus())
                    .newStatus(event.getNewStatus())
                    .citizenId(event.getCitizenId())
                    .eventType(eventType)
                    .eventTimestamp(eventTime)
                    .build();

            repository.save(audit);
            acknowledgment.acknowledge();
            System.out.println("[ms-audit] Evento auditado y confirmado (Commit Sync) para trámite ID: " + event.getRequestId());
        } catch (Exception e) {
            System.err.println("[ms-audit] Error al procesar evento de auditoría (Offset NO confirmado): " + e.getMessage());
        }
    }
}
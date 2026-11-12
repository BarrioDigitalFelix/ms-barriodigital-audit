package com.barriodigital.demo.repository;

import com.barriodigital.demo.model.AuditEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;

public interface AuditEventRepository extends JpaRepository<AuditEvent, Long> {
    boolean existsByEventId(String eventId);
    List<AuditEvent> findByRequestIdOrderByEventTimestampDesc(Long requestId);
    List<AuditEvent> findAllByOrderByEventTimestampDesc();
}
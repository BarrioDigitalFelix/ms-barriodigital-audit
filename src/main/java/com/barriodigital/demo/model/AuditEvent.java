package com.barriodigital.demo.model;

import jakarta.persistence.*;
import lombok.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "audit_events")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class AuditEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private Long requestId;

    private String oldStatus;

    @Column(nullable = false)
    private String newStatus;

    private String citizenId;

    private String eventType;

    @Column(nullable = false)
    private LocalDateTime eventTimestamp;
}
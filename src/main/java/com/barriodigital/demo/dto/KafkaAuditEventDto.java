package com.barriodigital.demo.dto;

import lombok.Data;

@Data
public class KafkaAuditEventDto {
    private String eventId;
    private Long requestId;
    private String oldStatus;
    private String newStatus;
    private String citizenId;
    private Long timestamp;
}
package com.barriodigital.demo.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class MunicipalAlertDto {
    private String eventId;
    private Long requestId;
    private String citizenId;
    private String statusTransition;
    private String reason;
    private String severity; // "ALTA", "MEDIA"
    private Long detectedAt;
}
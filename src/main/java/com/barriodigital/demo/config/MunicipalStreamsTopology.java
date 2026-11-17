package com.barriodigital.demo.config;

import com.barriodigital.demo.dto.KafkaAuditEventDto;
import com.barriodigital.demo.dto.MunicipalAlertDto;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JsonSerde;

@Configuration
@EnableKafkaStreams
public class MunicipalStreamsTopology {

    @Bean
    public KStream<String, KafkaAuditEventDto> municipalAlertPipeline(StreamsBuilder builder) {
        JsonSerde<KafkaAuditEventDto> auditEventSerde = new JsonSerde<>(KafkaAuditEventDto.class);
        auditEventSerde.deserializer().addTrustedPackages("*");
        auditEventSerde.deserializer().setUseTypeHeaders(false);

        JsonSerde<MunicipalAlertDto> alertSerde = new JsonSerde<>(MunicipalAlertDto.class);

        // PASO 1: LEER (Source) desde el tópico audit.timeline
        KStream<String, KafkaAuditEventDto> streamEntrada = builder.stream(
                "audit.timeline",
                Consumed.with(Serdes.String(), auditEventSerde)
        );

        // PASO 2: FILTRAR eventos válidos y trazar por consola
        KStream<String, KafkaAuditEventDto> eventosValidos = streamEntrada
                .filter((key, event) -> event != null && event.getRequestId() != null && event.getNewStatus() != null)
                .peek((key, event) -> System.out.println(
                        "[Kafka Streams] Evaluando flujo de trámite ID #" + event.getRequestId() +
                        " (" + event.getOldStatus() + " -> " + event.getNewStatus() + ")"
                ));

        // PASO 3: DETECTAR hitos críticos o irregularidades administrativas y transformar a Alerta
        KStream<String, MunicipalAlertDto> alertasMunicipales = eventosValidos
                .filter((key, event) -> esEventoDeAlerta(event))
                .mapValues(this::construirAlertaMunicipal);

        // PASO 4: ESCRIBIR (Sink) hacia el tópico municipal.alerts e imprimir alerta en consola
        alertasMunicipales
                .peek((key, alerta) -> System.out.println(
                        "[Kafka Streams - ALERTA " + alerta.getSeverity() + "] Trámite #" +
                        alerta.getRequestId() + " | Motivo: " + alerta.getReason()
                ))
                .to("municipal.alerts", Produced.with(Serdes.String(), alertSerde));

        return streamEntrada;
    }

    private boolean esEventoDeAlerta(KafkaAuditEventDto event) {
        String newStatus = event.getNewStatus();
        String oldStatus = event.getOldStatus();

        // Caso 1: Salto irregular de flujo (llega a EN_TERRENO o RESUELTO sin haber pasado por ADMITIDO)
        boolean saltoIrregular = ("EN_TERRENO".equals(newStatus) || "RESUELTO".equals(newStatus))
                && !"ADMITIDO".equals(oldStatus) && !"EN_TERRENO".equals(oldStatus);

        // Caso 2: Rechazo de trámite ciudadano (requiere revisión de contraloría)
        boolean tramiteRechazado = "RECHAZADO".equals(newStatus);

        // Caso 3: Resolución final de trámite (hito de cierre operativo)
        boolean tramiteResuelto = "RESUELTO".equals(newStatus);

        return saltoIrregular || tramiteRechazado || tramiteResuelto;
    }

    private MunicipalAlertDto construirAlertaMunicipal(KafkaAuditEventDto event) {
        String newStatus = event.getNewStatus();
        String oldStatus = event.getOldStatus();
        String transicion = (oldStatus != null ? oldStatus : "NUEVO") + " -> " + newStatus;

        boolean saltoIrregular = ("EN_TERRENO".equals(newStatus) || "RESUELTO".equals(newStatus))
                && !"ADMITIDO".equals(oldStatus) && !"EN_TERRENO".equals(oldStatus);

        if (saltoIrregular) {
            return new MunicipalAlertDto(
                    event.getEventId(),
                    event.getRequestId(),
                    event.getCitizenId(),
                    transicion,
                    "Salto irregular de etapa administrativa sin admisión previa",
                    "ALTA",
                    System.currentTimeMillis()
            );
        }

        if ("RECHAZADO".equals(newStatus)) {
            return new MunicipalAlertDto(
                    event.getEventId(),
                    event.getRequestId(),
                    event.getCitizenId(),
                    transicion,
                    "Solicitud ciudadana rechazada - Requiere validación de causal",
                    "ALTA",
                    System.currentTimeMillis()
            );
        }

        return new MunicipalAlertDto(
                event.getEventId(),
                event.getRequestId(),
                event.getCitizenId(),
                transicion,
                "Trámite resuelto exitosamente - Listo para emisión de certificado",
                "MEDIA",
                System.currentTimeMillis()
        );
    }
}
package br.insper.aposta.aposta;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.LocalDate;
import java.time.LocalDateTime;

@Document
@Getter
@Setter
public class Aposta {

    @Id
    private String id;
    private Integer idPartida;
    private LocalDateTime dataAposta;
    private String resultado;  // EMPATE, VITORIA_MANDANTE, VITORIA_VISITANTE
    private Double valor;

    private String status; // GANHOU, PERDEU, REALIZADA

}

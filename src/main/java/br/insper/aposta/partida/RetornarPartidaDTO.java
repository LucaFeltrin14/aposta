package br.insper.aposta.partida;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class RetornarPartidaDTO {
    private String nomeMandante;
    private String nomeVisitante;
    private Integer placarMandante;
    private Integer placarVisitante;
    private String status;

    public boolean isEmpate() {
        return placarMandante == placarVisitante;
    }

    public boolean isVitoriaMandante() {
        return placarMandante > placarVisitante;
    }

    public boolean isVitoriaVisitante() {
        return placarVisitante > placarMandante;
    }
}

package br.insper.aposta.partida;

public class PartidaNaoRealizadaException extends RuntimeException {
    public PartidaNaoRealizadaException(String message) {
        super(message);
    }
}

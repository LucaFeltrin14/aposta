package br.insper.aposta.partida;

public class PartidaNaoEncontradaException extends RuntimeException {
    public PartidaNaoEncontradaException(String message) {
        super(message);
    }
}

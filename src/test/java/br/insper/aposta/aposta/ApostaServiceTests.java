package br.insper.aposta.aposta;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;

@ExtendWith(MockitoExtension.class)
public class ApostaServiceTests {

    @InjectMocks
    ApostaService apostaService;

    @Mock
    ApostaRepository apostaRepository;

    @Test
    public void testGetApostaWhenApostaIsNull() {

        Mockito.when(apostaRepository.findById("1"))
                .thenReturn(Optional.empty());

        Assertions.assertThrows(ApostaNaoEncontradaException.class,
                () -> apostaService.getAposta("1"));
    }

    @Test
    public void testGetApostaWhenApostaIsNotNullStatusRealizada() {

        Aposta aposta = new Aposta();
        aposta.setStatus("GANHOU");

        Mockito.when(apostaRepository.findById("1"))
                .thenReturn(Optional.of(aposta));

        Aposta apostaRetorno = apostaService.getAposta("1");
        Assertions.assertNotNull(apostaRetorno);

    }


}

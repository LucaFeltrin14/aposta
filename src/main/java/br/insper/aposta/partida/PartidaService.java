package br.insper.aposta.partida;

import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
public class PartidaService {

    public ResponseEntity<RetornarPartidaDTO> getPartida(Integer idPartida) {
        RestTemplate restTemplate = new RestTemplate();
        return restTemplate.getForEntity(
                "http://campeonato:8080/partida/" + idPartida,
                RetornarPartidaDTO.class);
    }

}

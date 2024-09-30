package br.insper.aposta.aposta;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ApostaRepository extends MongoRepository<Aposta, String> {

    List<Aposta> findByResultado(String resultado);
}

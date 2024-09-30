package br.insper.aposta;

import br.insper.aposta.aposta.Aposta;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.web.client.RestTemplate;

@SpringBootApplication
public class ApostaApplication {

	public static void main(String[] args) {
		SpringApplication.run(ApostaApplication.class, args);
	}


}

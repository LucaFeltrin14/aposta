package br.insper.aposta.aposta;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/aposta")
public class ApostaController {

    @Autowired
    private ApostaService apostaService;

    @GetMapping
    public List<Aposta> listar() {
        return apostaService.listar();
    }

    @PostMapping
    public Aposta salvar(@RequestBody Aposta aposta) {
        return apostaService.salvar(aposta);
    }

    @GetMapping("/{idAposta}")
    public Aposta getAposta(@PathVariable String idAposta) {
        return apostaService.getAposta(idAposta);
    }
}

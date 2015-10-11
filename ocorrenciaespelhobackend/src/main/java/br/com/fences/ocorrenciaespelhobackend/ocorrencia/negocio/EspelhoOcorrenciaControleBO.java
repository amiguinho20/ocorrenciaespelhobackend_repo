package br.com.fences.ocorrenciaespelhobackend.ocorrencia.negocio;

import java.util.Set;

import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;

import br.com.fences.ocorrenciaentidade.controle.ControleOcorrencia;
import br.com.fences.ocorrenciaespelhobackend.ocorrencia.dao.EspelhoOcorrenciaControleDAO;

/**
 * Business Object
 *
 */
@RequestScoped
public class EspelhoOcorrenciaControleBO {

	@Inject
	private EspelhoOcorrenciaControleDAO espelhoOcorrenciaControleDAO;

	public String pesquisarUltimaDataRegistroNaoComplementar() 
	{
		String ultimaDataRegistro = "";
		ultimaDataRegistro = espelhoOcorrenciaControleDAO.pesquisarUltimaDataRegistroNaoComplementar();
		return ultimaDataRegistro;
	}

	public void adicionar(ControleOcorrencia controleOcorrencia) {
		espelhoOcorrenciaControleDAO.adicionar(controleOcorrencia);
	}

	public Set<ControleOcorrencia> pesquisarProcessarReprocessar() {
		Set<ControleOcorrencia> controleOcorrencias = null;
		controleOcorrencias = espelhoOcorrenciaControleDAO.pesquisarProcessarReprocessar();
		return controleOcorrencias;
	}

	public Set<ControleOcorrencia> pesquisarIndiciadosProcessarReprocessar() {
		Set<ControleOcorrencia> controleOcorrencias = null;
		controleOcorrencias = espelhoOcorrenciaControleDAO.pesquisarIndiciadosProcessarReprocessar();
		return controleOcorrencias;
	}

	
	public void substituir(ControleOcorrencia controleOcorrencia) {
		espelhoOcorrenciaControleDAO.substituir(controleOcorrencia);
	}

}

package br.com.fences.ocorrenciaespelhobackend.ocorrencia.negocio;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
	
	public String pesquisarProcessarReprocessarDataInicial() {
		String dataInicial = espelhoOcorrenciaControleDAO.pesquisarProcessarReprocessarDataInicial();
		return dataInicial;
	}

	public String pesquisarProcessarReprocessarDataFinal() {
		String dataFinal = espelhoOcorrenciaControleDAO.pesquisarProcessarReprocessarDataFinal();
		return dataFinal;

	}
	
	public Set<ControleOcorrencia> pesquisarProcessarReprocessar(String dataInicial, String dataFinal) {
		Set<ControleOcorrencia> controleOcorrencias = null;
		controleOcorrencias = espelhoOcorrenciaControleDAO.pesquisarProcessarReprocessar(dataInicial, dataFinal);
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

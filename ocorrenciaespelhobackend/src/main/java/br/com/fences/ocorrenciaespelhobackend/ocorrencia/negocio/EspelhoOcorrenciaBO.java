package br.com.fences.ocorrenciaespelhobackend.ocorrencia.negocio;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;

import br.com.fences.fencesutils.filtrocustom.ArvoreSimples;
import br.com.fences.fencesutils.filtrocustom.FiltroCondicao;
import br.com.fences.fencesutils.verificador.Verificador;
import br.com.fences.ocorrenciaentidade.ocorrencia.Ocorrencia;
import br.com.fences.ocorrenciaentidade.ocorrencia.natureza.Natureza;
import br.com.fences.ocorrenciaespelhobackend.ocorrencia.dao.EspelhoOcorrenciaDAO;

/**
 * Business Object
 *
 */
@RequestScoped
public class EspelhoOcorrenciaBO {
	
	@Inject
	private EspelhoOcorrenciaDAO espelhoOcorrenciaDAO;
	
	public Ocorrencia adicionar(Ocorrencia ocorrencia)
	{
		boolean existe = espelhoOcorrenciaDAO.isExisteNoBanco(ocorrencia);
		if (existe)
		{
			//-- recupera o ID
			Ocorrencia ocorrenciaConsultada = espelhoOcorrenciaDAO.consultar(ocorrencia);
			ocorrencia.setId(ocorrenciaConsultada.getId());
		}

		try
		{
			if (existe)
			{
				espelhoOcorrenciaDAO.substituir(ocorrencia);
			}
			else
			{
				ocorrencia = espelhoOcorrenciaDAO.adicionar(ocorrencia);
			}

			
			boolean atualizaRegistro = false;
			//--
			Set<Ocorrencia> filhos = new LinkedHashSet<>();
			filhos = espelhoOcorrenciaDAO.consultarFilhos(ocorrencia);
			
			for (Ocorrencia filho : filhos)
			{
				if (filho.getAuxiliar().getPai() == null)
				{
					filho.getAuxiliar().setPai(ocorrencia);
					espelhoOcorrenciaDAO.substituir(filho);
					ocorrencia.getAuxiliar().getFilhos().add(filho);
					atualizaRegistro = true;
				}
				else
				{
					Ocorrencia pai = filho.getAuxiliar().getPai();
					if (ocorrencia.getId().equals(pai.getId()))
					{
						//-- o id do pai no banco condiz com o id do registro atual... 
						//-- mantem a referencia
						ocorrencia.getAuxiliar().getFilhos().add(filho);
						atualizaRegistro = true;
					}
					else
					{
						String msg = "A ocorrencia[" + ocorrencia.getId() + "] possui um filho com um pai[" + pai.getId() + "] de ID diferente.";
						throw new RuntimeException(msg);
					}
				}
			}
			
			//--
			Ocorrencia pai = null;
			if (Verificador.isValorado(ocorrencia.getAnoReferenciaBo()))
			{
				//-- referencia cruzada
				pai = espelhoOcorrenciaDAO.consultarPai(ocorrencia);
				if (pai != null)
				{
					//-- adiciona o PAI no FILHO
					ocorrencia.getAuxiliar().setPai(pai);

					//-- adiciona o FILHO no PAI
					pai.getAuxiliar().getFilhos().add(ocorrencia);

					//-- verifica se registro inserido(filho) possui natureza de localizacao, caso sim, sinaliza o pai
					for (Natureza natureza : ocorrencia.getNaturezas())
					{
						String idOcorrencia = natureza.getIdOcorrencia();
						String idEspecie = natureza.getIdEspecie();
						if (Verificador.isValorado(idOcorrencia, idEspecie))
						{
							if (idOcorrencia.equals("40") && idEspecie.equals("40"))
							{
								pai.getAuxiliar().setFlagComplementarDeNaturezaLocalizacao("S");
							}
						}
					}
					espelhoOcorrenciaDAO.substituir(pai);
					atualizaRegistro = true;
				}
			}
			
			if (atualizaRegistro)
			{
				espelhoOcorrenciaDAO.substituir(ocorrencia);
			}
			
		}
		catch (Exception e)
		{
			String msg = "Erro na regra de adicao e associacoes. num[" + ocorrencia.getNumBo() + "] ano["
					+ ocorrencia.getAnoBo() + "] dlg[" + ocorrencia.getIdDelegacia() + "/"
					+ ocorrencia.getNomeDelegacia() + "] dtReg[" + ocorrencia.getDatahoraRegistroBo() + "] "
					+ "err[" + e.getMessage() + "].";
			throw new RuntimeException(msg);
		}	
		
		return ocorrencia;
		
	}
	
	public Map<String, Integer> agregarPorAno(Map<String, String> filtros)
	{
		return espelhoOcorrenciaDAO.agregarPorAno(filtros);
	}
	
	public Map<String, Integer> agregarPorComplementar(Map<String, String> filtros)
	{
		return espelhoOcorrenciaDAO.agregarPorComplementar(filtros);
	}
	
	public Map<String, Integer> agregarPorFlagrante(Map<String, String> filtros)
	{
		return espelhoOcorrenciaDAO.agregarPorFlagrante(filtros);
	}
	
	public Ocorrencia consultar(String id)
	{
		return espelhoOcorrenciaDAO.consultar(id);
	}

	public int contar(Map<String, String> filtros)
	{
		return espelhoOcorrenciaDAO.contar(filtros);
	}
	
	public int contarDinamico(List<FiltroCondicao> filtroCondicoes)
	{
		return espelhoOcorrenciaDAO.contarDinamico(filtroCondicoes);
	}
	
	public List<Ocorrencia> pesquisarLazy(Map<String, String> filtros, int primeiroRegistro, int registrosPorPagina)
	{
		return espelhoOcorrenciaDAO.pesquisarLazy(filtros, primeiroRegistro, registrosPorPagina);
	}
	
	public List<Ocorrencia> pesquisarDinamicoLazy(List<FiltroCondicao> filtroCondicoes, int primeiroRegistro,
			int registrosPorPagina) {
		
		return espelhoOcorrenciaDAO.pesquisarDinamicoLazy(filtroCondicoes, primeiroRegistro, registrosPorPagina);
	}
	
	public String pesquisarUltimaDataRegistroNaoComplementar()
	{
		return espelhoOcorrenciaDAO.pesquisarUltimaDataRegistroNaoComplementar();
	}
	
	public void substituir(Ocorrencia ocorrencia)
	{
		espelhoOcorrenciaDAO.substituir(ocorrencia);
	}
	
	public String pesquisarPrimeiraDataRegistro()
	{
		return espelhoOcorrenciaDAO.pesquisarPrimeiraDataRegistro();
	}
	
	public String pesquisarUltimaDataRegistro()
	{
		return espelhoOcorrenciaDAO.pesquisarUltimaDataRegistro();
	}

	public List<String> listarAnos()
	{
		return espelhoOcorrenciaDAO.listarAnos();
	}
	
	public Map<String, String> listarAnosMap()
	{
		List<String> lista = espelhoOcorrenciaDAO.listarAnos();
		Map<String, String> map = new LinkedHashMap<>();
		for (String valor : lista)
		{
			map.put(valor, valor);
		}
		return map;
	}
	
	public Map<String, String> listarDelegacias()
	{
		return espelhoOcorrenciaDAO.listarDelegacias();
	}
	
	public Map<String, String> listarNaturezas()
	{
		return espelhoOcorrenciaDAO.listarNaturezas();
	}
	
	public ArvoreSimples listarNaturezasArvore()
	{
		ArvoreSimples arvore = new ArvoreSimples();
		arvore.setChave("0");
		arvore.setValor("Raiz - naturezas");
		
		Map<String, String> mapNaturezas = espelhoOcorrenciaDAO.listarNaturezas();
		
		for (Map.Entry<String, String> entry : mapNaturezas.entrySet())
		{
			String chaveComposta[] = entry.getKey().split("\\|");
			String valorComposto[] = entry.getValue().split(">");
			
			int nivel = 0;
			
			alimentarArvore(arvore, nivel, chaveComposta, valorComposto);
		}
		
		return arvore;
	}
	
	public ArvoreSimples listarNaturezasArvoreComDesdobramentoCircunstancia()
	{
		ArvoreSimples arvore = new ArvoreSimples();
		arvore.setChave("0");
		arvore.setValor("Raiz - naturezas");
		
		Map<String, String> mapNaturezas = espelhoOcorrenciaDAO.listarNaturezasComDesdobramentoCircunstancia();
		
		for (Map.Entry<String, String> entry : mapNaturezas.entrySet())
		{
			String chaveComposta[] = entry.getKey().split("\\|");
			String valorComposto[] = entry.getValue().split(">");
			
			int nivel = 0;
			
			alimentarArvore(arvore, nivel, chaveComposta, valorComposto);
		}
		
		return arvore;
	}
	
	/**
	 * RECURSIVO!
	 * 
	 * Navega na arvore conforme o nivel de profundidade e insere o elemento na hierarquia e nivel adequados.
	 * @param arvore
	 * @param nivel
	 * @param chaveComposta
	 * @param valorComposto
	 */
	private void alimentarArvore(ArvoreSimples arvore, int nivel, String[] chaveComposta, String[] valorComposto)
	{
		if (nivel < chaveComposta.length)
		{
			String chave = montarChaveHierarquica(chaveComposta, nivel);
			String valor = valorComposto[nivel].trim();
			ArvoreSimples filho = recuperarElemento(arvore, chave, valor);
			if (filho == null)
			{
				filho = new ArvoreSimples(chave, valor);
				arvore.getFilhos().add(filho);
			}
			nivel++; 
			alimentarArvore(filho, nivel, chaveComposta, valorComposto);
		
		}
	}
	
	private ArvoreSimples recuperarElemento(ArvoreSimples arvore, String chave, String valor)
	{
		ArvoreSimples elementoRetorno = null;
		if (arvore.getFilhos().contains(new ArvoreSimples(chave, valor)))
		{
			for (ArvoreSimples elemento : arvore.getFilhos())
			{
				if (elemento.getChave().equals(chave))
				{
					elementoRetorno = elemento;
				}
			}
		}
		return elementoRetorno;
	}
	
	private String montarChaveHierarquica(String[] chaves, int nivel)
	{
		StringBuffer chaveHierarquica = new StringBuffer();
		
		if (chaves != null && chaves.length > 0)
		{
			for (int indice = 0; indice <= nivel; indice++)
			{
				String chave = chaves[indice];
				if (!chaveHierarquica.toString().isEmpty())
				{
					chaveHierarquica.append("|");
				}
				chaveHierarquica.append(chave);
			}
		}
		
		return chaveHierarquica.toString();
	}
	
	public Map<String, String> listarTipoPessoas()
	{
		return espelhoOcorrenciaDAO.listarTipoPessoas();
	}
	
	public Map<String, String> listarTipoObjetos()
	{
		return espelhoOcorrenciaDAO.listarTipoObjetos();
	}

	public ArvoreSimples listarTipoObjetosArvore()
	{
		ArvoreSimples arvore = new ArvoreSimples();
		arvore.setChave("0");
		arvore.setValor("Raiz - tipo objeto");
		Set<ArvoreSimples> nivel1 = arvore.getFilhos();
		
		//-- ordenacao por descricao
		Map<String, String> mapTipoObjetos = espelhoOcorrenciaDAO.listarTipoObjetos();
		
		for (Map.Entry<String, String> entry : mapTipoObjetos.entrySet())
		{
			String chaveComposta[] = entry.getKey().split("\\|");
			String valorComposto[] = entry.getValue().split(">");
			
			String chave = chaveComposta[0];
			String subChave = chaveComposta[1];
			String valor = valorComposto[0].trim();
			String subValor = valorComposto[1].trim();
			
			
			if (nivel1.contains(new ArvoreSimples(chave, valor)))
			{
				for (ArvoreSimples elementoNivel1 : nivel1)
				{
					if (elementoNivel1.getChave().equals(chave))
					{
						ArvoreSimples elementoNivel2 = new ArvoreSimples(subChave, subValor);
						elementoNivel1.getFilhos().add(elementoNivel2);
					}
				}
			}
			else
			{
				ArvoreSimples elementoNivel1 = new ArvoreSimples(chave, valor);
				ArvoreSimples elementoNivel2 = new ArvoreSimples(subChave, subValor);
				elementoNivel1.getFilhos().add(elementoNivel2);
				nivel1.add(elementoNivel1);
			}
			
		}
		
		return arvore;
	}

	
	
}

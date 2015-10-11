package br.com.fences.ocorrenciaespelhobackend.rest;

import java.io.InputStream;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import br.com.fences.fencesutils.conversor.InputStreamParaJson;
import br.com.fences.fencesutils.conversor.converter.ColecaoJsonAdapter;
import br.com.fences.fencesutils.conversor.converter.Converter;
import br.com.fences.fencesutils.filtrocustom.ArvoreSimples;
import br.com.fences.fencesutils.filtrocustom.FiltroCondicao;
import br.com.fences.ocorrenciaentidade.controle.ControleOcorrencia;
import br.com.fences.ocorrenciaentidade.ocorrencia.Ocorrencia;
import br.com.fences.ocorrenciaespelhobackend.ocorrencia.negocio.EspelhoOcorrenciaBO;
import br.com.fences.ocorrenciaespelhobackend.ocorrencia.negocio.EspelhoOcorrenciaControleBO;


@RequestScoped
@Path("/")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class OcorrenciaEspelhoResource {
	
	@Inject
	private transient Logger logger;

	@Inject
	private EspelhoOcorrenciaControleBO espelhoOcorrenciaControleBO;

	@Inject 
	private EspelhoOcorrenciaBO espelhoOcorrenciaBO;

	@Inject
	private Converter<Ocorrencia> converterOcorrencia;
	
	@Inject
	private Converter<ControleOcorrencia> converterControleOcorrencia;
	
	private Gson gson = new GsonBuilder()
			.registerTypeHierarchyAdapter(Collection.class, new ColecaoJsonAdapter())
			.create();

    @PUT
    @Path("espelhoOcorrencia/adicionar")
	public String espelhoOcorrenciaAdicionar(InputStream ipFiltros)
	{
    	String json = InputStreamParaJson.converter(ipFiltros);
    	Ocorrencia ocorrencia = converterOcorrencia.paraObjeto(json, Ocorrencia.class);
		ocorrencia = espelhoOcorrenciaBO.adicionar(ocorrencia);
		json = converterOcorrencia.paraJson(ocorrencia);
		return json;
	}
	
    @POST
    @Path("espelhoOcorrencia/agregarPorAno")
	public String espelhoOcorrenciaAgregarPorAno(InputStream ipFiltros)
	{
    	String json = InputStreamParaJson.converter(ipFiltros);
    	Map<String, String> filtros = gson.fromJson(json, Map.class);
		Map<String, Integer> mapRetorno = espelhoOcorrenciaBO.agregarPorAno(filtros);
		json = gson.toJson(mapRetorno);
    	return json;
	}

    @POST
    @Path("espelhoOcorrencia/agregarPorComplementar")
	public String respelhoOcorrenciaAgregarPorComplementar(InputStream ipFiltros)
	{
    	String json = InputStreamParaJson.converter(ipFiltros);
    	Map<String, String> filtros = gson.fromJson(json, Map.class);
    	Map<String, Integer> mapRetorno =  espelhoOcorrenciaBO.agregarPorComplementar(filtros);
    	json = gson.toJson(mapRetorno);
    	return json;
	}

    @POST
    @Path("espelhoOcorrencia/agregarPorFlagrante")
	public String espelhoOcorrenciaAgregarPorFlagrante(InputStream ipFiltros)
	{
    	String json = InputStreamParaJson.converter(ipFiltros);
    	Map<String, String> filtros = gson.fromJson(json, Map.class);
    	Map<String, Integer> mapRetorno = espelhoOcorrenciaBO.agregarPorFlagrante(filtros);
    	json = gson.toJson(mapRetorno);
    	return json;
	}
	
    @GET
    @Path("espelhoOcorrencia/consultar/{id}")
	public String espelhoOcorrenciaConsultar(@PathParam("id") String id)
	{
		Ocorrencia ocorrencia = espelhoOcorrenciaBO.consultar(id);
		String json = converterOcorrencia.paraJson(ocorrencia);
		return json;
	}

    @POST
    @Path("espelhoOcorrencia/contar")
	public String espelhoOcorrenciaContar(InputStream ipFiltros)
	{
    	String json = InputStreamParaJson.converter(ipFiltros);
    	Map<String, String> filtros = gson.fromJson(json, Map.class);
		int count = espelhoOcorrenciaBO.contar(filtros);
		return Integer.toString(count);
	}
    
    @POST
    @Path("espelhoOcorrencia/contarDinamico")
	public String espelhoOcorrenciaContarDinamico(InputStream ipFiltros)
	{
    	String json = InputStreamParaJson.converter(ipFiltros);
    	Type collectionType = new TypeToken<List<FiltroCondicao>>(){}.getType();
    	List<FiltroCondicao> filtroCondicoes = gson.fromJson(json, collectionType); 
		int count = espelhoOcorrenciaBO.contarDinamico(filtroCondicoes);
		return Integer.toString(count);
	}
	
    @POST
    @Path("espelhoOcorrencia/pesquisarLazy/{primeiroRegistro}/{registrosPorPagina}")
	public String rouboCargaPesquisarLazy(    		
			@PathParam("primeiroRegistro") int primeiroRegistro,
    		@PathParam("registrosPorPagina") int registrosPorPagina,
    		InputStream ipFiltros)
	{
    	String json = InputStreamParaJson.converter(ipFiltros);
    	Map<String, String> filtros = gson.fromJson(json, Map.class); 
    	List<Ocorrencia> ocorrencias = espelhoOcorrenciaBO.pesquisarLazy(filtros, primeiroRegistro, registrosPorPagina);
    	json = converterOcorrencia.paraJson(ocorrencias);
    	return json; 
	}
    
    @POST
    @Path("espelhoOcorrencia/pesquisarDinamicoLazy/{primeiroRegistro}/{registrosPorPagina}")
	public String rouboCargaPesquisarDinamicoLazy(    		
			@PathParam("primeiroRegistro") int primeiroRegistro,
    		@PathParam("registrosPorPagina") int registrosPorPagina,
    		InputStream ipFiltros)
	{
    	String json = InputStreamParaJson.converter(ipFiltros);
    	Type collectionType = new TypeToken<List<FiltroCondicao>>(){}.getType();
    	List<FiltroCondicao> filtroCondicoes = gson.fromJson(json, collectionType); 
    	List<Ocorrencia> ocorrencias = espelhoOcorrenciaBO.pesquisarDinamicoLazy(filtroCondicoes, primeiroRegistro, registrosPorPagina);
    	json = converterOcorrencia.paraJson(ocorrencias);
    	return json; 
	}

    @GET
    @Path("espelhoOcorrencia/pesquisarUltimaDataRegistroNaoComplementar")
	public String rouboCargaPesquisarUltimaDataRegistroNaoComplementar()
	{
		return espelhoOcorrenciaBO.pesquisarUltimaDataRegistroNaoComplementar();
	}

    @POST
    @Path("espelhoOcorrencia/substituir")
	public void espelhoOcorrenciaSubstituir(InputStream ipFiltros)
	{
    	String json = InputStreamParaJson.converter(ipFiltros);
    	Ocorrencia ocorrencia = converterOcorrencia.paraObjeto(json, Ocorrencia.class);
    	espelhoOcorrenciaBO.substituir(ocorrencia);
	}
    
    @GET
    @Path("espelhoOcorrencia/pesquisarPrimeiraDataRegistro")
	public String espelhoOcorrenciaPesquisarPrimeiraDataRegistro()
	{
		return espelhoOcorrenciaBO.pesquisarPrimeiraDataRegistro();
	}

    @GET
    @Path("espelhoOcorrencia/pesquisarUltimaDataRegistro")
	public String rouboCargaUltimaPrimeiraDataRegistro()
	{
		return espelhoOcorrenciaBO.pesquisarUltimaDataRegistro();
	}
    
    @GET
    @Path("espelhoOcorrencia/listarAnos")
	public String espelhoOcorrenciaListarAnos()
	{
		List<String> anos = espelhoOcorrenciaBO.listarAnos();
		String json = gson.toJson(anos);
		return json;
	}
    
    @GET
    @Path("espelhoOcorrencia/listarAnosMap")
	public String espelhoOcorrenciaListarAnosMap()
	{
		Map<String, String> anos = espelhoOcorrenciaBO.listarAnosMap();
		String json = gson.toJson(anos);
		return json;
	}
 
    @GET
    @Path("espelhoOcorrencia/listarDelegacias")
	public String espelhoOcorrenciaListarDelegacias()
	{
		Map<String, String> delegacias = espelhoOcorrenciaBO.listarDelegacias();
		String json = gson.toJson(delegacias);
		return json;
	}
    
    @GET
    @Path("espelhoOcorrencia/listarNaturezas")
	public String espelhoOcorrenciaListarNaturezas()
	{
		Map<String, String> naturezas = espelhoOcorrenciaBO.listarNaturezas();
		String json = gson.toJson(naturezas);
		return json;
	}
    
    @GET
    @Path("espelhoOcorrencia/listarNaturezasArvore")
	public String espelhoOcorrenciaListarNaturezasArvore()
	{
		ArvoreSimples arvore = espelhoOcorrenciaBO.listarNaturezasArvore();
		String json = gson.toJson(arvore);
		return json;
	}
    
    @GET
    @Path("espelhoOcorrencia/listarNaturezasArvoreComDesdobramentoCircunstancia")
	public String espelhoOcorrenciaListarNaturezasArvoreComDesdobramentoCircunstancia()
	{
		ArvoreSimples arvore = espelhoOcorrenciaBO.listarNaturezasArvoreComDesdobramentoCircunstancia();
		String json = gson.toJson(arvore);
		return json;
	}
    
    @GET
    @Path("espelhoOcorrencia/listarTipoPessoas")
	public String espelhoOcorrenciaListarTipoPessoas()
	{
		Map<String, String> tipos = espelhoOcorrenciaBO.listarTipoPessoas();
		String json = gson.toJson(tipos);
		return json;
	}
    
    @GET
    @Path("espelhoOcorrencia/listarTipoObjetos")
	public String espelhoOcorrenciaListarTipoObjetos()
	{
		Map<String, String> tipos = espelhoOcorrenciaBO.listarTipoObjetos();
		String json = gson.toJson(tipos);
		return json;
	}
 
    @GET
    @Path("espelhoOcorrencia/listarTipoObjetosArvore")
	public String rouboCargaListarTipoObjetosArvore()
	{
		ArvoreSimples arvore = espelhoOcorrenciaBO.listarTipoObjetosArvore();
		String json = gson.toJson(arvore);
		return json;
	}
    
    //--- CONTROLE OCORRENCIA
    @GET
    @Path("espelhoOcorrenciaControle/pesquisarUltimaDataRegistroNaoComplementar")
	public String espelhoOcorrenciaControlePesquisarUltimaDataRegistroNaoComplementar()
	{
		return espelhoOcorrenciaControleBO.pesquisarUltimaDataRegistroNaoComplementar();
	}
    
    @PUT
    @Path("espelhoOcorrenciaControle/adicionar")
	public void espelhoOcorrenciaControleAdicionar(InputStream ipFiltros)
	{
    	String json = InputStreamParaJson.converter(ipFiltros);
    	ControleOcorrencia controleOcorrencia = converterControleOcorrencia.paraObjeto(json, ControleOcorrencia.class);
    	espelhoOcorrenciaControleBO.adicionar(controleOcorrencia);
	}
    
    @GET
    @Path("espelhoOcorrenciaControle/pesquisarProcessarReprocessar")
	public String espelhoOcorrenciaControlePesquisarProcessarReprocessar()
	{
		Set<ControleOcorrencia> controleOcorrencias = espelhoOcorrenciaControleBO.pesquisarProcessarReprocessar();
		String json = converterControleOcorrencia.paraJson(controleOcorrencias);
		return json;
	}

    @GET
    @Path("controleOcorrencia/pesquisarIndiciadosProcessarReprocessar")
	public String espelhoOcorrenciaControlePesquisarIndiciadosProcessarReprocessar()
	{
		Set<ControleOcorrencia> controleOcorrencias = espelhoOcorrenciaControleBO.pesquisarIndiciadosProcessarReprocessar();
		String json = converterControleOcorrencia.paraJson(controleOcorrencias);
		return json;
	}

    
    @POST
    @Path("espelhoOcorrenciaControle/substituir")
	public void espelhoOcorrenciaControleSubstituir(InputStream ipFiltros)
	{
    	String json = InputStreamParaJson.converter(ipFiltros);
    	ControleOcorrencia controleOcorrencia = converterControleOcorrencia.paraObjeto(json, ControleOcorrencia.class);
    	espelhoOcorrenciaControleBO.substituir(controleOcorrencia);
	}
    
       
}

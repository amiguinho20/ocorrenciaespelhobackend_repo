package br.com.fences.ocorrenciaespelhobackend.ocorrencia.dao;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.inject.Named;

import org.apache.log4j.Logger;
import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;

import br.com.fences.fencesutils.constante.EstadoProcessamento;
import br.com.fences.fencesutils.conversor.converter.Converter;
import br.com.fences.fencesutils.formatar.FormatarData;
import br.com.fences.fencesutils.verificador.Verificador;
import br.com.fences.ocorrenciaentidade.controle.ControleOcorrencia;
import br.com.fences.ocorrenciaespelhobackend.ocorrencia.provider.ColecaoEspelhoOcorrenciaControle;

@Named
@ApplicationScoped
public class EspelhoOcorrenciaControleDAO {

	@Inject
	private transient Logger logger;

	@Inject
	private Converter<ControleOcorrencia> espelhoOcorrenciaControleConverter;

	@Inject
	@ColecaoEspelhoOcorrenciaControle
	private MongoCollection<Document> colecao;

	public String pesquisarUltimaDataRegistroNaoComplementar() {
		String datahoraRegistroBo = null;

		BasicDBObject pesquisa = new BasicDBObject("complementar", "N");
		BasicDBObject projecao = new BasicDBObject("datahoraRegistroBo", 1).append("_id", 0);
		BasicDBObject ordenacao = new BasicDBObject("datahoraRegistroBo", -1);

		MongoCursor<Document> cursor = colecao.find(pesquisa).projection(projecao).sort(ordenacao).limit(1).iterator();

		try {
			if (cursor.hasNext()) {
				Document documento = cursor.next();
				datahoraRegistroBo = documento.getString("datahoraRegistroBo");
			}
		} finally {
			cursor.close();
		}
		return datahoraRegistroBo;
	}

	public void adicionar(ControleOcorrencia controleOcorrencia) {

		try {
			controleOcorrencia.setEstadoProcessamentoOcorrencia(EstadoProcessamento.PROCESSAR);
			controleOcorrencia.setDatahoraProcessamento(FormatarData.dataHoraCorrente());
			Document documento = espelhoOcorrenciaControleConverter.paraDocumento(controleOcorrencia);
			colecao.insertOne(documento);
		} catch (MongoWriteException e){
			if (Verificador.isValorado(controleOcorrencia.getComplementar()))
			{
				if (controleOcorrencia.getComplementar().equals("S"))
				{
					ControleOcorrencia controleExistente = pesquisar(controleOcorrencia);
					if (controleExistente != null)
					{
						controleExistente.setEstadoProcessamentoOcorrencia(EstadoProcessamento.PROCESSAR);
						substituir(controleExistente);
					}
				}
			}
			else
			{
				String msg = "Erro na adicao MongoWriteException. " + controleOcorrencia + " err[" + e.getMessage() + "].";
				throw new RuntimeException(msg);  
			}
		} catch (Exception e) {
			String msg = "Erro na adicao. " + controleOcorrencia + " err[" + e.getMessage() + "].";
			throw new RuntimeException(msg);
		}
	}

	public void substituir(ControleOcorrencia controleOcorrencia) {

		if (!Verificador.isValorado(controleOcorrencia.getId()))
		{
			String msg = "Erro no substituir. num[" + controleOcorrencia.getNumBo() + "] ano["
					+ controleOcorrencia.getAnoBo() + "] dlg[" + controleOcorrencia.getIdDelegacia() + "] dtReg["
					+ controleOcorrencia.getDatahoraRegistroBo() + "] " + "err[nao possui ID para substituicao].";
			throw new RuntimeException(msg);
		}
		
		try {
			controleOcorrencia.setDatahoraProcessamento(FormatarData.dataHoraCorrente());
			Document documento = espelhoOcorrenciaControleConverter.paraDocumento(controleOcorrencia);
			colecao.replaceOne(Filters.eq("_id", documento.get("_id")), documento);
		} catch (Exception e) {
			String msg = "Erro no substituir. num[" + controleOcorrencia.getNumBo() + "] ano["
					+ controleOcorrencia.getAnoBo() + "] dlg[" + controleOcorrencia.getIdDelegacia() + "] dtReg["
					+ controleOcorrencia.getDatahoraRegistroBo() + "] " + "err[" + e.getMessage() + "].";
			throw new RuntimeException(msg);
		}

	}

	public ControleOcorrencia pesquisar(ControleOcorrencia controleOcorrencia)
	{
		ControleOcorrencia resultado = null;
		BasicDBObject pesquisa = new BasicDBObject();
		pesquisa.append("idDelegacia", controleOcorrencia.getIdDelegacia());
		pesquisa.append("anoBo", controleOcorrencia.getAnoBo());
		pesquisa.append("numBo", controleOcorrencia.getNumBo());
		Document documento = colecao.find(pesquisa).first();
		if (documento != null)
		{
			resultado = espelhoOcorrenciaControleConverter.paraObjeto(documento, ControleOcorrencia.class);
		}
		return resultado;
	}
	
	public Set<ControleOcorrencia> pesquisarProcessarReprocessar() {
		
		Set<ControleOcorrencia> controleOcorrencias = new LinkedHashSet<>();
		
		BasicDBObject pesquisa = 
				new BasicDBObject("estadoProcessamentoOcorrencia", 
					new BasicDBObject("$in", Arrays.asList(
							EstadoProcessamento.PROCESSAR.converterParaValor(),
							EstadoProcessamento.REPROCESSAR.converterParaValor())));
		BasicDBObject ordenacao = new BasicDBObject("datahoraRegistroBo", 1);
		
		MongoCursor<Document> cursor = colecao.find(pesquisa).sort(ordenacao).iterator();

		try {
			while (cursor.hasNext()) {
				Document documento = cursor.next();
				ControleOcorrencia controleOcorrencia = espelhoOcorrenciaControleConverter.paraObjeto(documento, ControleOcorrencia.class);
				controleOcorrencias.add(controleOcorrencia);
			}
		} finally {
			cursor.close();
		}

		return controleOcorrencias;
	}
	
	public Set<ControleOcorrencia> pesquisarIndiciadosProcessarReprocessar() {
		
		Set<ControleOcorrencia> controleOcorrencias = new LinkedHashSet<>();
		
		BasicDBObject pesquisa = 
				new BasicDBObject("estadoProcessamentoIndiciados", 
					new BasicDBObject("$in", Arrays.asList(
							EstadoProcessamento.PROCESSAR.converterParaValor(),
							EstadoProcessamento.REPROCESSAR.converterParaValor())));
		BasicDBObject ordenacao = new BasicDBObject("datahoraRegistroBo", 1);
		
		MongoCursor<Document> cursor = colecao.find(pesquisa).sort(ordenacao).iterator();

		try {
			while (cursor.hasNext()) {
				Document documento = cursor.next();
				ControleOcorrencia controleOcorrencia = espelhoOcorrenciaControleConverter.paraObjeto(documento, ControleOcorrencia.class);
				controleOcorrencias.add(controleOcorrencia);
			}
		} finally {
			cursor.close();
		}

		return controleOcorrencias;
	}


}

package br.com.fences.ocorrenciaespelhobackend.ocorrencia.provider;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;

import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;

import br.com.fences.fencesutils.verificador.Verificador;
import br.com.fences.ocorrenciaespelhobackend.config.AppConfig;

@ApplicationScoped
public class MongoProvider {  

	private static final String COLECAO_ESPELHO_OCORRENCIA = "espelho_ocorrencia01";   
	
	private static final String COLECAO_ESPELHO_OCORRENCIA_CONTROLE = "espelho_ocorrencia_controle01";
	
	private MongoClient conexao;
	private MongoDatabase banco;
	private MongoCollection<Document> colecaoEspelhoOcorrencia;
	private MongoCollection<Document> colecaoEspelhoOcorrenciaControle;
	
	@Inject
	private AppConfig appConfig;
	
	@PostConstruct
	public void abrirConexao() 
	{
		String dbMongoHost = appConfig.getDbMongoHost();
		String dbMongoPort = appConfig.getDbMongoPort();
		String dbMongoDatabase = appConfig.getDbMongoDatabase();
		String dbMongoUser = appConfig.getDbMongoUser();
		String dbMongoPass = appConfig.getDbMongoPass();
		
		if (Verificador.isValorado(dbMongoUser))
		{
			String uriConexao = String.format("mongodb://%s:%s@%s:%s/%s", dbMongoUser, dbMongoPass, dbMongoHost, dbMongoPort, dbMongoDatabase);
			MongoClientURI uri  = new MongoClientURI(uriConexao); 
			conexao = new MongoClient(uri);
		}
		else
		{
			conexao = new MongoClient(dbMongoHost, Integer.parseInt(dbMongoPort));
		}
		banco = conexao.getDatabase(dbMongoDatabase);
		
		colecaoEspelhoOcorrencia = banco.getCollection(COLECAO_ESPELHO_OCORRENCIA);
		if (colecaoEspelhoOcorrencia == null)
		{
			banco.createCollection(COLECAO_ESPELHO_OCORRENCIA);
			colecaoEspelhoOcorrencia = banco.getCollection(COLECAO_ESPELHO_OCORRENCIA);
			   
			BasicDBObject campos = new BasicDBObject();
			campos.append("DATAHORA_REGISTRO_BO", 1);
			
			IndexOptions opcoes =  new IndexOptions();
			opcoes.unique(true);
			
			colecaoEspelhoOcorrencia.createIndex(campos, opcoes);
			
			BasicDBObject campos2 = new BasicDBObject();
			campos2.append("ANO_BO", 1);
			campos2.append("NUM_BO", 1);
			campos2.append("ID_DELEGACIA", 1);
			
			colecaoEspelhoOcorrencia.createIndex(campos2, opcoes);
		}

		colecaoEspelhoOcorrenciaControle = banco.getCollection(COLECAO_ESPELHO_OCORRENCIA_CONTROLE);
		if (colecaoEspelhoOcorrenciaControle == null)
		{
			banco.createCollection(COLECAO_ESPELHO_OCORRENCIA_CONTROLE);
			colecaoEspelhoOcorrenciaControle = banco.getCollection(COLECAO_ESPELHO_OCORRENCIA_CONTROLE);
		}	
	}
	
	/**
	 * Fechar a conexao com o banco quando o objeto for destruido.
	 */
	@PreDestroy
	public void fecharConecao()
	{
		conexao.close();
	}
	
	@Produces @ColecaoEspelhoOcorrencia
	public MongoCollection<Document> getColecaoEspelhoOcorrencia()
	{
		return colecaoEspelhoOcorrencia;
	}
	
	@Produces @ColecaoEspelhoOcorrenciaControle
	public MongoCollection<Document> getColecaoEspelhoOcorrenciaControle()
	{
		return colecaoEspelhoOcorrenciaControle;
	}

}

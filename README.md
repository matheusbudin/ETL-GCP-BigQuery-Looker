
![Arquitetura](https://github.com/matheusbudin/rox-gcp-test/blob/features-dev/images_for_readme/gcp-architecture.png)


Este projeto foi desenvolvido baseado em um desafio promovido pela Rox Partner a qual proveu as fontes de dados e requisitou uma infraestrutura em nuvem.
A base de dados fornecida trás o contexto de uma empresa que produz bicicletas fictícia.

TO DO:

## -Realizar a modelagem dos dados;
## -Criação de infraestrutura na nuvem 
## -Criação de todos os artefatos necessários para carga dos arquivos para o banco de dados;
## -Desenvolvimento dos scripts para análise de dados;
## -opcional: criar um relatório com qualquer ferramenta de BI.

Para o desenvolvimento do projeto foi utilizada a plataforma da Google - GCP por ser a cloud que desejo me especializar. Porém foi um grande desafio por ser meu primeiro contato em desenvolvimento de um projeto "end to end", pois trabalho atualmente com AWS e já havia utilizado Azure Databricks.

Considerando um projeto piloto que possui espaços para melhorias que serão listadas no final deste readme. A proposta inicial foi utilizar boa parte da criação dos recursos por meio de programação em python, utilizando a API oficial da GCP. Dos recursos utilizados temos:

 **Google Cloud Storage**: Buckets para armazenar os arquivos csv já limpos (depois do tratamento pelos notebooks.ipynb);

 **Google Big Quey**: A ideia de utilizar o GCP big query foi por ser um serviço serverless e um Modern Data Warehouse com muito poder de processamento, muito potencial com as próximas atualizações já anunciadas (DuetAI, notebooks, etc) e seu custo relativamente baixo (cautela ao usar). Além de facilitar as consultas Ad-hoc e ter conexão nativa com o Looker Studio que foi a nossa escolha de ferramenta de BI;
 
 **Google Looker Studio**: Por já estar desenvolvendo todo o projeto na GCP nada melhor do que utilizar a ferramenta de BI da própria provider, facilitando as conexões e evitando problemas na exportação de dados, confecção de dashboards e estruturação de relacionamento entre as tabelas. Que, por sinal, foi bem intuitiva nesta ferramenta.

 **Google IAM**: Aqui definimos as chaves de acesso de API que foram utilizadas nos scripts Python que fazem a criação do DataBase, criação das tabelas por meio dos DDL's, e carga de dados do google storage para cada tabela respectiva.

 **PySpark**: Os scripts de tratamento de dados e limpeza das tabelas foram confeccioandos em PySpark, já pensando em um cenário de Big Data onde poderiamos utilizar um **GCP DataProc**, para termos um cluster e realizar esses tratamentos sem precisar re-escrever o código.

 **drawio**: utilizado para criação da ilustração da arquitetura utilizada.


 ### Utilização dos Scripts:

 para utilizar os scripts ".py" da pasta [Scripts](https://github.com/matheusbudin/rox-gcp-test/tree/features-dev/scripts) é necessário que você faça os exports das variáveis de ambiente, e principalmente das suas credenciais.json (uma dica valiosa, coloque um tempo para que as credenciais fiquem desativadas ou apague-as manualmente após o uso). Exemplos das variáveis de ambiente:


export  MY_GCS_BUCKET_URL = "<yourbucket_url>"

export  GOOGLE_APPLICATION_CREDENTIALS="<your_credentials.json>"

export DDL_BUCKET = "<your_credentials.json>"  # Este bucket é para melhoria futura que utilizará **GCP Functions**

export DATA_BUCKET = "<yourbucket_name>" 

export DATASET_NAME = "<yourDataSetName>"

**Observação: foi utilizado uma virtualenv para este projeto e as libs utilziadas estão no requeriments.txt**

### Limpeza de Dados:

Antes de começar a carregar os dados e realizar todas as análises, foi necessário um carinho com cada arquivo que compoe a base de dados. Como já dito anteriormente, foi utilizado **PySpark** para esses tratamentos já para utilizados em um cenário BigData em um **cluster DataProc** se necessário. Esses arquivos estão na pasta: [scripts/ipynbs_data_cleaning](https://github.com/matheusbudin/rox-gcp-test/tree/features-dev/scripts/ipynbs_data_cleaning).

Considerações importantes: 
- Foi "forçado" o schema de cada tabela para ter congruência entre o DDL e o dado exportado pelo spark e dessa forma não ter conflitos na carga para o GCP big query;
- O separador do csv foi trocado de ";" para "," embora o Big query tenha suporte para mudança de delimitador, foi decidido deixar um padrão que a maioria dos projetos utilizam;
- O formato de tempo utilizado em todas as tabelas foi classificado como **TimeStamp** pois o big query consegue trazer de maneira mais minuciosa cada data em relação ao **datetime**;
- Textos com string "null" foram retirados com regex;
- Tabela customer a coluna PersonID foi setada para integer, foi um dos problemas ter visto ela inicialmente como string, os relacionamentos nao ocorriam nas querys, foi consertado em "mid-development".
- ID's de primary keys da tabela **SpecialOfferProduct" foram deduplicados.


### Modelagem dimensional

Topologia dos dados enviada:

[Relacionamento_tabelas](https://github.com/matheusbudin/rox-gcp-test/blob/features-dev/images_for_readme/relacionamento_tabelas.png)

Nosso "DB" que no bigquery é chamado de "DataSet" porém que possui funcionamento análogo ao DB. Teve a estrutura definida da seguinte forma:

.rox-test-task
├── testRoxDB/
│   ├── customer_personid_not_null
│   ├── Person
│   ├── Product
│   ├── SalesOrderDetail
│   ├── SalesOrderHeader
│   └── SpecialOfferProduct


Os DDLs utilizados no script de criação das tabelas diretamente no big query podem ser consultados na pasta a seguir: [DDLs.sql](https://github.com/matheusbudin/rox-gcp-test/tree/features-dev/local_csv_data/tabel_definitions_DDL) e para a criação do DB/ dataSet do big query que vai abrigar essas tabelas: [DB_dataset](https://github.com/matheusbudin/rox-gcp-test/tree/features-dev/local_csv_data/db_definition)

Com esses arquivos e os scripts: [create_tables](https://github.com/matheusbudin/rox-gcp-test/blob/features-dev/scripts/create_table_from_DDL.py) e [create_DB_dataset](https://github.com/matheusbudin/rox-gcp-test/blob/features-dev/scripts/create_dataset_db_bq.py) foi possivel criar respectivamente, as tabelas e o Db/Datset no bigquery,.


### ETL

A ingestão de dados foi pela pelo script python: [migrate_csv_to_gcp](https://github.com/matheusbudin/rox-gcp-test/blob/features-dev/scripts/migrate_csv_to_gcp.py). O Script pega os arquivos locais e faz a carga no gcp storage. Vale ressaltar, que os arquivos que foram migrados do local para nuvem são os arquivos ja tratados resultantes dos jupyter notebooks.

Uma vez que temos do passo anterior, o DB e as tabelas criadas (com schema definido pelos DDL.sql porém vazias em primeiro momento) foi possível utilizar o script [insert_data_into_tables](https://github.com/matheusbudin/rox-gcp-test/blob/features-dev/scripts/insert_data_into_tables.py) para fazer a carga dos dados de cada csv limpo para cada respectiva tabela.

Dessa forma já temos dados populando as tabelas e prontos para a etapa de **Análise de Dados**.

### Análise de Dados -  Respondendo as questões de negócio

1. Escreva uma query que retorna a quantidade de linhas na tabela Sales.SalesOrderDetail pelo campo SalesOrderID, desde que tenham pelo menos três linhas de detalhes.

```
SELECT 
	SalesOrderID AS id, 
	COUNT(*) AS lines_qtd 
FROM `rox-test-task.testRoxDB.SaleOrderDetails`  as sod
GROUP BY SalesOrderID
HAVING lines_qtd >= 3
ORDER BY lines_qtd DESC
```

![query_1](https://github.com/matheusbudin/rox-gcp-test/blob/features-dev/images_for_readme/query_1.png)

2.	Escreva uma query que ligue as tabelas Sales.SalesOrderDetail, Sales.SpecialOfferProduct e Production.Product e retorne os 3 produtos (Name) mais vendidos (pela soma de OrderQty), agrupados pelo número de dias para manufatura (DaysToManufacture).

```
WITH RankedProducts AS (
  SELECT
      p.Name AS ProductName,
      p.DaysToManufacture AS DaysToManufacture,
      SUM(sod.OrderQty) AS TotalQuantitySold,
      ROW_NUMBER() OVER(PARTITION BY p.DaysToManufacture ORDER BY sum(sod.OrderQty) DESC) as pos,
  FROM `rox-test-task.testRoxDB.SpecialOfferProduct` AS sop 
  JOIN `rox-test-task.testRoxDB.products` AS p ON sop.ProductID = p.ProductID
  JOIN `rox-test-task.testRoxDB.SaleOrderDetails` AS sod ON sop.SpecialOfferID = sod.SalesOrderDetailID
  GROUP BY p.Name, p.DaysToManufacture
)
SELECT * FROM RankedProducts WHERE pos <= 3
```
![query_2](https://github.com/matheusbudin/rox-gcp-test/blob/features-dev/images_for_readme/query_2.png)

3.	Escreva uma query ligando as tabelas Person.Person, Sales.Customer e Sales.SalesOrderHeader de forma a obter uma lista de nomes de clientes e uma contagem de pedidos efetuados.

```
SELECT
	c.CustomerID AS id,
	CONCAT(p.FirstName, ' ', p.LastName) AS full_name, 
	COUNT(*) AS qtd
FROM `rox-test-task.testRoxDB.SalesOrderHeader` soh
JOIN `rox-test-task.testRoxDB.customer_personid_not_null` c ON soh.CustomerID = c.CustomerID
JOIN `rox-test-task.testRoxDB.person`  p ON c.PersonID = p.BusinessEntityID 
GROUP BY c.CustomerID, full_name
ORDER BY qtd DESC;
```

![query_3](https://github.com/matheusbudin/rox-gcp-test/blob/features-dev/images_for_readme/query_3.png)

4.	Escreva uma query usando as tabelas Sales.SalesOrderHeader, Sales.SalesOrderDetail e Production.Product, de forma a obter a soma total de produtos (OrderQty) por ProductID e OrderDate.

```
SELECT
    sod.ProductID AS id,
    p.Name AS nome,
    SUM(sod.OrderQty) AS qtd,
    soh.OrderDate AS order_date
FROM `rox-test-task.testRoxDB.SaleOrderDetails` sod
JOIN `rox-test-task.testRoxDB.SalesOrderHeader` soh ON sod.SalesOrderID = soh.SalesOrderID
JOIN `rox-test-task.testRoxDB.products` p ON sod.ProductID = p.ProductID
GROUP BY id, nome, order_date
ORDER BY order_date, qtd DESC;
```

![query_4](https://github.com/matheusbudin/rox-gcp-test/blob/features-dev/images_for_readme/query_4.png)

5.	Escreva uma query mostrando os campos SalesOrderID, OrderDate e TotalDue da tabela Sales.SalesOrderHeader. Obtenha apenas as linhas onde a ordem tenha sido feita durante o mês de setembro/2011 e o total devido esteja acima de 1.000. Ordene pelo total devido decrescente.

```
SELECT 
	SalesOrderID as id,
	CAST(OrderDate AS DATE) AS date, 
	TotalDue AS total_devido
FROM `rox-test-task.testRoxDB.SalesOrderHeader`
WHERE OrderDate BETWEEN '2011-09-01' AND '2011-09-30' AND TotalDue > 1000
ORDER BY total_devido DESC;
```


![query_5](https://github.com/matheusbudin/rox-gcp-test/blob/features-dev/images_for_readme/query_5.png)

**Evidencia questão 5.**


```
SELECT 
	SalesOrderID as id,
	CAST(OrderDate AS DATE) AS date, 
	TotalDue AS total_devido
FROM `rox-test-task.testRoxDB.SalesOrderHeader`
WHERE OrderDate BETWEEN '2011-08-01' AND '2011-10-01' AND TotalDue > 1000
ORDER BY date;
```

![evidencia_query_5](https://github.com/matheusbudin/rox-gcp-test/blob/features-dev/images_for_readme/query_5_evidencia.png)


### Dashboard - BI

Foi utilizado a ferramenta nativa do GCP para visualização de dados o Google Looker Studio (antigo Google Data Studio)

Relacionamento das tabelas:

![relacionamento_tabelas](https://github.com/matheusbudin/rox-gcp-test/blob/features-dev/images_for_readme/relacionamento_looker_studio.png)

Alguns gráficos:
-Quantidade de perdidos por data
-Total por CustomerID (cumulativo e em escala logarítimica)


![dashboard](https://github.com/matheusbudin/rox-gcp-test/blob/main/images_for_readme/dashboard_rox.png)


## Melhorias no radar:

- De imediato a primeira melhoria é colocar uma GCP Function que terá o Trigger configurado para que seja disparada sempre que um arquivo novo entre no storage e faça a carga incremental na tabela correspondente;

- Explorar um serviço GCP de base de dados relacional não serverless para um cenário Big Data e comparar os custos com a opção direta do bigquery abordada inicialmente neste projeto;

- utilizar o cenário Big Data: aumentar o tamanho dos arquivos CSV, e utilizar o cluster da GCP o **Data Proc**

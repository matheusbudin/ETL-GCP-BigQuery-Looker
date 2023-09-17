# rox-gcp-test
important : create and manage your credentials.json and then pass as an variable on git bash

export GOOGLE_APPLICATION_CREDENTIALS="path_to_your_service_account_file.json"

you need to have a .env file that stores all your local enviroments to protect your data for example:

export  MY_GCS_BUCKET = "<bucket_name>"
export  MY_GCS_BUCKET_URL = "<bucket_URL>"
export  GOOGLE_APPLICATION_CREDENTIALS="<your_credentials_path>"



![Arquitetura](https://github.com/matheusbudin/rox-gcp-test/blob/features-dev/images_for_readme/gcp-architecture.png)


Este projeto foi desenvolvido em função de um teste da Rox Partener a qual proveu as fontes de dados e requisitou uma infraestrutura em nuvem.
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


### Limpeza de Dados:

Antes de começar a carregar os dados e realizar todas as análises, foi necessário um carinho com cada arquivo que compoe a base de dados. Como já dito anteriormente, foi utilizado **PySpark** para esses tratamentos já para utilizados em um cenário BigData em um **cluster DataProc** se necessário. Esses arquivos estão na pasta: [scripts/ipynbs_data_cleaning](https://github.com/matheusbudin/rox-gcp-test/tree/features-dev/scripts/ipynbs_data_cleaning).

Considerações importantes: 
- Foi "forçado" o schema de cada tabela para ter congruência entre o DDL e o dado exportado pelo spark e dessa forma não ter conflitos na carga para o GCP big query;
- O separador do csv foi trocado de ";" para "," embora o Big query tenha suporte para mudança de delimitador, foi decidido deixar um padrão que a maioria dos projetos utilizam;
- O formato de tempo utilizado em todas as tabelas foi classificado como **TimeStamp** pois o big query consegue trazer de maneira mais minuciosa cada data em relação ao **datetime**;
- Textos com string "null" foram retirados com regex;
- Tabela customer a coluna PersonID foi setada para integer, foi um dos problemas ter visto ela inicialmente como string, os relacionamentos nao ocorriam nas querys, foi consertado em "mid-development".
- ID's de primary keys da tabela **SpecialOfferProduct" foram deduplicados.


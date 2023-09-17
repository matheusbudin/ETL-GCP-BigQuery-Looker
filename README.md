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

** -Realizar a modelagem dos dados;
** -Criação de infraestrutura na nuvem (nesse caso foi escolhida a GCP - meu primeiro contato inclusive com esta nuvem pois trabalho com AWS);
** -Criação de todos os artefatos necessários para carga dos arquivos para o banco de dados;
** -Desenvolvimento dos scripts para análise de dados;
** -opcional: criar um relatório com qualquer ferramenta de BI.


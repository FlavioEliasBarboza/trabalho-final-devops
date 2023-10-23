## Trabalho Final - DataOps - Develop and Deliver Analytics
 
Repositórios do trabalho final da diciplina de DataOps do MBA Data Engineering (Faculdade Impacta).

## Objetivo

- Desenvolver uma aplicação simplificada de extração de dados referentes a Pessoas, Planetas e Filmes referente a Saga de Starwars, extraído da *SWAPI - The Star Wars API* [(Saiba Mais)](https://swapi.dev/documentation)
- Tratamento e Manipulação dos dados em camadas *Silver*, *Bronze* e *Gold*. 

## Requisitos

-   Formato da tabela de entrega: csv
-   Frequência de atualização do dado: frequência de 1x por dia
-   Parâmetro de coleta: 1 página por requisição
-   Salvar logs do processo em CSV
-   Armazenamento dos dados brutos
-   Armazenamento dos dados saneados: Tratamento de tipos, nomes e nulos
-   Armazenamento dos dados agregados e tratados
-   Validação de qualidade de dados: Validação de duplicados e Tolerância de nulos

## Origem dos dados
- https://swapi.dev/api/people/?
- https://swapi.dev/api/planets/?
- https://swapi.dev/api/films/?

## Data Paths
- **Bronze**: ./data/bronze/*
- **Siver**: ./data/silver/*
- **Gold**: ./data/gold/*
- **Log**: ./data/logs.csv
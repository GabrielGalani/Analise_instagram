# Instagram Data Analysis Project

Este projeto tem como intuito coletar dados da API do Instagram, tanto de contas próprias quanto de contas pesquisadas, visando analisar e extrair insights para a tomada de decisões no setor de marketing.

## Descrição do Projeto

A análise de dados do Instagram é crucial para entender o engajamento e a eficácia das campanhas de marketing. Este projeto foi desenvolvido para:

1. Extrair dados da API do Instagram.
2. Montar um banco de dados estruturado.
3. Criar visualizações e relatórios no Power BI para facilitar a análise e a tomada de decisões.

## Pipeline de Dados

O pipeline de dados do projeto é composto pelas seguintes etapas:

1. **Extração de Dados**: Coleta de informações da API do Instagram.
2. **Transformação e Carga**: Estruturação dos dados em um banco de dados.
3. **Visualização**: Criação de views e relatórios no Power BI.

## BI Finalizado

A conclusão do projeto se deu com a criação do BI abaixo:

(Foto do dashboard no Power BI)

## Tecnologias Utilizadas

- **Python**: Para a extração e processamento dos dados.
- **Power BI**: Para a criação das visualizações e relatórios.

## Como Utilizar

1. **Configuração do Ambiente**:
   - Clone este repositório.
   - Instale as dependências listadas em `requirements.txt`.

2. **Extração de Dados**:
   - Configure as credenciais da API do Instagram.
   - Coloque o nome do seu perfil do instagram Execute o script de extração de dados (`scr\tratamento_bronze_api_ig.py`).
   - Caso queira pesquisar dados de outros perfis, coloque o nome do perfil no arquivo e execute o arquivo(`scr\tratamento_bronze_api_search_ig.py`)

2.1 **Execute os arquivos sql de criação do banco de dados**

3. **Transformação e Carga**:
   - Execute os scripts de transformação e carga de dados para popular o banco de dados.

4. **Visualização no Power BI**:
   - Importe os dados para o Power BI.

## Contribuição

Sinta-se à vontade para contribuir com o projeto abrindo issues e pull requests.

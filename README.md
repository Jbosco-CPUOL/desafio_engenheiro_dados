# Desafio Engenharia de Dados - Pipeline e Modelagem Dimensional - João Bosco

## 🏗️ Arquitetura e Tecnologias Escolhidas

Embora o desafio sugerisse o uso de bancos relacionais (PostgreSQL/MySQL), optei por implementar uma arquitetura moderna de **Data Lakehouse** utilizando **PySpark** e **Apache Iceberg**.

* **Motivação:** A ideia de usar essa stack veio de algumas experiências ruins que tive em outros projetos de Big Data antes de usar o Spark, onde os processos simplesmente não escalavam. O PySpark resolve isso rodando tudo em paralelo, e o Iceberg encaixa como uma luva: entrega mais performance e segurança necessário, sem os gargalos de um banco comum quando o volume aumenta.

---

## 📌 Decisões Técnicas e Premissas Assumidas

### Desafio 1: Ingestão e Sanitização (Camada Raw/Bronze)
* **Encoding:** Utilizado o formato `ISO-8859-1` na leitura dos ficheiros `.csv` para preservar corretamente a acentuação (ex: "Crédito", "Débito").
* **Tipagem e Limpeza:** * O `cod_cooperativa` foi padronizado para 4 dígitos utilizando a função `lpad` nativa do Spark.
    * O `num_cpf_cnpj` foi limpo usando expressões regulares (**Regex**) para manter apenas caracteres numéricos.
    * O `vlr_transacao` teve a vírgula substituída por ponto e foi tipado como `Double` para permitir operações matemáticas.

### Desafio 2: Tabela Flat (Camada Silver/Gold)
* **Premissa de Tempo:** Como o dataset fornecido é estático, utilizar a data atual do sistema (`current_date`) resultaria numa tabela vazia. 
* **Solução:** Utilizei a data máxima (`MAX(dat_transacao)`) existente na base de dados como âncora ("hoje") para garantir o cálculo correto dos indicadores de atividade e frequência nos últimos 3 meses.
* **Técnica:** Utilização de **CTEs** e **Conditional Aggregation** em PySpark SQL para criar as flags booleanas (1 ou 0).

### Desafio 3: Modelagem Fato x Dimensão (Star Schema)
* **Surrogate Keys (SK):** Para as tabelas `dim_associado` e `dim_agencia`, foram geradas chaves substitutas artificiais numéricas (`monotonically_increasing_id()`). Isso protege o Data Warehouse contra alterações nas chaves de negócio nos sistemas operacionais.
* **Granularidade da Fato:** Definida uma linha por transação de cartão.

---

## 🚀 Passo a Passo para Execução (Ambiente Linux/WSL)

Os scripts foram desenvolvidos e testados em ambiente **Linux (Ubuntu/WSL)** utilizando **Python 3.11** e **Java 17**.

### 1. Preparação do Ambiente
Certifique-se de ter o Java 17 e o Python 3.11 instalados:

```bash
sudo apt update
sudo apt install openjdk-17-jdk python3.11 python3.11-venv -y
```

### 2. Configuração do Ambiente Virtual

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install pyspark==3.4.1 pandas
```

### 3. Execução da Pipeline

Coloque os ficheiros .csv na mesma pasta dos scripts e execute-os por ordem:

#### Etapa 1: Ingestão e Sanitização

```bash
python desafio_1.py
```

#### Etapa 2: Construção da Tabela Flat

```bash
python desafio_2.py
```

#### Etapa 3: Construção do Data Warehouse

```bash
python desafio_3.py
```

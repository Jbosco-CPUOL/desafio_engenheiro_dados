from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lpad, regexp_replace, to_date,to_timestamp

spark = SparkSession.builder \
    .appName("ETL_Cooperativa_Desafio1") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local_catalog.type", "hadoop") \
    .config("spark.sql.catalog.local_catalog.warehouse", "spark-warehouse/iceberg") \
    .getOrCreate()

def extrair_dados(spark):

    df_associado = spark.read.csv('data/db_pessoa.associado.csv', sep=';', header=True, inferSchema=True, encoding="ISO-8859-1")
    df_agencia = spark.read.csv('data/db_entidade.agencia.csv', sep=';', header=True, inferSchema=True, encoding="ISO-8859-1")
    df_transacoes = spark.read.csv('data/db_cartoes.transacoes.csv', sep=';', header=True, inferSchema=True, encoding="ISO-8859-1")
    
    return df_associado, df_agencia, df_transacoes

def transformar_dados(df_associado, df_agencia, df_transacoes):

    df_agencia = df_agencia.withColumn("cod_cooperativa", lpad(col("cod_cooperativa").cast("string"), 4, "0"))
    df_transacoes = df_transacoes.withColumn("cod_cooperativa", lpad(col("cod_cooperativa").cast("string"), 4, "0"))
    
    df_associado = df_associado.withColumn("num_cpf_cnpj", regexp_replace(col("num_cpf_cnpj").cast("string"), r"\D", ""))
    df_transacoes = df_transacoes.withColumn("num_cpf_cnpj", regexp_replace(col("num_cpf_cnpj").cast("string"), r"\D", ""))
    
    df_associado = df_associado.withColumn("dat_associacao", to_date(col("dat_associacao"), "dd/MM/yyyy"))
    df_transacoes = df_transacoes.withColumn("dat_transacao", to_timestamp(col("dat_transacao"), "dd/MM/yyyy HH:mm:ss"))

    df_transacoes = df_transacoes.withColumn(
        "vlr_transacao", 
        regexp_replace(col("vlr_transacao").cast("string"), ",", ".").cast("double")
    )
    
    return df_associado, df_agencia, df_transacoes

def carregar_dados_iceberg(spark, df_associado, df_agencia, df_transacoes):
    """Cria os bancos de dados (namespaces) e carrega as tabelas no Iceberg."""
    
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local_catalog.db_pessoa")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local_catalog.db_entidade")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local_catalog.db_cartoes")
    
    df_associado.write.format("iceberg") \
        .mode("overwrite") \
        .saveAsTable("local_catalog.db_pessoa.associado")
        
    df_agencia.write.format("iceberg") \
        .mode("overwrite") \
        .saveAsTable("local_catalog.db_entidade.agencia")
        
    df_transacoes.write.format("iceberg") \
        .mode("overwrite") \
        .saveAsTable("local_catalog.db_cartoes.transacoes")

if __name__ == "__main__":
    print("Iniciando pipeline ETL...")
    associados, agencias, transacoes = extrair_dados(spark)
    
    print("Aplicando transformações...")
    associados_t, agencias_t, transacoes_t = transformar_dados(associados, agencias, transacoes)
    
    print("Persistindo dados nas tabelas...")
    carregar_dados_iceberg(spark, associados_t, agencias_t, transacoes_t)
    
    print("Desafio 1 concluído!")
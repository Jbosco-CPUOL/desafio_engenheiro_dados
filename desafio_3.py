from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, to_date

spark = SparkSession.builder \
    .appName("ETL_Cooperativa_Desafio3") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local_catalog.type", "hadoop") \
    .config("spark.sql.catalog.local_catalog.warehouse", "spark-warehouse/iceberg") \
    .getOrCreate()

def construir_modelo_dimensional(spark):
    
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local_catalog.db_dw")
    
    print("Construindo e salvando Dimensão Associado...")
    spark.sql("""
        SELECT 
            monotonically_increasing_id() AS sk_associado,
            num_cpf_cnpj,
            des_nome_associado,
            dat_associacao,
            des_faixa_renda
        FROM local_catalog.db_pessoa.associado
    """).write.format("iceberg").mode("overwrite").saveAsTable("local_catalog.db_dw.dim_associado")
    
    print("Construindo e salvando Dimensão Agência...")
    spark.sql("""
        SELECT 
            monotonically_increasing_id() AS sk_agencia,
            cod_cooperativa,
            des_nome_cooperativa,
            cod_agencia,
            des_nome_agencia
        FROM local_catalog.db_entidade.agencia
    """).write.format("iceberg").mode("overwrite").saveAsTable("local_catalog.db_dw.dim_agencia")
    
    print("Construindo e salvando Tabela Fato Transações...")
    query = """
        SELECT 
            a.sk_associado,
            ag.sk_agencia,
            
            t.num_plastico,
            t.cod_conta,
            t.nom_modalidade,
            
            to_date(t.dat_transacao) as dat_referencia_transacao,
            t.vlr_transacao
            
        FROM local_catalog.db_cartoes.transacoes t
        LEFT JOIN local_catalog.db_dw.dim_associado a 
            ON t.num_cpf_cnpj = a.num_cpf_cnpj
        LEFT JOIN local_catalog.db_dw.dim_agencia ag 
            ON t.cod_cooperativa = ag.cod_cooperativa 
            AND t.cod_agencia = ag.cod_agencia
    """
    
    df_fato = spark.sql(query)
    df_fato.write.format("iceberg").mode("overwrite").saveAsTable("local_catalog.db_dw.fato_transacoes")
    
    print("\nModelagem Dimensional construída com sucesso!")
    
    print("\nAmostra da Fato Transações:")
    spark.sql("SELECT * FROM local_catalog.db_dw.fato_transacoes LIMIT 5").show(truncate=False)

    print("\nAmostra da Dimensão Associados:")
    spark.sql("SELECT * FROM local_catalog.db_dw.dim_associado LIMIT 5").show(truncate=False)
    
    print("\nAmostra da Dimensão Agência:")
    spark.sql("SELECT * FROM local_catalog.db_dw.dim_agencia LIMIT 5").show(truncate=False)

if __name__ == "__main__":
    construir_modelo_dimensional(spark)
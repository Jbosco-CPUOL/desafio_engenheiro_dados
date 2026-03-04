from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ETL_Cooperativa_Desafio2") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local_catalog.type", "hadoop") \
    .config("spark.sql.catalog.local_catalog.warehouse", "spark-warehouse/iceberg") \
    .getOrCreate()

def criar_tabela_flat(spark):    
    query_sql = """
    WITH ultima_data AS (
        SELECT MAX(dat_transacao) AS data_ref 
        FROM local_catalog.db_cartoes.transacoes
    ),
    transacoes_ultimos_3_meses AS (
        SELECT 
            t.num_cpf_cnpj,
            t.nom_modalidade,
            t.dat_transacao,
            date_format(t.dat_transacao, 'yyyy-MM') AS mes_ano
        FROM local_catalog.db_cartoes.transacoes t
        CROSS JOIN ultima_data ud
        WHERE t.dat_transacao >= add_months(ud.data_ref, -3)
    ),
    indicadores_transacoes AS (
        SELECT 
            num_cpf_cnpj,
            
            MAX(CASE WHEN lower(nom_modalidade) = 'crédito' THEN 1 ELSE 0 END) AS ind_ativo_credito,
            
            MAX(CASE WHEN lower(nom_modalidade) = 'débito' THEN 1 ELSE 0 END) AS ind_ativo_debito,
            
            CASE WHEN COUNT(DISTINCT mes_ano) >= 3 THEN 1 ELSE 0 END AS ind_frequente
            
        FROM transacoes_ultimos_3_meses
        GROUP BY num_cpf_cnpj
    )
    SELECT 
        a.num_cpf_cnpj,
        a.des_nome_associado,
        a.dat_associacao,
        a.cod_faixa_renda,
        a.des_faixa_renda,
        COALESCE(i.ind_ativo_credito, 0) AS ind_ativo_credito,
        COALESCE(i.ind_ativo_debito, 0) AS ind_ativo_debito,
        COALESCE(i.ind_frequente, 0) AS ind_frequente
    FROM local_catalog.db_pessoa.associado a
    LEFT JOIN indicadores_transacoes i 
           ON a.num_cpf_cnpj = i.num_cpf_cnpj
    """
    
    print("Executando query SQL para gerar a tabela Flat...")
    df_flat = spark.sql(query_sql)
    
    df_flat.show(truncate=False)
    
    print("Salvando a tabela no Data Lake...")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local_catalog.db_relatorios")
    df_flat.write.format("iceberg") \
        .mode("overwrite") \
        .saveAsTable("local_catalog.db_relatorios.associado_flat")

if __name__ == "__main__":
    criar_tabela_flat(spark)
    print("Desafio 2 concluído com sucesso!")
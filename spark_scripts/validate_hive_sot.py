import sys
from pyspark.sql import SparkSession
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset

def main():
    # Inicializando SparkSession
    # Em um ambiente real, adicionariamos as configuracoes do S3 (hadoop.fs.s3a...)
    spark = SparkSession.builder \
        .appName("ValidateHivePartitioning") \
        .getOrCreate()
        
    s3_path = "s3a://my-bucket/hive_table/"
    
    # Simulando a leitura. Se falhar, criamos um DataFrame dummy para o laboratorio.
    try:
        df = spark.read.parquet(s3_path)
    except Exception as e:
        print(f"Aviso: Nao foi possivel ler do S3 ({s3_path}). Gerando dados dummy para o laboratorio.")
        data = [
            ("2023", "10", "24", "valor_A"),
            ("2023", "10", "25", "valor_B")
        ]
        columns = ["year", "month", "day", "dado"]
        df = spark.createDataFrame(data, columns)

    # Convertendo o Spark DataFrame para um SparkDFDataset do Great Expectations
    gx_df = SparkDFDataset(df)
    
    # Definindo e rodando as Expectations
    results = []
    
    # 1. As colunas de particao devem existir
    results.append(gx_df.expect_column_to_exist("year"))
    results.append(gx_df.expect_column_to_exist("month"))
    results.append(gx_df.expect_column_to_exist("day"))
    
    # 2. As colunas de particao nao podem ser nulas
    results.append(gx_df.expect_column_values_to_not_be_null("year"))
    results.append(gx_df.expect_column_values_to_not_be_null("month"))
    results.append(gx_df.expect_column_values_to_not_be_null("day"))

    # Verificando se alguma expectativa falhou
    failed = False
    print("=== Resultados da Validacao Hive ===")
    for res in results:
        success = res.success
        expectation_type = res.expectation_config.expectation_type
        column = res.expectation_config.kwargs.get('column', 'N/A')
        print(f"Regra: {expectation_type} na coluna '{column}' -> Sucesso: {success}")
        
        if not success:
            failed = True
            
    if failed:
        print("Validacao Hive Falhou.")
        sys.exit(1)
    else:
        print("Validacao Hive Concluida com Sucesso.")
        sys.exit(0)

if __name__ == "__main__":
    main()

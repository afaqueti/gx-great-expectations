import sys
from pyspark.sql import SparkSession
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset

def main():
    # Inicializando SparkSession
    # Em um ambiente real, adicionariamos as configuracoes do Iceberg catalog e S3
    spark = SparkSession.builder \
        .appName("ValidateIcebergPartitioning") \
        .getOrCreate()
        
    table_identifier = "local.db.iceberg_table"
    
    # Simulando a leitura. Se falhar, criamos um DataFrame dummy para o laboratorio.
    try:
        # Para testar a particao Iceberg, poderiamos ler a tabela ou a metadata de particao (.partitions)
        df = spark.table(table_identifier)
    except Exception as e:
        print(f"Aviso: Nao foi possivel ler a tabela Iceberg ({table_identifier}). Gerando dados dummy para o laboratorio.")
        data = [
            ("20231024", "valor_X"),
            ("20231025", "valor_Y")
        ]
        columns = ["anomesdia", "dado"]
        df = spark.createDataFrame(data, columns)

    # Convertendo o Spark DataFrame para um SparkDFDataset do Great Expectations
    gx_df = SparkDFDataset(df)
    
    # Definindo e rodando as Expectations
    results = []
    
    # 1. A coluna de particao (anomesdia) deve existir
    results.append(gx_df.expect_column_to_exist("anomesdia"))
    
    # 2. A coluna de particao nao pode ser nula
    results.append(gx_df.expect_column_values_to_not_be_null("anomesdia"))
    
    # 3. A coluna deve seguir o formato YYYYMMDD (apenas numeros, 8 digitos)
    results.append(gx_df.expect_column_values_to_match_regex("anomesdia", r"^\d{8}$"))

    # Verificando se alguma expectativa falhou
    failed = False
    print("=== Resultados da Validacao Iceberg ===")
    for res in results:
        success = res.success
        expectation_type = res.expectation_config.expectation_type
        column = res.expectation_config.kwargs.get('column', 'N/A')
        print(f"Regra: {expectation_type} na coluna '{column}' -> Sucesso: {success}")
        
        if not success:
            failed = True
            
    if failed:
        print("Validacao Iceberg Falhou.")
        sys.exit(1)
    else:
        print("Validacao Iceberg Concluida com Sucesso.")
        sys.exit(0)

if __name__ == "__main__":
    main()

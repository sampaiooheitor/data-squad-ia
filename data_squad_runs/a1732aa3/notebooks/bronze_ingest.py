from pyspark.sql import SparkSession
import pandas as pd
import io

spark = SparkSession.builder.getOrCreate()

csv_data = """ID_ENTREGA|NM_DESTINATARIO|CD_CPF_DESTINATARIO|DT_POSTAGEM|DT_ENTREGA_PREVISTA|VL_FRETE|CD_STATUS_ENTREGA|FL_ENTREGA_REALIZADA
1001|Carlos Alberto Mendes|312.456.789-01|2024-01-03|2024-01-07|28.50|ENTREGUE|true
1002|Fernanda Souza Lima|423.567.890-12|2024-01-04|2024-01-09|15.90|ENTREGUE|true
1003|Roberto Alves Pereira|534.678.901-23|2024-01-05|2024-01-10|42.00|DEVOLVIDO|false
1004|Mariana Costa Ribeiro|645.789.012-34|2024-01-06|2024-01-11|19.75|ENTREGUE|true
1005|Joao Paulo Ferreira|756.890.123-45|2024-01-07|2024-01-12|33.20|EM_TRANSITO|false
1006|Patricia Nunes Oliveira|867.901.234-56|2024-01-08|2024-01-13|22.40|PENDENTE|false
1007|Lucas Henrique Santos|978.012.345-67|2024-01-09|2024-01-14|55.80|ENTREGUE|true
1008|Beatriz Campos Teixeira|089.123.456-78|2024-01-10|2024-01-15|18.60|ENTREGUE|true
1009|Andre Luis Barbosa|190.234.567-89|2024-01-11|2024-01-16|29.90|DEVOLVIDO|false
1010|Camila Rodrigues Dias|201.345.678-90|2024-01-12|2024-01-17|14.50|ENTREGUE|true
1011|Felipe Augusto Martins|312.456.789-02|2024-01-13|2024-01-18|67.30|EM_TRANSITO|false
1012|Juliana Moreira Castro|423.567.890-13|2024-01-14|2024-01-19|38.00|ENTREGUE|true
1013|Renato Gomes Carvalho|534.678.901-24|2024-01-15|2024-01-20|21.75|PENDENTE|false
1014|Amanda Freitas Silva|645.789.012-35|2024-01-16|2024-01-21|44.90|ENTREGUE|true
1015|Gustavo Lopes Nascimento|756.890.123-46|2024-01-17|2024-01-22|12.80|ENTREGUE|true
1016|Tatiane Vieira Cardoso|867.901.234-57|2024-01-18|2024-01-23|53.60|DEVOLVIDO|false
1017|Diego Ramos Araujo|978.012.345-68|2024-01-19|2024-01-24|31.20|ENTREGUE|true
1018|Larissa Pinto Melo|089.123.456-79|2024-01-20|2024-01-25|26.40|EM_TRANSITO|false
1019|Thiago Borges Monteiro|190.234.567-80|2024-01-21|2024-01-26|48.70|ENTREGUE|true
1020|Vanessa Cruz Rocha|201.345.678-91|2024-01-22|2024-01-27|17.30|PENDENTE|false
1021|Marcos Vinicius Cunha|312.456.789-03|2024-01-23|2024-01-28|35.50|ENTREGUE|true
1022|Isabela Farias Correia|423.567.890-14|2024-01-24|2024-01-29|23.90|ENTREGUE|true
1023|Rodrigo Tavares Duarte|534.678.901-25|2024-01-25|2024-01-30|61.00|DEVOLVIDO|false
1024|Aline Monteiro Bezerra|645.789.012-36|2024-01-26|2024-01-31|14.20|ENTREGUE|true
1025|Bruno Cesar Cavalcanti|756.890.123-47|2024-01-27|2024-02-01|39.80|EM_TRANSITO|false
1026|Priscila Andrade Fonseca|867.901.234-58|2024-01-28|2024-02-02|27.60|ENTREGUE|true
1027|Leandro Queiroz Moura|978.012.345-69|2024-01-29|2024-02-03|52.40|PENDENTE|false
1028|Natalia Brito Guimaraes|089.123.456-70|2024-01-30|2024-02-04|18.90|ENTREGUE|true
1029|Victor Hugo Medeiros|190.234.567-81|2024-01-31|2024-02-05|43.10|ENTREGUE|true
1030|Simone Almeida Pacheco|201.345.678-92|2024-02-01|2024-02-06|36.70|DEVOLVIDO|false
1031|Henrique Nogueira Pires|312.456.789-04|2024-02-02|2024-02-07|25.00|ENTREGUE|true
1032|Gabriela Macedo Leal|423.567.890-15|2024-02-03|2024-02-08|57.30|EM_TRANSITO|false
1033|Eduardo Mendonca Esteves|534.678.901-26|2024-02-04|2024-02-09|19.50|ENTREGUE|true
1034|Flavia Azevedo Sampaio|645.789.012-37|2024-02-05|2024-02-10|33.80|ENTREGUE|true
1035|Sergio Batista Peixoto|756.890.123-48|2024-02-06|2024-02-11|46.20|DEVOLVIDO|false
1036|Cristiane Figueiredo Vaz|867.901.234-59|2024-02-07|2024-02-12|22.10|ENTREGUE|true
1037|Mauricio Siqueira Neto|978.012.345-60|2024-02-08|2024-02-13|38.90|EM_TRANSITO|false
1038|Elaine Teles Braga|089.123.456-71|2024-02-09|2024-02-14|15.70|ENTREGUE|true
1039|Alexandre Cardoso Matos|190.234.567-82|2024-02-10|2024-02-15|59.40|PENDENTE|false
1040|Debora Vilas Boas|201.345.678-93|2024-02-11|2024-02-16|28.00|ENTREGUE|true
"""

print("[BRONZE] Parsing CSV data with pandas...")
pdf = pd.read_csv(io.StringIO(csv_data), sep='|')
print(f"[BRONZE] Pandas dataframe shape: {pdf.shape}")

sdf = spark.createDataFrame(pdf)
print("[BRONZE] Spark DataFrame created successfully.")
print("[BRONZE] Schema:")
sdf.printSchema()

bronze_table = "bronze_TBLOG_ENTREGAS_a1732aa3"
print(f"[BRONZE] Writing to Delta table: {bronze_table} ...")
sdf.write.format("delta").mode("overwrite").saveAsTable(bronze_table)

row_count = spark.read.table(bronze_table).count()
print(f"[BRONZE] Write complete. Row count in '{bronze_table}': {row_count}")

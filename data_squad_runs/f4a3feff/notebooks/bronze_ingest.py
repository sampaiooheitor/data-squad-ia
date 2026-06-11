from pyspark.sql import SparkSession
import pandas as pd
import io

spark = SparkSession.builder.getOrCreate()

# Full CSV data embedded as a multi-line string
csv_data = """ID_TRANSACAO|NM_CLIENTE|CD_CPF|CD_TIPO_TRANSACAO|VL_TRANSACAO|DT_TRANSACAO|FL_ATIVO|CD_AGENCIA
1|Mariana Souza Ferreira|312.456.789-01|PIX|1500.00|2024-01-03|true|AG-001
2|Carlos Eduardo Lima|423.567.890-12|TED|8750.50|2024-01-05|true|AG-002
3|Fernanda Costa Alves|534.678.901-23|CREDITO|320.75|2024-01-07|true|AG-003
4|Roberto Mendes Nunes|645.789.012-34|DEBITO|210.00|2024-01-08|false|AG-001
5|Juliana Pires Rocha|756.890.123-45|DOC|4200.00|2024-01-10|true|AG-004
6|Anderson Teixeira Gomes|867.901.234-56|PIX|980.25|2024-01-11|true|AG-002
7|Patricia Oliveira Santos|978.012.345-67|TED|15000.00|2024-01-13|true|AG-005
8|Gustavo Barbosa Cardoso|089.123.456-78|CREDITO|540.90|2024-01-14|true|AG-003
9|Renata Vieira Monteiro|190.234.567-89|DEBITO|125.50|2024-01-15|true|AG-006
10|Felipe Araújo Correia|201.345.678-90|PIX|2300.00|2024-01-17|false|AG-001
11|Larissa Martins Cruz|312.456.780-11|DOC|6700.00|2024-01-18|true|AG-007
12|Thiago Ribeiro Lopes|423.567.891-22|TED|3200.75|2024-01-20|true|AG-002
13|Camila Nascimento Dias|534.678.902-33|PIX|450.00|2024-01-21|true|AG-004
14|Daniel Freitas Borges|645.789.013-44|CREDITO|1890.00|2024-01-22|true|AG-008
15|Vanessa Carvalho Pinto|756.890.124-55|DEBITO|330.20|2024-01-23|false|AG-003
16|Bruno Almeida Farias|867.901.235-66|PIX|5600.00|2024-01-25|true|AG-005
17|Aline Pereira Duarte|978.012.346-77|TED|9800.50|2024-01-26|true|AG-006
18|Marcelo Cunha Tavares|089.123.457-88|DOC|1200.00|2024-01-28|true|AG-001
19|Simone Ramos Azevedo|190.234.568-99|CREDITO|780.40|2024-01-29|true|AG-009
20|Leonardo Moura Batista|201.345.679-01|DEBITO|95.00|2024-01-30|true|AG-002
21|Tatiane Leite Campos|312.456.781-12|PIX|3400.00|2024-02-01|true|AG-007
22|Rodrigo Fonseca Melo|423.567.892-23|TED|12500.00|2024-02-03|false|AG-004
23|Beatriz Andrade Silveira|534.678.903-34|PIX|670.00|2024-02-04|true|AG-003
24|Henrique Cavalcante Reis|645.789.014-45|CREDITO|4500.80|2024-02-06|true|AG-010
25|Monica Gonçalves Teles|756.890.125-56|DEBITO|215.60|2024-02-07|true|AG-001
26|Alexandre Queiroz Braga|867.901.236-67|DOC|8900.00|2024-02-09|true|AG-005
27|Sabrina Luz Machado|978.012.347-78|PIX|1100.25|2024-02-10|true|AG-002
28|Eduardo Sampaio Viana|089.123.458-89|TED|27000.00|2024-02-12|true|AG-008
29|Priscila Lacerda Nogueira|190.234.569-00|CREDITO|890.00|2024-02-13|false|AG-006
30|Mauricio Stein Becker|201.345.670-02|DEBITO|410.75|2024-02-14|true|AG-009
31|Luciana Prado Amaral|312.456.782-13|PIX|2200.00|2024-02-16|true|AG-003
32|Rafael Guimarães Paiva|423.567.893-24|DOC|5100.50|2024-02-17|true|AG-007
33|Cristina Bastos Valente|534.678.904-35|TED|6300.00|2024-02-19|true|AG-001
34|Jonas Ferreira Rezende|645.789.015-46|PIX|750.00|2024-02-20|true|AG-004
35|Elaine Moreira Siqueira|756.890.126-57|CREDITO|1340.90|2024-02-22|false|AG-002
36|Victor Hugo Esteves|867.901.237-68|DEBITO|185.00|2024-02-23|true|AG-010
37|Natalia Coelho Barros|978.012.348-79|PIX|4800.00|2024-02-25|true|AG-005
38|Diego Leal Magalhães|089.123.459-90|TED|18500.00|2024-02-26|true|AG-006
39|Isabel Rocha Pimentel|190.234.570-01|CREDITO|660.00|2024-02-28|true|AG-003
40|Fábio Nunes Drummond|201.345.671-03|DEBITO|290.50|2024-02-29|true|AG-001
41|Clarice Borba Spinelli|312.456.783-14|PIX|5900.00|2024-03-02|true|AG-007
42|Leandro Castro Villela|423.567.894-25|DOC|3700.80|2024-03-03|false|AG-002
43|Adriana Macedo Torres|534.678.905-36|TED|11200.00|2024-03-05|true|AG-008
44|Sergio Lemos Queiroga|645.789.016-47|PIX|820.00|2024-03-06|true|AG-004
45|Marcia Fontes Britto|756.890.127-58|CREDITO|1750.40|2024-03-08|true|AG-009
46|Danilo Vasconcelos Hora|867.901.238-69|DEBITO|440.25|2024-03-09|true|AG-005
47|Renato Siqueira Paes|978.012.349-80|PIX|6200.00|2024-03-11|true|AG-001
48|Flavia Teixeira Ponte|089.123.460-01|TED|22000.00|2024-03-12|false|AG-006
49|Paulo Menezes Junqueira|190.234.571-12|DOC|930.00|2024-03-14|true|AG-003
50|Sandra Aguiar Lustosa|201.345.672-04|CREDITO|2100.60|2024-03-15|true|AG-010"""

# Parse CSV with pandas using pipe separator
pdf = pd.read_csv(io.StringIO(csv_data), sep='|', dtype=str)

print(f"Pandas DataFrame shape: {pdf.shape}")
print(pdf.dtypes)

# Create Spark DataFrame from pandas DataFrame
sdf = spark.createDataFrame(pdf)

print("Spark DataFrame schema:")
sdf.printSchema()

# Write to Bronze Delta table
bronze_table = "prd.bronze.TBFIN_TRANSACOES"

sdf.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(bronze_table)

# Log row count after write
row_count = spark.read.table(bronze_table).count()
print(f"[BRONZE] Table '{bronze_table}' written successfully. Row count: {row_count}")

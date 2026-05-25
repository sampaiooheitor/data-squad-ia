from pyspark.sql import SparkSession
import pandas as pd
import io

spark = SparkSession.builder.getOrCreate()

# Embedded CSV data
csv_data = """id_entrega,nome_destinatario,cpf_destinatario,email_destinatario,cidade_destino,status_entrega,peso_kg,data_entrega
ENT-00001,Carlos Alberto Mendes,312.456.789-01,carlos.mendes@gmail.com,São Paulo,Entregue,3.5,2024-01-03
ENT-00002,Fernanda Lopes Carvalho,423.567.890-12,fernanda.lopes@hotmail.com,Rio de Janeiro,Entregue,1.2,2024-01-04
ENT-00003,Rodrigo Tavares Silva,534.678.901-23,rodrigo.tavares@yahoo.com,Belo Horizonte,Em Trânsito,7.8,2024-01-05
ENT-00004,Aline Costa Ferreira,645.789.012-34,aline.costa@email.com,Curitiba,Entregue,2.3,2024-01-06
ENT-00005,Marcelo Souza Nunes,756.890.123-45,marcelo.souza@gmail.com,Porto Alegre,Atrasado,5.6,2024-01-07
ENT-00006,Juliana Martins Rocha,867.901.234-56,juliana.martins@hotmail.com,Salvador,Entregue,0.9,2024-01-08
ENT-00007,Paulo Henrique Alves,978.012.345-67,paulo.alves@email.com,Fortaleza,Em Trânsito,4.1,2024-01-09
ENT-00008,Tatiana Borges Lima,089.123.456-78,tatiana.borges@gmail.com,Manaus,Entregue,6.3,2024-01-10
ENT-00009,Diego Fonseca Pereira,190.234.567-89,diego.fonseca@yahoo.com,Recife,Cancelado,2.7,2024-01-11
ENT-00010,Camila Ribeiro Santos,201.345.678-90,camila.ribeiro@hotmail.com,Goiânia,Entregue,3.0,2024-01-12
ENT-00011,Alexandre Moreira Costa,312.456.789-02,alexandre.moreira@gmail.com,Florianópolis,Em Trânsito,8.4,2024-01-13
ENT-00012,Beatriz Neves Cardoso,423.567.890-13,beatriz.neves@email.com,Vitória,Entregue,1.5,2024-01-14
ENT-00013,Lucas Oliveira Barros,534.678.901-24,lucas.oliveira@gmail.com,Campo Grande,Atrasado,4.9,2024-01-15
ENT-00014,Priscila Duarte Melo,645.789.012-35,priscila.duarte@hotmail.com,Cuiabá,Entregue,2.1,2024-01-16
ENT-00015,Thiago Correia Araújo,756.890.123-46,thiago.correia@yahoo.com,Natal,Entregue,6.7,2024-01-17
ENT-00016,Vanessa Pinto Vieira,867.901.234-57,vanessa.pinto@email.com,Maceió,Em Trânsito,3.3,2024-01-18
ENT-00017,Gustavo Freitas Moura,978.012.345-68,gustavo.freitas@gmail.com,Teresina,Entregue,5.0,2024-01-19
ENT-00018,Larissa Campos Teixeira,089.123.456-79,larissa.campos@hotmail.com,João Pessoa,Cancelado,1.8,2024-01-20
ENT-00019,Rafael Cunha Sampaio,190.234.567-90,rafael.cunha@email.com,Aracaju,Entregue,7.2,2024-01-21
ENT-00020,Mônica Azevedo Lacerda,201.345.678-01,monica.azevedo@gmail.com,Porto Velho,Em Trânsito,4.4,2024-01-22
ENT-00021,Fábio Siqueira Ramos,312.456.789-03,fabio.siqueira@yahoo.com,Macapá,Entregue,2.6,2024-01-23
ENT-00022,Renata Magalhães Cruz,423.567.890-14,renata.magalhaes@hotmail.com,Boa Vista,Atrasado,9.1,2024-01-24
ENT-00023,Eduardo Batista Coelho,534.678.901-25,eduardo.batista@gmail.com,Rio Branco,Entregue,3.8,2024-01-25
ENT-00024,Simone Andrade Peixoto,645.789.012-36,simone.andrade@email.com,Palmas,Em Trânsito,5.5,2024-01-26
ENT-00025,Henrique Vasconcelos Dias,756.890.123-47,henrique.vasconcelos@gmail.com,São Luís,Entregue,1.1,2024-01-27
ENT-00026,Patrícia Monteiro Guedes,867.901.234-58,patricia.monteiro@hotmail.com,Belém,Entregue,6.0,2024-01-28
ENT-00027,Anderson Gomes Figueiredo,978.012.345-69,anderson.gomes@email.com,São Paulo,Em Trânsito,4.7,2024-01-29
"""

# Parse CSV with pandas
pdf = pd.read_csv(io.StringIO(csv_data))

# Ensure all columns are read as strings to preserve raw data in bronze
pdf = pdf.astype(str)

# Replace pandas NA string representations with None
pdf = pdf.where(pd.notnull(pdf), None)

print(f"Pandas DataFrame shape: {pdf.shape}")
print(pdf.dtypes)

# Create Spark DataFrame from pandas DataFrame
sdf = spark.createDataFrame(pdf)

print("Spark DataFrame schema:")
sdf.printSchema()

# Write to Bronze Delta table
bronze_table_name = "bronze_entregas_logistica_00012ba4"

sdf.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(bronze_table_name)

# Log row count after write
row_count = spark.read.table(bronze_table_name).count()
print(f"[BRONZE] Table '{bronze_table_name}' written successfully. Row count: {row_count}")

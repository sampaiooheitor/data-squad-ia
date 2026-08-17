from pyspark.sql import SparkSession
import pandas as pd
import io

spark = SparkSession.builder.getOrCreate()

CSV_DATA = """id_entrega,nome_cliente,cpf_cliente,email_cliente,cidade_destino,status_entrega,data_envio,peso_kg
ENT-00001,Carlos Mendes Oliveira,312.456.789-01,carlos.oliveira@email.com,São Paulo,Entregue,2024-01-03,2.5
ENT-00002,Fernanda Lima Souza,423.567.890-12,fernanda.lima@email.com,Rio de Janeiro,Em trânsito,2024-01-04,1.2
ENT-00003,Roberto Alves Pereira,534.678.901-23,roberto.alves@email.com,Belo Horizonte,Entregue,2024-01-05,4.7
ENT-00004,Juliana Costa Martins,645.789.012-34,juliana.costa@email.com,Curitiba,Pendente,2024-01-06,0.8
ENT-00005,Marcos Henrique Dias,756.890.123-45,marcos.dias@email.com,Porto Alegre,Entregue,2024-01-07,3.3
ENT-00006,Ana Paula Ferreira,867.901.234-56,ana.ferreira@email.com,Salvador,Cancelado,2024-01-08,5.1
ENT-00007,Lucas Rodrigues Neto,978.012.345-67,lucas.rodrigues@email.com,Fortaleza,Entregue,2024-01-09,2.0
ENT-00008,Patricia Gomes Vieira,089.123.456-78,patricia.gomes@email.com,Manaus,Em trânsito,2024-01-10,6.4
ENT-00009,Eduardo Ribeiro Campos,190.234.567-89,eduardo.ribeiro@email.com,Recife,Entregue,2024-01-11,1.9
ENT-00010,Camila Nascimento Braga,201.345.678-90,camila.nascimento@email.com,Goiânia,Pendente,2024-01-12,3.7
ENT-00011,Thiago Barbosa Lima,312.456.789-01,thiago.barbosa@email.com,Brasília,Entregue,2024-01-13,2.2
ENT-00012,Leticia Cardoso Pinto,423.567.890-12,leticia.cardoso@email.com,Belém,Em trânsito,2024-01-14,4.0
ENT-00013,Felipe Moreira Santos,534.678.901-23,felipe.moreira@email.com,São Luís,Entregue,2024-01-15,1.5
ENT-00014,Vanessa Teixeira Cruz,645.789.012-34,vanessa.teixeira@email.com,Maceió,Cancelado,2024-01-16,7.8
ENT-00015,Gabriel Antunes Rocha,756.890.123-45,gabriel.antunes@email.com,Natal,Entregue,2024-01-17,2.9
ENT-00016,Aline Fonseca Duarte,867.901.234-56,aline.fonseca@email.com,Teresina,Em trânsito,2024-01-18,3.1
ENT-00017,Diego Cavalcanti Melo,978.012.345-67,diego.cavalcanti@email.com,Campo Grande,Entregue,2024-01-19,5.5
ENT-00018,Renata Azevedo Lopes,089.123.456-78,renata.azevedo@email.com,Cuiabá,Pendente,2024-01-20,0.6
ENT-00019,Bruno Machado Freitas,190.234.567-89,bruno.machado@email.com,Porto Velho,Entregue,2024-01-21,4.3
ENT-00020,Isabela Cunha Ramos,201.345.678-90,isabela.cunha@email.com,Macapá,Em trânsito,2024-01-22,2.7
ENT-00021,Rodrigo Bittencourt Vale,312.456.789-11,rodrigo.bittencourt@email.com,Florianópolis,Entregue,2024-01-23,3.9
ENT-00022,Mariana Peixoto Abreu,423.567.890-22,mariana.peixoto@email.com,Vitória,Cancelado,2024-01-24,1.1
ENT-00023,Gustavo Aragão Matos,534.678.901-33,gustavo.aragao@email.com,João Pessoa,Entregue,2024-01-25,6.2
ENT-00024,Simone Lacerda Queiroz,645.789.012-44,simone.lacerda@email.com,Aracaju,Em trânsito,2024-01-26,2.4
ENT-00025,Alexandre Nunes Correia,756.890.123-55,alexandre.nunes@email.com,Rio Branco,Pendente,2024-01-27,5.0
ENT-00026,Tatiana Salles Borges,867.901.234-66,tatiana.salles@email.com,Palmas,Entregue,2024-01-28,3.6
ENT-00027,Henrique Vasconcelos Maia,978.012.345-77,henrique.vasconcelos@email.com,Boa Vista,Em trânsito,2024-01-29,1.8
ENT-00028,Priscila Drummond Faria,089.123.456-88,priscila.drummond@email.com,Macapá,Pendente,2024-01-30,4.1
ENT-00029,Leonardo Silveira Prado,190.234.567-99,leonardo.silveira@email.com,São Paulo,Entregue,2024-01-31,2.6
ENT-00030,Daniela Monteiro Esteves,201.345.678-00,daniela.monteiro@email.com,Rio de Janeiro,Cancelado,2024-02-01,3.2
"""

BRONZE_TABLE = "bronze_logistica_entregas_37674643"

print("[Bronze] Parsing CSV data...")
pdf = pd.read_csv(io.StringIO(CSV_DATA.strip()))
print(f"[Bronze] Rows parsed from CSV: {len(pdf)}")

df = spark.createDataFrame(pdf)

print(f"[Bronze] Writing to Delta table: {BRONZE_TABLE}")
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(BRONZE_TABLE)

row_count = spark.read.table(BRONZE_TABLE).count()
print(f"[Bronze] Rows written to {BRONZE_TABLE}: {row_count}")
print("[Bronze] Notebook completed successfully.")

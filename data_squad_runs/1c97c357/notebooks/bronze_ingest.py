from pyspark.sql import SparkSession
import pandas as pd
import io

spark = SparkSession.builder.getOrCreate()

CSV_DATA = """id_transacao,nome_completo,cpf,email,tipo_transacao,valor,data_transacao,status
TXN-00001,Ana Paula Ferreira,123.456.789-01,ana.ferreira@email.com,Crédito,1500.00,2024-01-05,Aprovada
TXN-00002,Bruno Henrique Souza,234.567.890-02,bruno.souza@email.com,Débito,320.50,2024-01-06,Aprovada
TXN-00003,Carla Mendes Lima,345.678.901-03,carla.lima@webmail.com,Transferência,870.00,2024-01-07,Pendente
TXN-00004,Diego Alves Rocha,456.789.012-04,diego.rocha@correio.com,Crédito,2450.75,2024-01-08,Aprovada
TXN-00005,Elaine Costa Pinto,567.890.123-05,elaine.pinto@email.com,Débito,150.00,2024-01-09,Recusada
TXN-00006,Fábio Nascimento Gomes,678.901.234-06,fabio.gomes@mail.com,PIX,980.00,2024-01-10,Aprovada
TXN-00007,Gabriela Torres Vieira,789.012.345-07,gabriela.vieira@email.com,Crédito,3200.00,2024-01-11,Aprovada
TXN-00008,Henrique Batista Cunha,890.123.456-08,henrique.cunha@webmail.com,Débito,430.25,2024-01-12,Pendente
TXN-00009,Isabela Ramos Freitas,901.234.567-09,isabela.freitas@correio.com,PIX,560.00,2024-01-13,Aprovada
TXN-00010,João Pedro Oliveira,012.345.678-10,joao.oliveira@mail.com,Transferência,1100.00,2024-01-14,Aprovada
TXN-00011,Karen Silva Monteiro,123.654.987-11,karen.monteiro@email.com,Crédito,780.50,2024-01-15,Recusada
TXN-00012,Leonardo Pereira Castro,234.765.098-12,leonardo.castro@email.com,Débito,220.00,2024-01-16,Aprovada
TXN-00013,Mariana Albuquerque,345.876.209-13,mariana.albuquerque@webmail.com,PIX,1350.00,2024-01-17,Aprovada
TXN-00014,Nicolas Ferraz Teixeira,456.987.310-14,nicolas.teixeira@correio.com,Transferência,490.75,2024-01-18,Pendente
TXN-00015,Olívia Duarte Sampaio,567.098.421-15,olivia.sampaio@mail.com,Crédito,5000.00,2024-01-19,Aprovada
TXN-00016,Paulo Vitor Barbosa,678.109.532-16,paulo.barbosa@email.com,Débito,310.00,2024-01-20,Aprovada
TXN-00017,Quésia Lopes Andrade,789.210.643-17,quesia.andrade@email.com,PIX,740.00,2024-01-21,Aprovada
TXN-00018,Rafael Moreira Nunes,890.321.754-18,rafael.nunes@webmail.com,Crédito,1875.00,2024-01-22,Recusada
TXN-00019,Sabrina Cardoso Luz,901.432.865-19,sabrina.luz@correio.com,Transferência,620.00,2024-01-23,Aprovada
TXN-00020,Thiago Martins Correia,012.543.976-20,thiago.correia@mail.com,Débito,95.50,2024-01-24,Aprovada
TXN-00021,Ursula Figueiredo Prado,123.654.087-21,ursula.prado@email.com,PIX,2100.00,2024-01-25,Pendente
TXN-00022,Vinícius Araújo Campos,234.765.198-22,vinicius.campos@email.com,Crédito,4300.00,2024-01-26,Aprovada
TXN-00023,Wanessa Ribeiro Melo,345.876.309-23,wanessa.melo@webmail.com,Débito,175.25,2024-01-27,Aprovada
TXN-00024,Xavier Guimarães Leal,456.987.410-24,xavier.leal@correio.com,Transferência,830.00,2024-01-28,Aprovada
TXN-00025,Yasmin Tavares Brito,567.098.521-25,yasmin.brito@mail.com,PIX,410.00,2024-01-29,Recusada
TXN-00026,Zélio Costa Brandão,678.109.632-26,zelio.brandao@email.com,Crédito,950.00,2024-01-30,Aprovada
TXN-00027,Amanda Silveira Paiva,789.210.743-27,amanda.paiva@email.com,Débito,540.75,2024-01-31,Aprovada
TXN-00028,Bernardo Fonseca Cruz,890.321.854-28,bernardo.cruz@webmail.com,PIX,1200.00,2024-02-01,Aprovada
TXN-00029,Cecília Drummond Neto,901.432.965-29,cecilia.neto@correio.com,Crédito,670.00,2024-02-02,Pendente
TXN-00030,Daniel Augusto Braga,012.543.076-30,daniel.braga@mail.com,Débito,88.00,2024-02-03,Aprovada
TXN-00031,Eduarda Pinheiro Vaz,123.654.187-31,eduarda.vaz@email.com,Transferência,3450.00,2024-02-04,Aprovada
TXN-00032,Felipe Quaresma Dias,234.765.298-32,felipe.dias@email.com,PIX,225.00,2024-02-05,Recusada
TXN-00033,Giovanna Lacerda Souto,345.876.409-33,giovanna.souto@webmail.com,Crédito,1690.00,2024-02-06,Aprovada
TXN-00034,Humberto Cavalcante,456.987.510-34,humberto.cavalcante@correio.com,Débito,760.50,2024-02-07,Aprovada
TXN-00035,Ingrid Moraes Leite,567.098.621-35,ingrid.leite@mail.com,PIX,310.00,2024-02-08,Pendente
TXN-00036,Jonas Queiroz Abreu,678.109.732-36,jonas.abreu@email.com,Transferência,920.00,2024-02-09,Aprovada
TXN-00037,Larissa Nogueira Cury,789.210.843-37,larissa.cury@email.com,Crédito,5500.00,2024-02-10,Aprovada
TXN-00038,Marcos Vinicius Lago,890.321.954-38,marcos.lago@webmail.com,Débito,445.25,2024-02-11,Aprovada
TXN-00039,Natália Bezerra Forte,901.432.065-39,natalia.forte@correio.com,PIX,130.00,2024-02-12,Recusada
TXN-00040,Otávio Lins Pacheco,012.543.176-40,otavio.pacheco@mail.com,Crédito,2780.00,2024-02-13,Aprovada
"""

print("[Bronze] Parsing CSV data with pandas...")
pdf = pd.read_csv(io.StringIO(CSV_DATA))
print(f"[Bronze] Rows parsed from CSV: {len(pdf)}")

df = spark.createDataFrame(pdf)

BRONZE_TABLE = "bronze_transacoes_financeiras_1c97c357"

print(f"[Bronze] Writing to Delta table: {BRONZE_TABLE}...")
df.write.format("delta").mode("overwrite").saveAsTable(BRONZE_TABLE)

row_count = spark.read.table(BRONZE_TABLE).count()
print(f"[Bronze] Write complete. Row count in '{BRONZE_TABLE}': {row_count}")

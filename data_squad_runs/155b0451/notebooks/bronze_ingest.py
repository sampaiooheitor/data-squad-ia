from pyspark.sql import SparkSession
import pandas as pd
import io

spark = SparkSession.builder.getOrCreate()

csv_data = """id_transacao,nome_completo,cpf,email,tipo_transacao,valor,data_transacao,status
TXN-00001,Marcelo Augusto Ferreira,312.654.987-01,marcelo.ferreira@email.com,Transferência,1500.00,2024-01-03 08:12:00,Aprovada
TXN-00002,Juliana Cristina Souza,423.765.098-12,juliana.souza@webmail.com,Pagamento,320.50,2024-01-04 09:45:00,Aprovada
TXN-00003,Roberto Carlos Mendes,534.876.109-23,roberto.mendes@correio.com,Saque,800.00,2024-01-05 11:00:00,Aprovada
TXN-00004,Fernanda Lima Pereira,645.987.210-34,fernanda.pereira@inbox.com,Depósito,2500.00,2024-01-06 13:30:00,Aprovada
TXN-00005,Gustavo Henrique Alves,756.098.321-45,gustavo.alves@mailbox.com,Transferência,450.75,2024-01-07 14:20:00,Pendente
TXN-00006,Camila Rodrigues Costa,867.109.432-56,camila.costa@email.com,Pagamento,199.90,2024-01-08 15:10:00,Aprovada
TXN-00007,Thiago Barbosa Nunes,978.210.543-67,thiago.nunes@webmail.com,Saque,1200.00,2024-01-09 16:00:00,Recusada
TXN-00008,Patrícia Oliveira Santos,089.321.654-78,patricia.santos@correio.com,Depósito,3400.00,2024-01-10 10:05:00,Aprovada
TXN-00009,Leonardo Carvalho Lima,190.432.765-89,leonardo.lima@inbox.com,Pagamento,560.00,2024-01-11 11:30:00,Aprovada
TXN-00010,Aline Martins Ribeiro,201.543.876-90,aline.ribeiro@mailbox.com,Transferência,875.25,2024-01-12 12:45:00,Aprovada
TXN-00011,Bruno Ferreira Gomes,312.654.987-02,bruno.gomes@email.com,Saque,600.00,2024-01-13 09:00:00,Aprovada
TXN-00012,Vanessa Souza Dias,423.765.098-13,vanessa.dias@webmail.com,Pagamento,145.00,2024-01-14 10:20:00,Pendente
TXN-00013,Felipe Mendes Teixeira,534.876.109-24,felipe.teixeira@correio.com,Depósito,7800.00,2024-01-15 14:55:00,Aprovada
TXN-00014,Larissa Pereira Rocha,645.987.210-35,larissa.rocha@inbox.com,Transferência,230.00,2024-01-16 15:40:00,Aprovada
TXN-00015,Renato Alves Cardoso,756.098.321-46,renato.cardoso@mailbox.com,Pagamento,980.50,2024-01-17 08:30:00,Recusada
TXN-00016,Isabela Costa Vieira,867.109.432-57,isabela.vieira@email.com,Saque,400.00,2024-01-18 09:15:00,Aprovada
TXN-00017,Diego Nunes Batista,978.210.543-68,diego.batista@webmail.com,Depósito,1100.00,2024-01-19 10:50:00,Aprovada
TXN-00018,Mônica Santos Araújo,089.321.654-79,monica.araujo@correio.com,Transferência,3200.00,2024-01-20 13:00:00,Aprovada
TXN-00019,Eduardo Lima Pinto,190.432.765-90,eduardo.pinto@inbox.com,Pagamento,67.30,2024-01-21 14:10:00,Aprovada
TXN-00020,Simone Ribeiro Castro,201.543.876-91,simone.castro@mailbox.com,Saque,950.00,2024-01-22 16:25:00,Pendente
TXN-00021,Rodrigo Gomes Freitas,312.654.987-03,rodrigo.freitas@email.com,Depósito,5600.00,2024-01-23 08:00:00,Aprovada
TXN-00022,Tatiane Dias Monteiro,423.765.098-14,tatiane.monteiro@webmail.com,Transferência,720.00,2024-01-24 09:35:00,Aprovada
TXN-00023,Claudio Teixeira Correia,534.876.109-25,claudio.correia@correio.com,Pagamento,430.80,2024-01-25 11:20:00,Aprovada
TXN-00024,Priscila Rocha Azevedo,645.987.210-36,priscila.azevedo@inbox.com,Saque,300.00,2024-01-26 12:10:00,Recusada
TXN-00025,Anderson Cardoso Barros,756.098.321-47,anderson.barros@mailbox.com,Depósito,4100.00,2024-01-27 13:50:00,Aprovada
TXN-00026,Juliana Freitas Silva,867.109.432-58,juliana.silva@email.com,Transferência,650.00,2024-01-28 08:45:00,Aprovada
TXN-00027,Marcos Monteiro Lopes,978.210.543-69,marcos.lopes@webmail.com,Pagamento,275.60,2024-01-29 09:30:00,Aprovada
TXN-00028,Renata Correia Fonseca,089.321.654-80,renata.fonseca@correio.com,Saque,520.00,2024-01-30 11:15:00,Pendente
TXN-00029,Carlos Azevedo Moreira,190.432.765-91,carlos.moreira@inbox.com,Depósito,9200.00,2024-01-31 14:00:00,Aprovada
TXN-00030,Beatriz Barros Cunha,201.543.876-92,beatriz.cunha@mailbox.com,Transferência,380.90,2024-02-01 15:30:00,Aprovada
"""

pdf = pd.read_csv(io.StringIO(csv_data))

print("Parsed CSV with pandas. Shape:", pdf.shape)
print(pdf.dtypes)

sdf = spark.createDataFrame(pdf)

BRONZE_TABLE = "bronze_transacoes_financeiras_155b0451"

sdf.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(BRONZE_TABLE)

row_count = spark.read.table(BRONZE_TABLE).count()
print(f"[BRONZE] Table '{BRONZE_TABLE}' written successfully. Row count: {row_count}")

from pyspark.sql import SparkSession
import pandas as pd
import io

spark = SparkSession.builder.getOrCreate()

csv_data = """order_id,nome_completo,email,cpf,produto,categoria,valor_total,status_pedido
ORD-10001,João Pereira Silva,joao.silva@fictmail.com,382.917.645-01,Smartphone Galaxy X10,Eletrônicos,1299.90,Entregue
ORD-10002,Mariana Costa Souza,mariana.costa@fictmail.com,741.258.963-02,Notebook Ultrafino Pro,Eletrônicos,3499.00,Entregue
ORD-10003,Carlos Eduardo Lima,carlos.lima@fictmail.com,159.357.486-03,Tênis Running Speed,Esportes,289.90,Em trânsito
ORD-10004,Ana Paula Ferreira,ana.ferreira@fictmail.com,246.813.579-04,Cafeteira Espresso Max,Eletrodomésticos,459.00,Entregue
ORD-10005,Roberto Alves Mendes,roberto.mendes@fictmail.com,963.852.741-05,Livro: Gestão Moderna,Livros,59.90,Entregue
ORD-10006,Fernanda Lima Torres,fernanda.torres@fictmail.com,357.159.268-06,Fone Bluetooth SoundX,Eletrônicos,199.90,Cancelado
ORD-10007,Diego Martins Rocha,diego.rocha@fictmail.com,852.963.174-07,Cadeira Gamer ProSeat,Móveis,899.00,Entregue
ORD-10008,Patrícia Nunes Barros,patricia.barros@fictmail.com,741.963.258-08,Mochila Executiva Top,Acessórios,149.90,Entregue
ORD-10009,Lucas Henrique Costa,lucas.costa@fictmail.com,369.258.147-09,Monitor LED 24 Polegadas,Eletrônicos,799.00,Em trânsito
ORD-10010,Juliana Ramos Figueiredo,juliana.figueiredo@fictmail.com,258.147.369-10,Livro: Python Avançado,Livros,89.90,Entregue
ORD-10011,Anderson Souza Pinto,anderson.pinto@fictmail.com,147.369.258-11,Camiseta Esportiva Dry,Vestuário,79.90,Entregue
ORD-10012,Camila Vieira Santos,camila.santos@fictmail.com,963.741.852-12,Panela Elétrica Smart,Eletrodomésticos,229.00,Devolvido
ORD-10013,Thiago Oliveira Melo,thiago.melo@fictmail.com,852.741.963-13,Teclado Mecânico RGB,Eletrônicos,349.90,Entregue
ORD-10014,Beatriz Carvalho Lopes,beatriz.lopes@fictmail.com,741.852.963-14,Perfume Floral Essence,Beleza,189.00,Entregue
ORD-10015,Rafael Cunha Azevedo,rafael.azevedo@fictmail.com,369.741.852-15,Suplemento Whey Protein,Saúde,159.90,Em trânsito
ORD-10016,Larissa Monteiro Reis,larissa.reis@fictmail.com,258.963.741-16,Smartwatch Fit Pro,Eletrônicos,599.00,Entregue
ORD-10017,Gustavo Freitas Moura,gustavo.moura@fictmail.com,147.852.963-17,Jogo de Panelas Inox,Eletrodomésticos,319.00,Entregue
ORD-10018,Aline Batista Correia,aline.correia@fictmail.com,963.258.741-18,Tênis Casual Urban,Esportes,199.90,Cancelado
ORD-10019,Felipe Rodrigues Borges,felipe.borges@fictmail.com,852.369.741-19,Mesa de Escritório Oak,Móveis,749.00,Em trânsito
ORD-10020,Vanessa Teixeira Almeida,vanessa.almeida@fictmail.com,741.258.963-20,Câmera DSLR PhotoPro,Eletrônicos,2899.00,Entregue
ORD-10021,Rodrigo Nascimento Dias,rodrigo.dias@fictmail.com,369.852.741-21,Creme Hidratante Skin,Beleza,49.90,Entregue
ORD-10022,Priscila Gomes Cardoso,priscila.cardoso@fictmail.com,258.741.963-22,Headset Gamer VoiceX,Eletrônicos,279.90,Entregue
ORD-10023,Marcelo Araújo Fonseca,marcelo.fonseca@fictmail.com,147.963.852-23,Bicicleta Speed Carbon,Esportes,3199.00,Em trânsito
ORD-10024,Tatiane Cruz Ribeiro,tatiane.ribeiro@fictmail.com,963.147.258-24,Livro: Finanças Pessoais,Livros,69.90,Entregue
ORD-10025,Eduardo Peixoto Barbosa,eduardo.barbosa@fictmail.com,852.258.147-25,Ar Condicionado 12000,Eletrodomésticos,1899.00,Em trânsito
ORD-10026,Simone Cavalcanti Mota,simone.mota@fictmail.com,741.147.369-26,Vestido Casual Chic,Vestuário,129.90,Entregue
ORD-10027,Renato Andrade Xavier,renato.xavier@fictmail.com,369.963.852-27,Roteador Wifi 6 Pro,Eletrônicos,349.00,Entregue
ORD-10028,Cristiane Leite Duarte,cristiane.duarte@fictmail.com,258.852.741-28,Kit Skincare Premium,Beleza,299.00,Devolvido
ORD-10029,Alexandre Campos Neto,alexandre.neto@fictmail.com,147.741.963-29,Sofá Retrátil Comfort,Móveis,2199.00,Entregue
ORD-10030,Natalia Pereira Faria,natalia.faria@fictmail.com,963.369.258-30,Whey Protein Isolado,Saúde,189.90,Em trânsito"""

pdf = pd.read_csv(io.StringIO(csv_data))

print(f"Parsed {len(pdf)} rows from CSV data.")
print(pdf.dtypes)

sdf = spark.createDataFrame(pdf)

sdf.write.format("delta").mode("overwrite").saveAsTable("bronze_ecommerce_orders_2eb476bf")

row_count = spark.read.table("bronze_ecommerce_orders_2eb476bf").count()
print(f"Bronze table 'bronze_ecommerce_orders_2eb476bf' written successfully. Row count: {row_count}")

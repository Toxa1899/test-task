from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import psycopg2
from psycopg2 import sql
from decouple import config
from loguru import logger
import uuid




class ElasticsearchAndUpdateSimilarSku:
    def __init__(self, dbname: str, user: str, password: str, host: str = 'localhost', port: int = 5432):
        self.connection = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        self.cursor = self.connection.cursor()
        self.es = Elasticsearch(hosts=["http://localhost:9200"])
        
    
    def search(self):
        self.cursor.execute("SELECT uuid, title, brand FROM public.sku;")
        products = self.cursor.fetchall()

        for product in products:
            uuid_id = product[0]
            title = product[1]
            brand = product[2]

            
            if not title or not brand:
                logger.info(f"Пропускаем продукт с UUID {uuid_id} из-за пустых значений: title='{title}', brand='{brand}'")
                continue  

            query = {
                "query": {
                    "bool": {
                        "must": [
                            {"match": {"title": title}},
                            {"match": {"brand": brand}}
                        ]
                    }
                }
            }

            # Поиск товаров
            try:
                response = self.es.search(index='products', body=query)
               
                similar_skus = [hit['_source']['uuid'] for hit in response['hits']['hits'][:5]]

                for s in similar_skus:
                    self.cursor.execute(
                        """
                        UPDATE public.sku 
                        SET similar_sku = 
                            CASE 
                                WHEN similar_sku IS NULL 
                                THEN ARRAY[%s]::uuid[] 
                                ELSE array_append(similar_sku, %s) 
                            END 
                        WHERE uuid = %s;
                        """,
                        (s, s, uuid_id)
                    )

            except Exception as e:
                logger.info(f"Ошибка при выполнении поиска для продукта с UUID {uuid_id}: {e}")

           
        self.connection.commit()
        self.cursor.close()
        self.connection.close()
        
    


# connection = psycopg2.connect(
#     dbname=config('DB_NAME'),
#     user=config('DB_USER'),
#     password=config('DB_PASSWORD'),
#     host='localhost',
#     port=5432
# )

# cursor = connection.cursor()


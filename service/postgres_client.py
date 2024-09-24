import psycopg2
from psycopg2 import sql
from typing import Any, Dict, List
from loguru import logger
from decouple import config

class PostgresClient:
    def __init__(self, dbname: str, user: str, password: str, host: str = 'localhost', port: int = 5432):
        """Инициализация клиента PostgreSQL."""
        self.connection = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        self.cursor = self.connection.cursor()

    def close(self):
        """Закрываем соединение с базой данных."""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

    def insert_product(self, offer_data: Dict[str, Any]) -> None:
        """Вставляем данные о товаре в таблицу sku."""
        insert_query = sql.SQL("""
            INSERT INTO public.sku (
                uuid, marketplace_id, product_id, title, description, brand, seller_id,
                seller_name, first_image_url, category_id, category_lvl_1, category_lvl_2,
                category_lvl_3, category_remaining, features, rating_count, rating_value,
                price_before_discounts, discount, price_after_discounts, bonuses, sales,
                currency, barcode, similar_sku
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s
            )
            
        """)

        params = (
            offer_data['uuid'],
            offer_data['marketplace_id'],
            offer_data['product_id'],
            offer_data['title'],
            offer_data['description'],
            offer_data['brand'],
            offer_data.get('seller_id', None),
            offer_data.get('seller_name', None),
            offer_data.get('first_image_url', None),
            offer_data.get('category_id', None),
            offer_data.get('category_lvl_1', None),
            offer_data.get('category_lvl_2', None),
            offer_data.get('category_lvl_3', None),
            offer_data.get('category_remaining', None),
            offer_data.get('features', None),
            offer_data.get('rating_count', 0),
            offer_data.get('rating_value', 0.0),
            offer_data.get('price_before_discounts', 0.0),
            offer_data.get('discount', 0.0),
            offer_data.get('price_after_discounts', 0.0),
            offer_data.get('bonuses', 0),
            offer_data.get('sales', 0),
            offer_data.get('currency', None),
            offer_data.get('barcode', None),
            offer_data.get('similar_sku', [])
        )

        try:
            self.cursor.execute(insert_query, params)
            self.connection.commit()
            logger.info('успешно сохраннено в бд')
        except Exception as e:
            print(f"Error executing query: {e}")
            self.connection.rollback()


    def fetch_all_skus(self) -> List[Dict]:
        """Читаем все поля из таблицы public.sku."""
        try:
            self.cursor.execute("SELECT * FROM public.sku;")
            rows = self.cursor.fetchall()
            columns = [desc[0] for desc in self.cursor.description]
            return [dict(zip(columns, row)) for row in rows]
        except Exception as e:
            print(f"Произошла ошибка: {e}")
            return []

    
    
    
# ps = PostgresClient(dbname=config('DB_NAME'), user=config('DB_USER'), password=config('DB_PASSWORD'))
# print(ps.fetch_all_skus())
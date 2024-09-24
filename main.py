from service.postgres_client import PostgresClient
from decouple import config
from loguru import logger
from service.elasticsearch_client import ElasticsearchAndUpdateSimilarSku
from service.xml_parser import MarketplaceXMLParser




if __name__ == '__main__':
    logger.info('Запуск парсинга')
    print(config('PATH'))
    mark = MarketplaceXMLParser(xml_file=config('XML_FILE'))
    pg_client = PostgresClient(dbname=config('DB_NAME'), user=config('DB_USER'), password=config('DB_PASSWORD'))
    count_pars = 0  
    for offer in mark.parse_offers():
        pg_client.insert_product(offer)
        count_pars+= 1
        logger.info(f'спаршено {count_pars}')
    sku_updater = ElasticsearchAndUpdateSimilarSku(dbname=config('DB_NAME'), user=config('DB_USER'), password=config('DB_PASSWORD'))    
    sku_updater.search()
    pg_client.close()
    logger.info('Закрываем соединение с бд')
    

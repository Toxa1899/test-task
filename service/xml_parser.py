import lxml.etree as ET
from typing import Iterator
import uuid
from typing import Dict
from loguru import logger
import json

logger.add("xml_service.log")


class MarketplaceXMLParser:
    def __init__(self, xml_file: str):
        self.xml_file = xml_file
        self.categories = {}
        self.parse_categories()


    def parse_categories(self) -> Dict[int, dict]:
        """ Парсинг категорий из xml """
        category = ET.iterparse(self.xml_file, events=('end',), tag='category')
        for _ , elem in category:
            category_id = int(elem.attrib['id'])
            parent_id = int(elem.attrib.get('parentId', 0))
            category_name = elem.text
            self.categories[category_id] = {
                'name' : category_name,
                'parent_id': parent_id,
            }   
            elem.clear()
        return self.categories

    def get_category_path(self, category_id: int):
        """ Находим путь категории и возвращаем в виде [списка] """
    
        path = []
        while category_id in self.categories:
            category = self.categories[category_id]
            path.append(category['name'])
            category_id = category['parent_id']
        path.reverse()
        return path
        


    def parse_offers(self) -> Iterator[dict]:
        """Итерирует по XML-файлу и возвращает товары как словари."""

        offer = ET.iterparse(self.xml_file, events=('end',), tag='offer')
        for _, elem in offer:    
            
            offer_data = {
                'uuid': str(uuid.uuid4()), 
                'marketplace_id': 1,  
                'product_id': int(elem.attrib['id']),
                'title': elem.findtext('name'),
                'description': elem.findtext('description'),
                'brand': elem.findtext('vendor'),
                'seller_id': int(elem.findtext('seller_id', '0')), 
                'seller_name': elem.findtext('seller_name'),
                'first_image_url': elem.findtext('picture'),
                'category_id': int(elem.findtext('categoryId', '0')),
               
                'rating_count': 0, 
                'rating_value': 0.0,  
                'price_before_discounts': float(elem.findtext('oldprice', '0')),
                'discount': self.calculate_discount(
                    elem.findtext('oldprice'), elem.findtext('price')),
                'price_after_discounts': float(elem.findtext('price', '0')),
                'bonuses': 0, 
                'sales': 0,  
                'inserted_at': None, 
                'updated_at': None,  
                'currency': elem.findtext('currencyId'),
                'barcode': int(elem.findtext('barcode', '0')),
                'similar_sku': [] 
            }
            
            # group_id
            param = elem.findall('param')
            features = {}
            for p in param:
                name = p.attrib.get('name', '')
                text = p.text or ''
                features[name] = text
            
            offer_data['features'] = json.dumps(features)
            category_path = self.get_category_path(offer_data['category_id'])
            offer_data['category_lvl_1'] = category_path[0] if len(category_path) > 0 else None
            offer_data['category_lvl_2'] = category_path[1] if len(category_path) > 1 else None
            offer_data['category_lvl_3'] = category_path[2] if len(category_path) > 2 else None
            offer_data['category_remaining'] = "/".join(category_path[3:]) if len(category_path) > 3 else None 
            
            yield offer_data
            
            elem.clear()
            
   

    def calculate_discount(self, old_price: int, new_price: int):
        # функция для расчета скидки (discount)
        if old_price and new_price:
            try:
                old_price = float(old_price)
                new_price = float(new_price)
                if old_price > new_price:
                    return round(((old_price - new_price) / old_price) * 100, 2)
            except ValueError:
                return 0
        return 0
    
    
    
    
  
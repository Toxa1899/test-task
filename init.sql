CREATE TABLE public.sku (
    uuid                   uuid,
    marketplace_id         integer,
    product_id             bigint,
    title                  text,
    description            text,
    brand                  text,
    seller_id              integer,
    seller_name            text,
    first_image_url        text,
    category_id            integer,
    category_lvl_1         text,
    category_lvl_2         text,
    category_lvl_3         text,
    category_remaining     text,
    features               json,
    rating_count           integer,
    rating_value           double precision,
    price_before_discounts real,
    discount               double precision,
    price_after_discounts  real,
    bonuses                integer,
    sales                  integer,
    inserted_at            timestamp default now(),
    updated_at             timestamp default now(),
    currency               text,
    barcode                bigint,
    similar_sku            uuid[]
);

COMMENT ON COLUMN public.sku.uuid IS 'id товара в нашей бд';
COMMENT ON COLUMN public.sku.marketplace_id IS 'id маркетплейса';
COMMENT ON COLUMN public.sku.product_id IS 'id товара в маркетплейсе';
COMMENT ON COLUMN public.sku.title IS 'название товара';
COMMENT ON COLUMN public.sku.description IS 'описание товара';
COMMENT ON COLUMN public.sku.category_lvl_1 IS 'Первая часть категории товара';
COMMENT ON COLUMN public.sku.category_lvl_2 IS 'Вторая часть категории товара';
COMMENT ON COLUMN public.sku.category_lvl_3 IS 'Третья часть категории товара';
COMMENT ON COLUMN public.sku.category_remaining IS 'Остаток категории товара';
COMMENT ON COLUMN public.sku.features IS 'Характеристики товара';
COMMENT ON COLUMN public.sku.rating_count IS 'Кол-во отзывов о товаре';
COMMENT ON COLUMN public.sku.rating_value IS 'Рейтинг товара (0-5)';
COMMENT ON COLUMN public.sku.barcode IS 'Штрихкод';

CREATE INDEX sku_brand_index
    ON public.sku (brand);

CREATE UNIQUE INDEX sku_marketplace_id_sku_id_uindex
    ON public.sku (marketplace_id, product_id);

CREATE UNIQUE INDEX sku_uuid_uindex
    ON public.sku (uuid);

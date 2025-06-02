-- Clean order items with price calculations
select
    order_id,
    order_item_id,
    product_id,
    seller_id,
    
    -- Price calculations
    price as item_price,
    freight_value,
    price + freight_value as total_item_value,
    
    -- Clean timestamps
    parse_datetime('%Y-%m-%d %H:%M:%S', shipping_limit_date) as shipping_limit_date

from {{ source('raw', 'order_items') }}

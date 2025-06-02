select
    order_id,
    customer_id,
    order_status,
    -- Clean up timestamp
    parse_datetime('%Y-%m-%d %H:%M:%S', order_purchase_timestamp) as order_purchase_timestamp,
    parse_datetime('%Y-%m-%d %H:%M:%S', order_approved_at) as order_approved_at,
    parse_datetime('%Y-%m-%d %H:%M:%S', order_delivered_carrier_date) as order_delivered_carrier_date,
    parse_datetime('%Y-%m-%d %H:%M:%S', order_delivered_customer_date) as order_delivered_customer_date,
    parse_datetime('%Y-%m-%d %H:%M:%S', order_estimated_delivery_date) as order_estimated_delivery_date,
    
    -- Clean order status
    upper(trim(order_status)) as order_status_clean

from {{ source('raw', 'orders') }}

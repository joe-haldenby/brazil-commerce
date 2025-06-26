-- Order item fact table for product analysis
select
    -- Identifiers
    oi.order_id,
    oi.order_item_id,
    oi.product_id,
    oi.seller_id,
    o.customer_id,
     c.customer_unique_id, 
    
    -- Order context
    o.order_purchase_timestamp,
    o.order_status,
    c.customer_state,  -- Fixed: get from customers table
    
    -- Product details
    p.product_category_name_pt,
    p.product_category_name_en,
    p.product_weight_g,
    p.product_volume_cm3,
    
    -- Financial metrics
    oi.item_price,
    oi.freight_value,
    oi.total_item_value,
    
    -- Delivery metrics
    oi.shipping_limit_date,
    o.order_delivered_customer_date,
    
    -- Review score for this order
    r.review_score

from {{ ref('stg_order_items') }} oi
left join {{ ref('stg_orders') }} o using(order_id)
left join {{ ref('stg_products') }} p using(product_id)
left join {{ ref('stg_customers') }} c using(customer_id)
left join {{ ref('stg_reviews') }} r using(order_id)

-- Complete order fact table with all business metrics
with order_items_agg as (
    select 
        order_id,
        sum(total_item_value) as total_order_value,
        sum(freight_value) as total_freight_value,
        count(*) as item_count
    from {{ ref('stg_order_items') }}
    group by order_id
),

primary_payments as (
    select 
        order_id,
        payment_type as primary_payment_type,
        sum(payment_value) as total_payment_value,
        max(payment_installments) as payment_installments
    from {{ ref('stg_payments') }}
    group by order_id, payment_type
    qualify row_number() over(partition by order_id order by sum(payment_value) desc) = 1
)

select
    -- Order identifiers
    o.order_id,
    o.customer_id,
    
    -- Order details
    o.order_status,
    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,
    
    -- Customer details  
    c.customer_city,
    c.customer_state,
    
    -- Financial metrics
    oi.total_order_value,
    oi.total_freight_value,
    oi.item_count,
    
    -- Payment details
    p.primary_payment_type,
    p.total_payment_value,
    p.payment_installments,
    
    -- Review metrics
    r.review_score,
    
    -- Calculated business metrics
    date_diff(o.order_delivered_customer_date, o.order_purchase_timestamp, day) as days_to_deliver,
    date_diff(o.order_delivered_customer_date, o.order_estimated_delivery_date, day) as delivery_vs_estimate

from {{ ref('stg_orders') }} o
left join {{ ref('stg_customers') }} c using(customer_id)
left join order_items_agg oi using(order_id)
left join primary_payments p using(order_id)
left join {{ ref('stg_reviews') }} r using(order_id)

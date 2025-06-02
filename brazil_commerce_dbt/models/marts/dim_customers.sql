-- Customer dimension with lifetime metrics and segmentation
with customer_order_metrics as (
    select 
        customer_id,
        count(*) as total_orders,
        sum(total_order_value) as lifetime_value,
        avg(total_order_value) as avg_order_value,
        min(order_purchase_timestamp) as first_order_date,
        max(order_purchase_timestamp) as last_order_date,
        avg(review_score) as avg_review_score
    from {{ ref('fct_orders') }}
    group by customer_id
),

customer_segments as (
    select 
        *,
        case 
            when total_orders >= 5 then 'Loyal'
            when total_orders >= 2 then 'Repeat'
            else 'One-time'
        end as customer_segment,
        
        case 
            when lifetime_value >= 500 then 'High Value'
            when lifetime_value >= 200 then 'Medium Value'
            else 'Low Value'
        end as value_segment
    from customer_order_metrics
)

select
    -- Customer identifiers
    c.customer_id,
    c.customer_unique_id,
    
    -- Geographic details
    c.customer_city,
    c.customer_state,
    c.customer_zip_code_prefix,
    
    -- Lifetime metrics
    m.total_orders,
    m.lifetime_value,
    m.avg_order_value,
    m.first_order_date,
    m.last_order_date,
    m.avg_review_score,
    
    -- Segmentation
    m.customer_segment,
    m.value_segment,
    
    -- Calculated metrics
    date_diff(current_date(), m.last_order_date, day) as days_since_last_order

from {{ ref('stg_customers') }} c
left join customer_segments m using(customer_id)

-- Clean products with category translations
select
    p.product_id,
    
    -- Product attributes
    p.product_category_name as product_category_name_pt,
    coalesce(t.product_category_name_english, p.product_category_name) as product_category_name_en,
    
    -- Physical dimensions
    p.product_name_lenght,
    p.product_description_lenght,
    p.product_photos_qty,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm,
    
    -- Calculate volume
    p.product_length_cm * p.product_height_cm * p.product_width_cm as product_volume_cm3

from {{ source('raw', 'products') }} p
left join {{ source('raw', 'category_translation') }} t
    on p.product_category_name = t.product_category_name

-- Clean review data
select
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    
    -- Clean timestamps
    parse_datetime('%Y-%m-%d %H:%M:%S', review_creation_date) as review_creation_date,
    parse_datetime('%Y-%m-%d %H:%M:%S', review_answer_timestamp) as review_answer_timestamp

from {{ source('raw', 'order_reviews') }}

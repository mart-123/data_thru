SELECT 
    t1.delivery_code, 
    t1.delivery_received, 
    t1.collection_ref, 
    t1.collection_sent, 
    t1.delivery_description,
    t1.source_file

FROM 
    {{ source('hesa', 'load_hesa_delivery_metadata') }} t1

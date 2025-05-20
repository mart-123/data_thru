SELECT 
    t1.delivery_code, 
    t1.delivery_received,
    t1.delivery_version,
    UPPER(t1.delivery_current) as delivery_current, -- no extract script to handle this
    t1.collection_ref, 
    t1.collection_sent, 
    t1.delivery_description,
    t1.source_file

FROM 
    {{ source('hesa', 'load_hesa_delivery_metadata') }} t1

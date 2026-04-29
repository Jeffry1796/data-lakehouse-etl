select
    1 as payment_method_id,
    'Credit Card' as payment_method_name

union all

select
    2 as payment_method_id,
    'Cash' as payment_method_name

union all

select
    3 as payment_method_id,
    'No Charge' as payment_method_name

union all

select
    4 as payment_method_id,
    'Dispute' as payment_method_name

union all

select
    5 as payment_method_id,
    'Unknown' as payment_method_name

union all

select
    6 as payment_method_id,
    'Voided Trip' as payment_method_name

union all

select
    7 as payment_method_id,
    'Others' as payment_method_name
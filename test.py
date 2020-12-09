import datetime

s = """ss_location_cache
sid

uc_orders
order_id

em_sent
sent_id

retailer_order
id
 
ss_auto_activation
sid
 
ss_comm
comm_id
 
ss_leads
lead_id

ss_retailers
retailer_id

uc_attribute_options
did

uc_attributes
aid

uc_coupons_orders
cud

uc_order_line_items
line_item_id

uc_order_products
order_product_id

uc_order_products_data
order_product_id

uc_order_returns
order_return_id

uc_payment_receipts
receipt_id

visitor_tests
id

Master_Order_ID_Table
order_id

"""

lines = s.split("\n")


i = 0

table = None
pk = None

for line in lines:

    line = line.strip()

    t = i % 3

    if t == 0:
        table = line
    elif t == 1:
        pk = line

        sql = f"""
        create or replace view datalake_agg.{table} as
        select o.* from datalake.{table} o
        join (
        select j.{pk}, max("_op_timestamp")as t from datalake.{table} as j
        group by j.{pk}
        ) m on m.{pk} = o.{pk} and m.t = o."_op_timestamp" and o."_op_type" != 'd'
        ;
        """

        print(sql)

    i += 1
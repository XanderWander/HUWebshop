from PostgreSQL.connect_db import connect_db
import tqdm

connection = connect_db(host='localhost', database='HUwebshop', user='postgres', password='Theredslime123')
cursor = connection.cursor()
prodid = 8532

def get_all_product_ids():
    cursor.execute(f""" SELECT DISTINCT product_id  FROM ordered_products""")
    prodid_list = cursor.fetchall()
    return prodid_list


def others_also_bought(prodid):

    cursor.execute(f""" SELECT op_2.product_id, COUNT(op_2.product_id) AS ct FROM ordered_products as op_1
                    JOIN sessions on sessions.id = op_1.session_id
                    JOIN ordered_products as op_2 on op_2.session_id = op_1.session_id
                    WHERE op_1.product_id = '{prodid[0]}' AND NOT op_2.product_id = '{prodid[0]}'
                    GROUP BY 1
                    ORDER BY ct DESC;""")
    reco = cursor.fetchall()

    return reco

def create_reco_others_bought_table():
    cursor.execute(f""" create table reco_others_bought(
    product_id varchar,
    reco_product_id varchar,
    ct int)
    """)
    connection.commit()

def get_all_others_bought():
    prodid_list = get_all_product_ids()

    for _id in tqdm.tqdm(prodid_list):
        reco_list = others_also_bought(_id)[0:8]
        for reco in reco_list:
            cursor.execute(f""" insert into reco_others_bought(product_id, reco_product_id, ct)
            values ('{_id[0]}', '{reco[0]}', {reco[1]})
                """)




get_all_others_bought()
connection.commit()
connection.exit()

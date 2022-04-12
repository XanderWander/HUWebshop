from postgresql import Postgres


def main():
    others_combined_query = "DROP TABLE IF EXISTS others_combined;" \
                            "CREATE TABLE others_combined AS (" \
                            "   SELECT pid2 AS pid, pid1 AS combined_id, count(pid1) AS ct FROM (" \
                            "       SELECT op2.product_id AS pid2, op1.product_id AS pid1 FROM ordered_products AS op1" \
                            "       JOIN sessions ON sessions.id = session_id" \
                            "       JOIN ordered_products AS op2 ON op2.session_id = sessions.id" \
                            "       WHERE NOT op2.product_id = op1.product_id" \
                            "   ) AS x" \
                            "   GROUP BY pid2, pid1" \
                            ");"

    pg = Postgres()
    pg.execute(others_combined_query)
    pg.close()


if __name__ == "__main__":
    main()

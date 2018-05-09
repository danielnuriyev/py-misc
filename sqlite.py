
import sqlite3
import sys

if __name__ == '__main__':

    products_path = sys.argv[1] # TODO check argv length, check if it has index 1
    sales_path = sys.argv[2]    # TODO check if argv has index 2

    conn = sqlite3.connect(':memory:')
    c = conn.cursor()

    c.execute('''CREATE TABLE products
                 (product text, category text)''')

    f = open(products_path)
    for line in f:
        parts = line.split('\t')
        if len(parts) < 2: continue # TODO: print out bad records
        #print(line)

        c.execute("INSERT INTO products VALUES (?,?)" , (parts[0].strip(), parts[1].strip()))

    f.close()

    c.execute('''CREATE TABLE sales
                     (product text, sale real)''')

    f = open(sales_path)
    for line in f:
        parts = line.split('\t')
        if len(parts) < 2: continue # TODO: print out bad records
        #print(line)

        c.execute("INSERT INTO sales VALUES (?,?)", (parts[0].strip(), parts[1].strip()))

    f.close()

    conn.commit()

    sql = """
        SELECT category FROM products p
        JOIN (SELECT * FROM sales ORDER BY sale LIMIT 5 ) s ON s.product = p.product
    """
    for row in c.execute(sql):
        print(row)

    sql = """
        SELECT s.product, s.sale FROM sales AS s
        JOIN products as p ON p.product = s.product AND p.category = 'Candy'
        ORDER BY s.sale DESC LIMIT 1
    """
    for row in c.execute(sql):
        print(row)

    conn.close()

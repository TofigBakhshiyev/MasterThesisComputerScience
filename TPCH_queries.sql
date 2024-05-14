-- Query 1.
SELECT l_returnflag,
      l_linestatus,
      SUM(l_quantity) AS sum_qty,
      SUM(l_extendedprice) AS
      sum_base_price,
      SUM(l_extendedprice * ( 1 - l_discount )) AS
      sum_disc_price,
      SUM(l_extendedprice * ( 1 - l_discount ) * ( 1 + l_tax )) AS sum_charge,
      Avg(l_quantity) AS avg_qty,
      Avg(l_extendedprice) AS avg_price,
      Avg(l_discount) AS avg_disc,
      Count(*) AS count_order
FROM   lineitem
WHERE  l_shipdate <= DATE '1998-12-01' - interval '[DELTA]' day (3)
GROUP  BY l_returnflag,
         l_linestatus
ORDER  BY l_returnflag,
         l_linestatus;

-- Query 2.
SELECT   s_acctbal,
        s_name,
        n_name,
        p_partkey,
        p_mfgr,
        s_address,
        s_phone,
        s_comment
FROM     part,
        supplier,
        partsupp,
        nation,
        region
WHERE    p_partkey = ps_partkey
AND      s_suppkey = ps_suppkey
AND      p_size = [SIZE]
AND      p_type LIKE '%[TYPE]'
AND      s_nationkey = n_nationkey
AND      n_regionkey = r_regionkey
AND      r_name = '[REGION]'
AND      ps_supplycost =
        (
           SELECT min(ps_supplycost)
           from   partsupp,
                  supplier,
                  nation,
                  region
           WHERE  p_partkey = ps_partkey
           AND    s_suppkey = ps_suppkey
           AND    s_nationkey = n_nationkey
           AND    n_regionkey = r_regionkey
           AND    r_name = '[REGION]' )
ORDER BY s_acctbal DESC,
        n_name,
        s_name,
        p_partkey;

-- Query 3.
SELECT l_orderkey,
       SUM(l_extendedprice * ( 1 - l_discount )) AS revenue,
       o_orderdate,
       o_shippriority
FROM   customer,
       orders,
       lineitem
WHERE  c_mktsegment = '[SEGMENT]'
       AND c_custkey = o_custkey
       AND l_orderkey = o_orderkey
       AND o_orderdate < DATE '[DATE]'
       AND l_shipdate > DATE '[DATE]'
GROUP  BY l_orderkey,
          o_orderdate,
          o_shippriority
ORDER  BY revenue DESC,
          o_orderdate; 

-- Query 4.
SELECT o_orderpriority,
       Count(*) AS order_count
FROM   orders
WHERE  o_orderdate >= DATE '[DATE]'
       AND o_orderdate < DATE '[DATE]' + interval '3' month
       AND EXISTS (SELECT *
                   FROM   lineitem
                   WHERE  l_orderkey = o_orderkey
                          AND l_commitdate < l_receiptdate)
GROUP  BY o_orderpriority
ORDER  BY o_orderpriority; 

-- Query 5.
SELECT n_name,
       SUM(l_extendedprice * ( 1 - l_discount )) AS revenue
FROM   customer,
       orders,
       lineitem,
       supplier,
       nation,
       region
WHERE  c_custkey = o_custkey
       AND l_orderkey = o_orderkey
       AND l_suppkey = s_suppkey
       AND c_nationkey = s_nationkey
       AND s_nationkey = n_nationkey
       AND n_regionkey = r_regionkey
       AND r_name = '[REGION]'
       AND o_orderdate >= DATE '[DATE]'
       AND o_orderdate < DATE '[DATE]' + interval '1' year
GROUP  BY n_name
ORDER  BY revenue DESC; 

-- Query 6.
SELECT Sum(l_extendedprice*l_discount) AS revenue
FROM   lineitem
WHERE  l_shipdate >= date '[DATE]'
AND    l_shipdate <  date '[DATE]'   + interval '1' year
AND    l_discount BETWEEN [DISCOUNT] - 0.01 AND    [DISCOUNT] + 0.01
AND    l_quantity < [QUANTITY];

-- Query 7.
SELECT supp_nation,
       cust_nation,
       l_year,
       SUM(volume) AS revenue
FROM   (SELECT n1.n_name                            AS supp_nation,
               n2.n_name                            AS cust_nation,
               Extract(year FROM l_shipdate)        AS l_year,
               l_extendedprice * ( 1 - l_discount ) AS volume
        FROM   supplier,
               lineitem,
               orders,
               customer,
               nation n1,
               nation n2
        WHERE  s_suppkey = l_suppkey
               AND o_orderkey = l_orderkey
               AND c_custkey = o_custkey
               AND s_nationkey = n1.n_nationkey
               AND c_nationkey = n2.n_nationkey
               AND ( ( n1.n_name = '[NATION1]'
                       AND n2.n_name = '[NATION2]' )
                      OR ( n1.n_name = '[NATION2]'
                           AND n2.n_name = '[NATION1]' ) )
               AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31')
       AS
       shipping
GROUP  BY supp_nation,
          cust_nation,
          l_year
ORDER  BY supp_nation,
          cust_nation,
          l_year; 

-- Query 8.
SELECT o_year,
       SUM(CASE
             WHEN nation = '[NATION]' THEN volume
             ELSE 0
           END) / SUM(volume) AS mkt_share
FROM   (SELECT Extract(year FROM o_orderdate)       AS o_year,
               l_extendedprice * ( 1 - l_discount ) AS volume,
               n2.n_name                            AS nation
        FROM   part,
               supplier,
               lineitem,
               orders,
               customer,
               nation n1,
               nation n2,
               region
        WHERE  p_partkey = l_partkey
               AND s_suppkey = l_suppkey
               AND l_orderkey = o_orderkey
               AND o_custkey = c_custkey
               AND c_nationkey = n1.n_nationkey
               AND n1.n_regionkey = r_regionkey
               AND r_name = '[REGION]'
               AND s_nationkey = n2.n_nationkey
               AND o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
               AND p_type = '[TYPE]') AS all_nations
GROUP  BY o_year
ORDER  BY o_year; 

-- Query 9.
SELECT nation,
       o_year,
       Sum(amount) AS sum_profit
FROM   (SELECT n_name
               AS
                      nation,
               Extract(year FROM o_orderdate)
               AS
                      o_year,
               l_extendedprice * ( 1 - l_discount ) - ps_supplycost * l_quantity
               AS
                      amount
        FROM   part,
               supplier,
               lineitem,
               partsupp,
               orders,
               nation
        WHERE  s_suppkey = l_suppkey
               AND ps_suppkey = l_suppkey
               AND ps_partkey = l_partkey
               AND p_partkey = l_partkey
               AND o_orderkey = l_orderkey
               AND s_nationkey = n_nationkey
               AND p_name LIKE '%[COLOR]%') AS profit
GROUP  BY nation,
          o_year
ORDER  BY nation,
          o_year DESC;

-- Query 10.
SELECT c_custkey,
       c_name,
       SUM(l_extendedprice * ( 1 - l_discount )) AS revenue,
       c_acctbal,
       n_name,
       c_address,
       c_phone,
       c_comment
FROM   customer,
       orders,
       lineitem,
       nation
WHERE  c_custkey = o_custkey
       AND l_orderkey = o_orderkey
       AND o_orderdate >= DATE '[DATE]'
       AND o_orderdate < DATE '[DATE]' + interval '3' month
       AND l_returnflag = 'R'
       AND c_nationkey = n_nationkey
GROUP  BY c_custkey,
          c_name,
          c_acctbal,
          c_phone,
          n_name,
          c_address,
          c_comment
ORDER  BY revenue DESC; 

-- Query 11.
SELECT ps_partkey,
      Sum(ps_supplycost * ps_availqty) AS value
FROM   partsupp,
      supplier,
      nation
WHERE  ps_suppkey = s_suppkey
      AND s_nationkey = n_nationkey
      AND n_name = '[NATION]'
GROUP  BY ps_partkey
HAVING
      Sum(ps_supplycost * ps_availqty) >
      (SELECT Sum(ps_supplycost * ps_availqty) * [fraction]
      FROM   partsupp,
             supplier,
             nation
      WHERE  ps_suppkey = s_suppkey
             AND s_nationkey = n_nationkey
             AND n_name = '[NATION]')
ORDER  BY value DESC;

-- Query 12.
SELECT l_shipmode,
       SUM(CASE
             WHEN o_orderpriority = '1-URGENT'
                   OR o_orderpriority = '2-HIGH' THEN 1
             ELSE 0
           END) AS high_line_count,
       SUM(CASE
             WHEN o_orderpriority <> '1-URGENT'
                  AND o_orderpriority <> '2-HIGH' THEN 1
             ELSE 0
           END) AS low_line_count
FROM   orders,
       lineitem
WHERE  o_orderkey = l_orderkey
       AND l_shipmode IN ( '[SHIPMODE1]', '[SHIPMODE2]' )
       AND l_commitdate < l_receiptdate
       AND l_shipdate < l_commitdate
       AND l_receiptdate >= DATE '[DATE]'
       AND l_receiptdate < DATE '[DATE]' + interval '1' year
GROUP  BY l_shipmode
ORDER  BY l_shipmode; 

-- Query 13.
SELECT c_count,
       Count(*) AS custdist
FROM   (SELECT c_custkey,
               Count(o_orderkey)
        FROM   customer
               LEFT OUTER JOIN orders
                            ON c_custkey = o_custkey
                               AND o_comment NOT LIKE '%[word1]%[word2]%'
        GROUP  BY c_custkey)AS c_orders (c_custkey, c_count)
GROUP  BY c_count
ORDER  BY custdist DESC,
          c_count DESC; 

-- Query 14.
SELECT 100.00 * SUM(CASE
                      WHEN p_type LIKE 'PROMO%' THEN l_extendedprice *
                                                     ( 1 - l_discount )
                      ELSE 0
                    END) / SUM(l_extendedprice * ( 1 - l_discount )) AS
       promo_revenue
FROM   lineitem,
       part
WHERE  l_partkey = p_partkey
       AND l_shipdate >= DATE '[DATE]'
       AND l_shipdate < DATE '[DATE]' + interval '1' month; 

-- Query 15.
CREATE VIEW revenue[STREAM_ID]
            (
                        supplier_no,
                        total_revenue
            )
            AS
SELECT   l_suppkey,
         sum(l_extendedprice * (1 - l_discount))
FROM     lineitem
WHERE    l_shipdate >= date '[DATE]'
AND      l_shipdate <  date '[DATE]' + interval '3' month
GROUP BY l_suppkey;SELECT   s_suppkey,
         s_name,
         s_address,
         s_phone,
         total_revenue
FROM     supplier,
         revenue[STREAM_ID]
WHERE    s_suppkey = supplier_no
AND      total_revenue =
         (
                SELECT Max(total_revenue)
                FROM   revenue[STREAM_ID] )
ORDER BY s_suppkey;DROP VIEW revenue[STREAM_ID];

-- Query 16.
SELECT p_brand,
       p_type,
       p_size,
       Count(DISTINCT ps_suppkey) AS supplier_cnt
FROM   partsupp,
       part
WHERE  p_partkey = ps_partkey
       AND p_brand <> '[BRAND]'
       AND p_type NOT LIKE '[TYPE]%'
       AND p_size IN ( [size1], [size2], [size3], [size4],
                       [size5], [size6], [size7], [size8] )
       AND ps_suppkey NOT IN (SELECT s_suppkey
                              FROM   supplier
                              WHERE  s_comment LIKE '%Customer%Complaints%')
GROUP  BY p_brand,
          p_type,
          p_size
ORDER  BY supplier_cnt DESC,
          p_brand,
          p_type,
          p_size; 

-- Query 17.
SELECT Sum(l_extendedprice) / 7.0 AS avg_yearly
FROM   lineitem,
       part
WHERE  p_partkey = l_partkey
       AND p_brand = '[BRAND]'
       AND p_container = '[CONTAINER]'
       AND l_quantity < (SELECT 0.2 * Avg(l_quantity)
                         FROM   lineitem
                         WHERE  l_partkey = p_partkey); 

-- Query 18.
SELECT c_name,
       c_custkey,
       o_orderkey,
       o_orderdate,
       o_totalprice,
       Sum(l_quantity)
FROM   customer,
       orders,
       lineitem
WHERE  o_orderkey IN (SELECT l_orderkey
                      FROM   lineitem
                      GROUP  BY l_orderkey
                      HAVING Sum(l_quantity) > [quantity])
       AND c_custkey = o_custkey
       AND o_orderkey = l_orderkey
GROUP  BY c_name,
          c_custkey,
          o_orderkey,
          o_orderdate,
          o_totalprice
ORDER  BY o_totalprice DESC,
          o_orderdate; 

-- Query 19.
SELECT Sum(l_extendedprice * (1 - l_discount) ) AS revenue
FROM   lineitem,
       part
WHERE  (
              p_partkey = l_partkey
       AND    p_brand = '[BRAND1]'
       AND    p_container IN ( 'sm case',
                              'sm box',
                              'sm pack',
                              'sm pkg')
       AND    l_quantity >= [QUANTITY1]
       AND    l_quantity <= [QUANTITY1] + 10
       AND    p_size BETWEEN 1 AND    5
       AND    l_shipmode IN ('air',
                             'air reg')
       AND    l_shipinstruct = 'deliver IN person' )
OR     (
              p_partkey = l_partkey
       AND    p_brand = '[BRAND2]'
       AND    p_container IN ('med bag',
                              'med box',
                              'med pkg',
                              'med pack')
       AND    l_quantity >= [QUANTITY2]
       AND    l_quantity <= [QUANTITY2] + 10
       AND    p_size BETWEEN 1 AND    10
       AND    l_shipmode IN ('air',
                             'air reg')
       AND    l_shipinstruct = 'deliver IN person' )
OR     (
              p_partkey = l_partkey
       AND    p_brand = '[BRAND3]'
       AND    p_container IN ( 'lg case',
                              'lg box',
                              'lg pack',
                              'lg pkg')
       AND    l_quantity >= [QUANTITY3]
       AND    l_quantity <= [QUANTITY3] + 10
       AND    p_size BETWEEN 1 AND    15
       AND    l_shipmode IN ('air',
                             'air reg')
       AND    l_shipinstruct = 'deliver IN person' );

-- Query 20.
SELECT   s_name,
         s_address
FROM     supplier,
         nation
WHERE    s_suppkey IN
         (
                SELECT ps_suppkey
                FROM   partsupp
                WHERE  ps_partkey IN
                       (
                              SELECT p_partkey
                              FROM   part
                              WHERE  p_name LIKE '[COLOR]%' )
                AND    ps_availqty >
                       (
                              SELECT 0.5 * Sum(l_quantity)
                              FROM   lineitem
                              WHERE  l_partkey = ps_partkey
                              AND    l_suppkey = ps_suppkey
                              AND    l_shipdate >= date('[DATE]') and l_shipdate < date('[DATE]') + interval '1' year ) )
AND      s_nationkey = n_nationkey
AND      n_name = '[NATION]'
ORDER BY s_name;

-- Query 21.
SELECT s_name,
       Count(*) AS numwait
FROM   supplier,
       lineitem l1,
       orders,
       nation
WHERE  s_suppkey = l1.l_suppkey
       AND o_orderkey = l1.l_orderkey
       AND o_orderstatus = 'F'
       AND l1.l_receiptdate > l1.l_commitdate
       AND EXISTS (SELECT *
                   FROM   lineitem l2
                   WHERE  l2.l_orderkey = l1.l_orderkey
                          AND l2.l_suppkey <> l1.l_suppkey)
       AND NOT EXISTS (SELECT *
                       FROM   lineitem l3
                       WHERE  l3.l_orderkey = l1.l_orderkey
                              AND l3.l_suppkey <> l1.l_suppkey
                              AND l3.l_receiptdate > l3.l_commitdate)
       AND s_nationkey = n_nationkey
       AND n_name = '[NATION]'
GROUP  BY s_name
ORDER  BY numwait DESC,
          s_name; 

-- Query 22.
SELECT cntrycode,
       Count(*)       AS numcust,
       Sum(c_acctbal) AS totacctbal
FROM   (
              SELECT substring(c_phone from 1 FOR 2) AS cntrycode,
                     c_acctbal
              FROM   customer
              WHERE  substring(c_phone FROM 1 FOR 2) IN ('[I1]', '[I2]','[I3]','[I4]','[I5]','[I6]','[I7]') and c_acctbal > ( select avg(c_acctbal) from customer where c_acctbal > 0.00 and substring (c_phone from 1 for 2) in ('[I1]','[I2]','[I3]','[I4]','[I5]','[I6]','[I7]') ) and not exists ( select * from orders where o_custkey = c_custkey ) ) as custsale group by cntrycode order by cntrycode;


/*
Copyright 2019 Materialize, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#include <mz-config.h>
#include "materialized.h"

const mz::Config& mz::defaultConfig() {
    static Config singleton {
        .expectedSources = {
            "mysql_tpcch_customer",
            "mysql_tpcch_history",
            "mysql_tpcch_district",
            "mysql_tpcch_neworder",
            "mysql_tpcch_order",
            "mysql_tpcch_orderline",
            "mysql_tpcch_warehouse",
            "mysql_tpcch_item",
            "mysql_tpcch_stock",
            "mysql_tpcch_nation",
            "mysql_tpcch_region",
            "mysql_tpcch_supplier"
        },
        .viewPattern = "mysql.tpcch.%",
        .materializedUrl = "postgresql://materialized:6875/?sslmode=disable",
        .kafkaUrl = "kafka://kafka:9092",
        .schemaRegistryUrl = "http://schema-registry:8081",
        .hQueries = {}
    };
    return singleton;
}

const std::unordered_map<std::string, mz::ViewDefinition> &mz::allHQueries() {
    static std::unordered_map<std::string, ViewDefinition> singleton {
            {"q01",
             {"SELECT\n"
                    "    ol_number,\n"
                    "    sum(ol_quantity) AS sum_qty,\n"
                    "    sum(ol_amount) AS sum_amount,\n"
                    "    avg(ol_quantity) AS avg_qty,\n"
                    "    avg(ol_amount) AS avg_amount,\n"
                    "    count(*) AS count_order\n"
                    "FROM mysql_tpcch_orderline\n"
                    "WHERE ol_delivery_d > TIMESTAMP '2007-01-02 00:00:00.000000'\n"
                    "GROUP BY ol_number\n"
                    "ORDER BY ol_number\n",

                    "ol_number", std::nullopt}
            },
            {"q02",
                    {"SELECT su_suppkey, su_name, n_name, i_id, i_name, su_address, su_phone, su_comment\n"
                    "FROM\n"
                    "    mysql_tpcch_item, mysql_tpcch_supplier, mysql_tpcch_stock, mysql_tpcch_nation, mysql_tpcch_region,\n"
                    "    (\n"
                    "        SELECT\n"
                    "            s_i_id AS m_i_id,\n"
                    "            min(s_quantity) AS m_s_quantity\n"
                    "        FROM mysql_tpcch_stock, mysql_tpcch_supplier, mysql_tpcch_nation, mysql_tpcch_region\n"
                    "        WHERE s_su_suppkey = su_suppkey\n"
                    "        AND su_nationkey = n_nationkey\n"
                    "        AND n_regionkey = r_regionkey\n"
                    "        AND r_name like 'EUROP%'\n"
                    "        GROUP BY s_i_id\n"
                    "    ) m\n"
                    "WHERE i_id = s_i_id\n"
                    "AND s_su_suppkey = su_suppkey\n"
                    "AND su_nationkey = n_nationkey\n"
                    "AND n_regionkey = r_regionkey\n"
                    "AND i_data like '%b'\n"
                    "AND r_name like 'EUROP%'\n"
                    "AND i_id = m_i_id\n"
                    "AND s_quantity = m_s_quantity\n"
                    "ORDER BY n_name, su_name, i_id\n",

                    "n_name, su_name, i_id", std::nullopt}
            },
            {"q03",
                    {"SELECT ol_o_id, ol_w_id, ol_d_id, sum(ol_amount) AS revenue, o_entry_d\n"
                    "FROM mysql_tpcch_customer, mysql_tpcch_neworder, mysql_tpcch_order, mysql_tpcch_orderline\n"
                    "WHERE c_state LIKE 'A%'\n"
                    "AND c_id = o_c_id\n"
                    "AND c_w_id = o_w_id\n"
                    "AND c_d_id = o_d_id\n"
                    "AND no_w_id = o_w_id\n"
                    "AND no_d_id = o_d_id\n"
                    "AND no_o_id = o_id\n"
                    "AND ol_w_id = o_w_id\n"
                    "AND ol_d_id = o_d_id\n"
                    "AND ol_o_id = o_id\n"
                    "AND o_entry_d > TIMESTAMP '2007-01-02 00:00:00.000000'\n"
                    "GROUP BY ol_o_id, ol_w_id, ol_d_id, o_entry_d\n"
                    "ORDER BY revenue DESC, o_entry_d\n",

                    "revenue DESC, o_entry_d", std::nullopt}
            },
            {"q04",
                    {"SELECT o_ol_cnt, count(*) AS order_count\n"
                    "FROM mysql_tpcch_order\n"
                    "WHERE o_entry_d >= TIMESTAMP '2007-01-02 00:00:00.000000'\n"
                    "AND o_entry_d < TIMESTAMP '2020-01-02 00:00:00.000000'\n"
                    "AND EXISTS (\n"
                    "    SELECT *\n"
                    "    FROM mysql_tpcch_orderline\n"
                    "    WHERE o_id = ol_o_id\n"
                    "    AND o_w_id = ol_w_id\n"
                    "    AND o_d_id = ol_d_id\n"
                    "    AND ol_delivery_d >= o_entry_d\n"
                    ")\n"
                    "GROUP BY o_ol_cnt\n"
                    "ORDER BY ol_o_cnt\n",

                    "o_ol_cnt", std::nullopt}
            },
            {"q05",
                    {"SELECT\n"
                    "    n_name,\n"
                    "    sum(ol_amount) AS revenue\n"
                    "FROM mysql_tpcch_customer, mysql_tpcch_order, mysql_tpcch_orderline, mysql_tpcch_stock, mysql_tpcch_supplier, mysql_tpcch_nation, mysql_tpcch_region\n"
                    "WHERE c_id = o_c_id\n"
                    "AND c_w_id = o_w_id\n"
                    "AND c_d_id = o_d_id\n"
                    "AND ol_o_id = o_id\n"
                    "AND ol_w_id = o_w_id\n"
                    "AND ol_d_id=o_d_id\n"
                    "AND ol_w_id = s_w_id\n"
                    "AND ol_i_id = s_i_id\n"
                    "AND s_su_suppkey = su_suppkey\n"
                    "AND c_n_nationkey = su_nationkey\n"
                    "AND su_nationkey = n_nationkey\n"
                    "AND n_regionkey = r_regionkey\n"
                    "AND r_name = 'EUROPE'\n"
                    "AND o_entry_d >= TIMESTAMP '2007-01-02 00:00:00.000000'\n"
                    "GROUP BY n_name\n"
                    "ORDER BY revenue DESC\n",

                    "revenue DESC", std::nullopt}
            },
            {"q06",
                    {"SELECT sum(ol_amount) AS revenue\n"
                    "FROM mysql_tpcch_orderline\n"
                    "WHERE ol_delivery_d >= TIMESTAMP '1999-01-01 00:00:00.000000'\n"
                    "AND ol_delivery_d < TIMESTAMP '2020-01-01 00:00:00.000000'\n"
                    "AND ol_quantity BETWEEN 1 AND 100000\n",

                    std::nullopt, std::nullopt}
            },
            {"q07",
                    {"SELECT\n"
                    "    su_nationkey AS supp_nation,\n"
                    "    substr(c_state,1,1) AS cust_nation,\n"
                    "    extract(year FROM o_entry_d) AS l_year,\n"
                    "    sum(ol_amount) AS revenue\n"
                    "FROM mysql_tpcch_supplier, mysql_tpcch_stock, mysql_tpcch_orderline, mysql_tpcch_order, mysql_tpcch_customer, mysql_tpcch_nation n1, mysql_tpcch_nation n2\n"
                    "WHERE ol_supply_w_id = s_w_id\n"
                    "AND ol_i_id = s_i_id\n"
                    "AND s_su_suppkey = su_suppkey\n"
                    "AND ol_w_id = o_w_id\n"
                    "AND ol_d_id = o_d_id\n"
                    "AND ol_o_id = o_id\n"
                    "AND c_id = o_c_id\n"
                    "AND c_w_id = o_w_id\n"
                    "AND c_d_id = o_d_id\n"
                    "AND su_nationkey = n1.n_nationkey\n"
                    "AND c_n_nationkey = n2.n_nationkey\n"
                    "AND (\n"
                    "    (n1.n_name = 'GERMANY' AND n2.n_name = 'CAMBODIA')\n"
                    "    OR\n"
                    "    (n1.n_name = 'CAMBODIA' AND n2.n_name = 'GERMANY')\n"
                    ")\n"
                    "AND ol_delivery_d BETWEEN TIMESTAMP '2007-01-02 00:00:00.000000' AND TIMESTAMP '2020-01-02 00:00:00.000000'\n"
                    "GROUP BY su_nationkey, substr(c_state, 1, 1), extract(year FROM o_entry_d)\n"
                    "ORDER BY su_nationkey, cust_nation, l_year\n",

                    // su_nationkey was aliased to supp_nation in view definition
                    "supp_nation, cust_nation, l_year", std::nullopt}
            },
            {"q08",
                    {"SELECT\n"
                    "    extract(year FROM o_entry_d) AS l_year,\n"
                    "    sum(CASE WHEN n2.n_name = 'GERMANY' THEN ol_amount ELSE 0 END) / sum(ol_amount) AS mkt_share\n"
                    "FROM mysql_tpcch_item, mysql_tpcch_supplier, mysql_tpcch_stock, mysql_tpcch_orderline, mysql_tpcch_order, mysql_tpcch_customer, mysql_tpcch_nation n1, mysql_tpcch_nation n2, mysql_tpcch_region\n"
                    "WHERE i_id = s_i_id\n"
                    "AND ol_i_id = s_i_id\n"
                    "AND ol_supply_w_id = s_w_id\n"
                    "AND s_su_suppkey = su_suppkey\n"
                    "AND ol_w_id = o_w_id\n"
                    "AND ol_d_id = o_d_id\n"
                    "AND ol_o_id = o_id\n"
                    "AND c_id = o_c_id\n"
                    "AND c_w_id = o_w_id\n"
                    "AND c_d_id = o_d_id\n"
                    "AND n1.n_nationkey = c_n_nationkey\n"
                    "AND n1.n_regionkey = r_regionkey\n"
                    "AND ol_i_id < 1000\n"
                    "AND r_name = 'EUROPE'\n"
                    "AND su_nationkey = n2.n_nationkey\n"
                    "AND o_entry_d BETWEEN TIMESTAMP '2007-01-02 00:00:00.000000' AND TIMESTAMP '2020-01-02 00:00:00.000000'\n"
                    "AND i_data like '%b'\n"
                    "AND i_id = ol_i_id\n"
                    "GROUP BY extract(year FROM o_entry_d)\n"
                    "ORDER BY l_year\n",

                    "l_year", std::nullopt}
            },
            {"q09",
                    {"SELECT\n"
                    "    n_name, extract(year FROM o_entry_d) AS l_year,\n"
                    "    sum(ol_amount) AS sum_profit\n"
                    "FROM mysql_tpcch_item, mysql_tpcch_stock, mysql_tpcch_supplier, mysql_tpcch_orderline, mysql_tpcch_order, mysql_tpcch_nation\n"
                    "WHERE ol_i_id = s_i_id\n"
                    "AND ol_supply_w_id = s_w_id\n"
                    "AND s_su_suppkey = su_suppkey\n"
                    "AND ol_w_id = o_w_id\n"
                    "AND ol_d_id = o_d_id\n"
                    "AND ol_o_id = o_id\n"
                    "AND ol_i_id = i_id\n"
                    "AND su_nationkey = n_nationkey\n"
                    "AND i_data like '%BB'\n"
                    "GROUP BY n_name, extract(year FROM o_entry_d)\n"
                    "ORDER BY n_name, l_year DESC\n",

                    "n_name, l_year DESC", std::nullopt}
            },
            {"q10",
                    {"SELECT\n"
                    "    c_id, c_last, sum(ol_amount) AS revenue, c_city, c_phone, n_name\n"
                    "FROM mysql_tpcch_customer, mysql_tpcch_order, mysql_tpcch_orderline, mysql_tpcch_nation\n"
                    "WHERE c_id = o_c_id\n"
                    "AND c_w_id = o_w_id\n"
                    "AND c_d_id = o_d_id\n"
                    "AND ol_w_id = o_w_id\n"
                    "AND ol_d_id = o_d_id\n"
                    "AND ol_o_id = o_id\n"
                    "AND o_entry_d >= TIMESTAMP '2007-01-02 00:00:00.000000'\n"
                    "AND o_entry_d <= ol_delivery_d\n"
                    "AND n_nationkey = c_n_nationkey\n"
                    "GROUP BY c_id, c_last, c_city, c_phone, n_name\n"
                    "ORDER BY revenue DESC\n",

                    "revenue DESC", std::nullopt}
            },
            {"q11",
                    {"SELECT s_i_id, sum(s_order_cnt) AS ordercount\n"
                    "FROM mysql_tpcch_stock, mysql_tpcch_supplier, mysql_tpcch_nation\n"
                    "WHERE s_su_suppkey = su_suppkey\n"
                    "AND su_nationkey = n_nationkey\n"
                    "AND n_name = 'GERMANY'\n"
                    "GROUP BY s_i_id\n"
                    "HAVING sum(s_order_cnt) > (\n"
                    "    SELECT sum(s_order_cnt) * 0.005\n"
                    "    FROM mysql_tpcch_stock, mysql_tpcch_supplier, mysql_tpcch_nation\n"
                    "    WHERE s_su_suppkey = su_suppkey\n"
                    "    AND su_nationkey = n_nationkey\n"
                    "    AND n_name = 'GERMANY'\n"
                    ")\n"
                    "ORDER BY ordercount DESC\n",

                    "ordercount DESC", std::nullopt}
            },
            {"q12",
                    {"SELECT\n"
                    "    o_ol_cnt,\n"
                    "    sum(CASE WHEN o_carrier_id = 1 OR o_carrier_id = 2 THEN 1 ELSE 0 END) AS high_line_count,\n"
                    "    sum(CASE WHEN o_carrier_id <> 1 AND o_carrier_id <> 2 THEN 1 ELSE 0 END) AS low_line_count\n"
                    "FROM\n"
                    "    mysql_tpcch_order, mysql_tpcch_orderline\n"
                    "WHERE ol_w_id = o_w_id\n"
                    "AND ol_d_id = o_d_id\n"
                    "AND ol_o_id = o_id\n"
                    "AND o_entry_d <= ol_delivery_d\n"
                    "AND ol_delivery_d < TIMESTAMP '2020-01-01 00:00:00.000000'\n"
                    "GROUP BY o_ol_cnt\n"
                    "ORDER BY o_ol_cnt\n",

                    "o_ol_cnt", std::nullopt}
            },
            {"q13",
                    {"SELECT\n"
                    "    c_count, count(*) AS custdist\n"
                    "FROM (\n"
                    "    SELECT c_id, count(o_id) as c_count\n"
                    "    FROM mysql_tpcch_customer\n"
                    "    LEFT OUTER JOIN mysql_tpcch_order ON (\n"
                    "        c_w_id = o_w_id AND c_d_id = o_d_id AND c_id = o_c_id AND o_carrier_id > 8\n"
                    "    )\n"
                    "    GROUP BY c_id\n"
                    ") AS c_orders\n"
                    "GROUP BY c_count\n"
                    "ORDER BY custdist DESC, c_count DESC\n",

                    "custdist desc, c_count desc", std::nullopt}
            },
            {"q14",
                    {"SELECT\n"
                    "    100.00 * sum(CASE WHEN i_data LIKE 'PR%' THEN ol_amount ELSE 0 END) / (1 + sum(ol_amount)) AS promo_revenue\n"
                    "FROM mysql_tpcch_orderline, mysql_tpcch_item\n"
                    "WHERE ol_i_id = i_id\n"
                    "AND ol_delivery_d >= TIMESTAMP '2007-01-02 00:00:00.000000'\n"
                    "AND ol_delivery_d < TIMESTAMP '2020-01-02 00:00:00.000000'\n",

                    std::nullopt, std::nullopt}
            },
            {"q15",
                    {"SELECT su_suppkey, su_name, su_address, su_phone, total_revenue\n"
                    "FROM\n"
                    "    mysql_tpcch_supplier,\n"
                    "    (\n"
                    "        SELECT\n"
                    "            s_su_suppkey AS supplier_no,\n"
                    "            sum(ol_amount) AS total_revenue\n"
                    "        FROM mysql_tpcch_orderline, mysql_tpcch_stock\n"
                    "        WHERE ol_i_id = s_i_id\n"
                    "        AND ol_supply_w_id = s_w_id\n"
                    "        AND ol_delivery_d >= TIMESTAMP '2007-01-02 00:00:00.000000'\n"
                    "        GROUP BY s_su_suppkey\n"
                    "    ) AS revenue\n"
                    "WHERE su_suppkey = supplier_no\n"
                    "AND total_revenue = (\n"
                    "    SELECT max(total_revenue)\n"
                    "    FROM (\n"
                    "        SELECT\n"
                    "            s_su_suppkey AS supplier_no,\n"
                    "            sum(ol_amount) AS total_revenue\n"
                    "            FROM mysql_tpcch_orderline, mysql_tpcch_stock\n"
                    "        WHERE ol_i_id = s_i_id\n"
                    "        AND ol_supply_w_id = s_w_id\n"
                    "        AND ol_delivery_d >= TIMESTAMP '2007-01-02 00:00:00.000000'\n"
                    "        GROUP BY s_su_suppkey\n"
                    "    ) AS revenue\n"
                    ")\n"
                    "ORDER BY su_suppkey\n",

                    "su_suppkey", std::nullopt}
            },
            {"q16",
                    {"SELECT\n"
                    "    i_name,\n"
                    "    substr(i_data, 1, 3) AS brand,\n"
                    "    i_price,\n"
                    "    count(DISTINCT s_su_suppkey) AS supplier_cnt\n"
                    "FROM mysql_tpcch_stock, mysql_tpcch_item\n"
                    "WHERE i_id = s_i_id\n"
                    "AND i_data NOT LIKE 'zz%'\n"
                    "AND (\n"
                    "    s_su_suppkey NOT IN (SELECT su_suppkey FROM mysql_tpcch_supplier WHERE su_comment like '%bad%')\n"
                    ")\n"
                    "GROUP BY i_name, substr(i_data, 1, 3), i_price\n"
                    "ORDER BY supplier_cnt DESC\n",

                    "supplier_cnt DESC", std::nullopt}
            },
            {"q17",
                    {"SELECT\n"
                    "    sum(ol_amount) / 2.0 AS avg_yearly\n"
                    "FROM\n"
                    "    mysql_tpcch_orderline,\n"
                    "    (\n"
                    "        SELECT i_id, avg(ol_quantity) AS a\n"
                    "        FROM mysql_tpcch_item, mysql_tpcch_orderline\n"
                    "        WHERE i_data LIKE '%b' AND ol_i_id = i_id\n"
                    "        GROUP BY i_id\n"
                    "    ) t\n"
                    "WHERE ol_i_id = t.i_id\n"
                    "AND ol_quantity < t.a\n",

                    std::nullopt, std::nullopt}
            },
            {"q18",
                    {"SELECT c_last, c_id, o_id, o_entry_d, o_ol_cnt, sum(ol_amount)\n"
                    "FROM mysql_tpcch_customer, mysql_tpcch_order, mysql_tpcch_orderline\n"
                    "WHERE c_id = o_c_id\n"
                    "AND c_w_id = o_w_id\n"
                    "AND c_d_id = o_d_id\n"
                    "AND ol_w_id = o_w_id\n"
                    "AND ol_d_id = o_d_id\n"
                    "AND ol_o_id = o_id\n"
                    "GROUP BY o_id, o_w_id, o_d_id, c_id, c_last, o_entry_d, o_ol_cnt\n"
                    "HAVING sum(ol_amount) > 200\n"
                    "ORDER BY sum(ol_amount)\n",

                    "sum(ol_amount)", std::nullopt}
            },
            {"q19",
                    {"SELECT sum(ol_amount) AS revenue\n"
                    "FROM mysql_tpcch_orderline, mysql_tpcch_item\n"
                    "WHERE (\n"
                    "    ol_i_id = i_id\n"
                    "    AND i_data LIKE '%a'\n"
                    "    AND ol_quantity >= 1\n"
                    "    AND ol_quantity <= 10\n"
                    "    AND i_price BETWEEN 1 AND 400000\n"
                    "    AND ol_w_id in (1, 2, 3)\n"
                    ") OR (\n"
                    "    ol_i_id = i_id\n"
                    "    AND i_data LIKE '%b'\n"
                    "    AND ol_quantity >= 1\n"
                    "    AND ol_quantity <= 10\n"
                    "    AND i_price BETWEEN 1 AND 400000\n"
                    "    AND ol_w_id IN (1, 2, 4)\n"
                    ") OR (\n"
                    "    ol_i_id = i_id\n"
                    "    AND i_data LIKE '%c'\n"
                    "    AND ol_quantity >= 1\n"
                    "    AND ol_quantity <= 10\n"
                    "    AND i_price BETWEEN 1 AND 400000\n"
                    "    AND ol_w_id in (1, 5, 3)\n"
                    ")\n",

                    std::nullopt, std::nullopt}
            },
            {"q20",
                    {"SELECT su_name, su_address\n"
                    "FROM mysql_tpcch_supplier, mysql_tpcch_nation\n"
                    "WHERE su_suppkey IN (\n"
                    "    SELECT mod(s_i_id * s_w_id, 10000)\n"
                    "    FROM mysql_tpcch_stock, mysql_tpcch_orderline\n"
                    "    WHERE s_i_id IN (SELECT i_id FROM mysql_tpcch_item WHERE i_data LIKE 'co%')\n"
                    "    AND ol_i_id = s_i_id\n"
                    "    AND ol_delivery_d > TIMESTAMP '2010-05-23 12:00:00'\n"
                    "    GROUP BY s_i_id, s_w_id, s_quantity\n"
                    "    HAVING 2 * s_quantity > sum(ol_quantity)\n"
                    ")\n"
                    "AND su_nationkey = n_nationkey\n"
                    "AND n_name = 'GERMANY'\n"
                    "ORDER BY su_name\n",

                    "su_name", std::nullopt}
            },
            {"q21",
                    {"SELECT\n"
                    "    su_name, count(*) as numwait\n"
                    "FROM\n"
                    "    mysql_tpcch_supplier, mysql_tpcch_orderline l1, mysql_tpcch_order, mysql_tpcch_stock, mysql_tpcch_nation\n"
                    "WHERE ol_o_id = o_id\n"
                    "AND ol_w_id = o_w_id\n"
                    "AND ol_d_id = o_d_id\n"
                    "AND ol_w_id = s_w_id\n"
                    "AND ol_i_id = s_i_id\n"
                    "AND s_su_suppkey = su_suppkey\n"
                    "AND l1.ol_delivery_d > o_entry_d\n"
                    "AND NOT EXISTS (\n"
                    "    SELECT *\n"
                    "    FROM mysql_tpcch_orderline l2\n"
                    "    WHERE l2.ol_o_id = l1.ol_o_id\n"
                    "    AND l2.ol_w_id = l1.ol_w_id\n"
                    "    AND l2.ol_d_id = l1.ol_d_id\n"
                    "    AND l2.ol_delivery_d > l1.ol_delivery_d\n"
                    ")\n"
                    "AND su_nationkey = n_nationkey\n"
                    "AND n_name = 'GERMANY'\n"
                    "GROUP BY su_name\n"
                    "ORDER BY numwait DESC, su_name",

                    "numwait DESC, su_name", std::nullopt}
            },
            {"q22",
                    {"SELECT\n"
                    "    substr(c_state, 1, 1) AS country,\n"
                    "    count(*) AS numcust,\n"
                    "    sum(c_balance) AS totacctbal\n"
                    "FROM mysql_tpcch_customer\n"
                    "WHERE substr(c_phone, 1, 1) IN ('1', '2', '3', '4', '5', '6', '7')\n"
                    "AND c_balance > (\n"
                    "    SELECT avg(c_balance)\n"
                    "    FROM mysql_tpcch_customer\n"
                    "    WHERE c_balance > 0.00\n"
                    "    AND substr(c_phone, 1, 1) IN ('1', '2', '3', '4', '5', '6', '7')\n"
                    ")\n"
                    "AND NOT EXISTS (\n"
                    "    SELECT *\n"
                    "    FROM mysql_tpcch_order\n"
                    "    WHERE o_c_id = c_id AND o_w_id = c_w_id AND o_d_id = c_d_id\n"
                    ")\n"
                    "GROUP BY substr(c_state, 1, 1)\n"
                    "ORDER BY substr(c_state, 1, 1)\n",

                    "substr(c_state, 1, 1)", std::nullopt}
            }
    };
    return singleton;
}

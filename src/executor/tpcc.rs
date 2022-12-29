use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::io::Write;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use chrono::Utc;
use itertools::Itertools;
use postgres::Client;
use tabled::{
    object::{Rows, Object, Columns},
    Alignment,
    ModifyObject,
    Style,
    Table,
    Tabled
};
use tokio_postgres::{Client as AsyncClient};
use rand::{distributions::Alphanumeric, Rng, seq::SliceRandom};
use rust_decimal::prelude::*;

use super::benchmark::{
    AddIndexes,
    AddPrimaryKeys,
    AddForeignKeys,
    Counter,
    BenchmarkDDL,
    BenchmarkTransaction,
    InitializeSchema,
    LoadData,
    PreLoadData,
    PrintResultsSummary,
    ReadWrite,
};

// TPC-C like benchmark
pub struct TPCC {
    pub name: String,
    pub description: String,
    pub scalefactor: u32,
    pub start_id: u32,
    pub end_id: u32,
    // Vector of the read and write transactions that will be executed for this benchmark
    pub transactions_rw: Vec<BenchmarkTransaction>,
    // Tables DDLs
    pub table_ddls: Vec<BenchmarkDDL>,
    // Primary keys DDLs
    pub pkey_ddls: Vec<BenchmarkDDL>,
    // Foreign keys DDLs
    pub fkey_ddls: Vec<BenchmarkDDL>,
    // Additional index DDLs
    pub index_ddls: Vec<BenchmarkDDL>,
}

#[derive(Tabled)]
pub struct TPCCTransactionSummary {
    description: String,
    n_commits: u64,
    n_errors: u64,
    error_rate: f64,
    avg_response_time_ms: f64,
    tpm_c: u32,
}

impl TPCCTransactionSummary {
    pub fn new(description: String, n_commits: u64, n_errors: u64, error_rate: f64, avg_response_time_ms: f64, tpm_c: u32) -> TPCCTransactionSummary {
        TPCCTransactionSummary {
            description: description,
            n_commits: n_commits,
            n_errors: n_errors,
            error_rate: error_rate,
            avg_response_time_ms: avg_response_time_ms,
            tpm_c: tpm_c,
        }
    }
}

#[derive(Debug)]
pub struct TPCCError(String);

impl fmt::Display for TPCCError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Error for TPCCError {}

// TPC-C-like implementation
impl TPCC {
    pub fn new(scalefactor: u32, start_id: u32, end_id: u32) -> TPCC {
        TPCC {
            name: "TPC-C-like benchmark".to_string(),
            description: "TPC-C-like benchmark implementation.".to_string(),
            scalefactor: scalefactor,
            start_id: start_id,
            end_id: end_id,
            transactions_rw: Vec::from(
                [
                    BenchmarkTransaction {
                        id: 1,
                        weight: 4,
                        description: "The Delivery transaction".to_string(),
                    },
                    BenchmarkTransaction {
                        id: 2,
                        weight: 45,
                        description: "The New-Order transaction".to_string(),
                    },
                    BenchmarkTransaction {
                        id: 3,
                        weight: 43,
                        description: "The Payment transaction".to_string(),
                    },
                    BenchmarkTransaction {
                        id: 4,
                        weight: 4,
                        description: "The Order-Status transaction".to_string(),
                    },
                    BenchmarkTransaction {
                        id: 5,
                        weight: 4,
                        description: "The Stock-Level transaction".to_string(),
                    },
                ]
            ),
            table_ddls: Vec::from(
                [
                    BenchmarkDDL {
                        sql: "DROP TABLE IF EXISTS warehouse CASCADE".to_string(),
                    },
                    BenchmarkDDL {
                        sql: r"
                            CREATE TABLE warehouse (
                                w_id INTEGER,
                                w_name VARCHAR(10),
                                w_street_1 VARCHAR(20),
                                w_street_2 VARCHAR(20),
                                w_city VARCHAR(20),
                                w_state CHAR(2),
                                w_zip CHAR(9),
                                w_tax REAL,
                                w_ytd NUMERIC(24, 12)
                            );".to_string(),
                    },
                    BenchmarkDDL {
                        sql: "DROP TABLE IF EXISTS district CASCADE".to_string(),
                    },
                    BenchmarkDDL {
                        sql: r"
                            CREATE TABLE district (
                                d_id INTEGER,
                                d_w_id INTEGER,
                                d_name VARCHAR(10),
                                d_street_1 VARCHAR(20),
                                d_street_2 VARCHAR(20),
                                d_city VARCHAR(20),
                                d_state CHAR(2),
                                d_zip CHAR(9),
                                d_tax REAL,
                                d_ytd NUMERIC(24, 12),
                                d_next_o_id INTEGER
                            );".to_string(),
                    },
                    BenchmarkDDL {
                        sql: "DROP TABLE IF EXISTS customer CASCADE".to_string(),
                    },
                    BenchmarkDDL {
                        sql: r"
                            CREATE TABLE customer (
                                c_id INTEGER,
                                c_d_id INTEGER,
                                c_w_id INTEGER,
                                c_first VARCHAR(16),
                                c_middle CHAR(2),
                                c_last VARCHAR(16),
                                c_street_1 VARCHAR(20),
                                c_street_2 VARCHAR(20),
                                c_city VARCHAR(20),
                                c_state CHAR(2),
                                c_zip CHAR(9),
                                c_phone CHAR(16),
                                c_since TIMESTAMP,
                                c_credit CHAR(2),
                                c_credit_lim NUMERIC(24, 12),
                                c_discount REAL,
                                c_balance NUMERIC(24, 12),
                                c_ytd_payment NUMERIC(24, 12),
                                c_payment_cnt REAL,
                                c_delivery_cnt REAL,
                                c_data VARCHAR(500)
                            );".to_string(),
                    },
                    BenchmarkDDL {
                        sql: "DROP TABLE IF EXISTS history CASCADE".to_string(),
                    },
                    BenchmarkDDL {
                        sql: r"
                            CREATE TABLE history (
                                h_c_id INTEGER,
                                h_c_d_id INTEGER,
                                h_c_w_id INTEGER,
                                h_d_id INTEGER,
                                h_w_id INTEGER,
                                h_date TIMESTAMP,
                                h_amount REAL,
                                h_data VARCHAR(24)
                            );".to_string(),
                    },
                    BenchmarkDDL {
                        sql: "DROP TABLE IF EXISTS new_order CASCADE".to_string(),
                    },
                    BenchmarkDDL {
                        sql: r"
                            CREATE TABLE new_order (
                                no_o_id INTEGER,
                                no_d_id INTEGER,
                                no_w_id INTEGER
                            );".to_string(),
                    },
                    BenchmarkDDL {
                        sql: "DROP TABLE IF EXISTS orders CASCADE".to_string(),
                    },
                    BenchmarkDDL {
                        sql: r"
                            CREATE TABLE orders (
                                o_id INTEGER,
                                o_d_id INTEGER,
                                o_w_id INTEGER,
                                o_c_id INTEGER,
                                o_entry_d TIMESTAMP,
                                o_carrier_id INTEGER,
                                o_ol_cnt INTEGER,
                                o_all_local INTEGER
                            );".to_string(),
                    },
                    BenchmarkDDL {
                        sql: "DROP TABLE IF EXISTS order_line CASCADE".to_string(),
                    },
                    BenchmarkDDL {
                        sql: r"
                            CREATE TABLE order_line (
                                ol_o_id INTEGER,
                                ol_d_id INTEGER,
                                ol_w_id INTEGER,
                                ol_number INTEGER,
                                ol_i_id INTEGER,
                                ol_supply_w_id INTEGER,
                                ol_delivery_d TIMESTAMP,
                                ol_quantity INTEGER,
                                ol_amount REAL,
                                ol_dist_info VARCHAR(24)
                            );".to_string(),
                    },
                    BenchmarkDDL {
                        sql: "DROP TABLE IF EXISTS item CASCADE".to_string(),
                    },
                    BenchmarkDDL {
                        sql: r"
                            CREATE TABLE item (
                                i_id INTEGER,
                                i_im_id INTEGER,
                                i_name VARCHAR(24),
                                i_price REAL,
                                i_data VARCHAR(50)
                            );".to_string(),
                    },
                    BenchmarkDDL {
                        sql: "DROP TABLE IF EXISTS stock CASCADE".to_string(),
                    },
                    BenchmarkDDL {
                        sql: r"
                            CREATE TABLE stock (
                                s_i_id INTEGER,
                                s_w_id INTEGER,
                                s_quantity INTEGER,
                                s_dist_01 VARCHAR(24),
                                s_dist_02 VARCHAR(24),
                                s_dist_03 VARCHAR(24),
                                s_dist_04 VARCHAR(24),
                                s_dist_05 VARCHAR(24),
                                s_dist_06 VARCHAR(24),
                                s_dist_07 VARCHAR(24),
                                s_dist_08 VARCHAR(24),
                                s_dist_09 VARCHAR(24),
                                s_dist_10 VARCHAR(24),
                                s_ytd NUMERIC(16, 8),
                                s_order_cnt REAL,
                                s_remote_cnt REAL,
                                s_data VARCHAR(50)
                            );".to_string(),
                    },
                ]
            ),
            pkey_ddls: Vec::from(
                [
                    BenchmarkDDL {
                        sql: "ALTER TABLE warehouse ADD PRIMARY KEY (w_id)".to_string(),
                    },
                    BenchmarkDDL {
                        sql: "ALTER TABLE district ADD PRIMARY KEY (d_w_id, d_id)".to_string(),
                    },
                    BenchmarkDDL {
                        sql: "ALTER TABLE customer ADD PRIMARY KEY (c_w_id, c_d_id, c_id)".to_string(),
                    },
                    BenchmarkDDL {
                        sql: "ALTER TABLE new_order ADD PRIMARY KEY (no_w_id, no_d_id, no_o_id)".to_string(),
                    },
                    BenchmarkDDL {
                        sql: "ALTER TABLE orders ADD PRIMARY KEY (o_w_id, o_d_id, o_id)".to_string(),
                    },
                    BenchmarkDDL {
                        sql: "ALTER TABLE order_line ADD PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number)".to_string(),
                    },
                    BenchmarkDDL {
                        sql: "ALTER TABLE stock ADD PRIMARY KEY (s_w_id, s_i_id)".to_string(),
                    },
                    BenchmarkDDL {
                        sql: "ALTER TABLE item ADD PRIMARY KEY (i_id)".to_string(),
                    },
                ]
            ),
            fkey_ddls: Vec::from(
                [
                    BenchmarkDDL {
                        sql: r"
                            ALTER TABLE district
                            ADD CONSTRAINT fk_district_warehouse
                            FOREIGN KEY (d_w_id)
                            REFERENCES warehouse (w_id)
                            ".to_string(),
                    },
                    BenchmarkDDL {
                        sql: r"
                            ALTER TABLE customer
                            ADD CONSTRAINT fk_customer_district
                            FOREIGN KEY (c_w_id, c_d_id)
                            REFERENCES district (d_w_id, d_id)
                            ".to_string(),
                    },
                    BenchmarkDDL {
                        sql: r"
                            ALTER TABLE history
                            ADD CONSTRAINT fk_history_customer
                            FOREIGN KEY (h_c_w_id, h_c_d_id, h_c_id)
                            REFERENCES customer (c_w_id, c_d_id, c_id)
                            ".to_string(),
                    },
                    BenchmarkDDL {
                        sql: r"
                            ALTER TABLE history
                            ADD CONSTRAINT fk_history_district
                            FOREIGN KEY (h_w_id, h_d_id)
                            REFERENCES district (d_w_id, d_id)
                            ".to_string(),
                    },
                    BenchmarkDDL {
                        sql: r"
                            ALTER TABLE new_order
                            ADD CONSTRAINT fk_new_order_orders
                            FOREIGN KEY (no_w_id, no_d_id, no_o_id)
                            REFERENCES orders (o_w_id, o_d_id, o_id)
                            ".to_string(),
                    },
                    BenchmarkDDL {
                        sql: r"
                            ALTER TABLE orders
                            ADD CONSTRAINT fk_orders_customer
                            FOREIGN KEY (o_w_id, o_d_id, o_c_id)
                            REFERENCES customer (c_w_id, c_d_id, c_id)
                            ".to_string(),
                    },
                    BenchmarkDDL {
                        sql: r"
                            ALTER TABLE order_line
                            ADD CONSTRAINT fk_order_line_orders
                            FOREIGN KEY (ol_w_id, ol_d_id, ol_o_id)
                            REFERENCES orders (o_w_id, o_d_id, o_id)
                            ".to_string(),
                    },
                    BenchmarkDDL {
                        sql: r"
                            ALTER TABLE order_line
                            ADD CONSTRAINT fk_order_line_stock
                            FOREIGN KEY (ol_supply_w_id, ol_i_id)
                            REFERENCES stock (s_w_id, s_i_id)
                            ".to_string(),
                    },
                    BenchmarkDDL {
                        sql: r"
                            ALTER TABLE stock
                            ADD CONSTRAINT fk_stock_warehouse
                            FOREIGN KEY (s_w_id)
                            REFERENCES warehouse (w_id)
                            ".to_string(),
                    },
                    BenchmarkDDL {
                        sql: r"
                            ALTER TABLE stock
                            ADD CONSTRAINT fk_stock_item
                            FOREIGN KEY (s_i_id)
                            REFERENCES item (i_id)
                            ".to_string(),
                    },
                ]
            ),
            index_ddls: Vec::from(
                [
                     BenchmarkDDL {
                        sql: "CREATE INDEX i_customer_c_last ON customer (c_last)".to_string(),
                    },
                ]
            )
        }
    }

    // Returns the transaction description for a given id
    pub fn get_transaction_description(&self, id: u16) -> String {
        for t in &self.transactions_rw {
            if t.id == id {
                return t.description.clone();
            }
        }

        "Description not found".to_string()
    }

    // The Delivery business transaction
    pub async fn delivery(client: &mut AsyncClient, warehouse_id :i32, _start_id :u32, _end_id :u32) -> Result<u128, Box<dyn std::error::Error>> {
        let start = Instant::now();

        let carrier_id :i32 = rand::thread_rng()
            .gen_range(1..=10);

        let transaction = client.transaction().await?;

        for district_id in 1..=10 {
            let rows = transaction.query(r"
                SELECT no_o_id
                FROM new_order
                WHERE
                    no_w_id = $1
                    AND no_d_id = $2
                ORDER BY no_o_id ASC
                LIMIT 1
                ", &[&warehouse_id, &district_id]).await?;
            if rows.len() > 0 {
                let order_id: i32 = rows[0].get("no_o_id");

                transaction.query(r"
                    DELETE FROM new_order
                    WHERE
                        no_o_id = $1
                        AND no_w_id = $2
                        AND no_d_id = $3
                ", &[&order_id, &warehouse_id, &district_id]).await?;

                let row_orders = transaction.query(r"
                    UPDATE orders
                    SET
                        o_carrier_id = $1
                    WHERE
                        o_id = $2
                        AND o_w_id = $3
                        AND o_d_id = $4
                    RETURNING o_c_id
                ", &[&carrier_id, &order_id, &warehouse_id, &district_id]).await?;

                if row_orders.len() == 0 {
                    transaction.rollback().await?;
                    return Err(Box::new(TPCCError("Delivery transaction rollbacked. Order not found.".into())));
                }

                let customer_id: i32 = row_orders[0].get("o_c_id");

                transaction.query(r"
                    UPDATE order_line
                    SET
                        ol_delivery_d = current_timestamp
                    WHERE
                        ol_o_id = $1
                        AND ol_w_id = $2
                        AND ol_d_id = $3
                ", &[&order_id, &warehouse_id, &district_id]).await?;

                let row_amount = transaction.query(r"
                    SELECT SUM(ol_amount * ol_quantity) AS total_ol_amount
                    FROM order_line
                    WHERE
                        ol_o_id = $1
                        AND ol_w_id = $2
                        AND ol_d_id = $3
                ", &[&order_id, &warehouse_id, &district_id]).await?;

                if row_amount.len() == 0 {
                    transaction.rollback().await?;
                    return Err(Box::new(TPCCError("Delivery transaction rollbacked. Order-line items not found.".into())));
                }

                let total_ol_amount :f64 = row_amount[0].get("total_ol_amount");
                let total_ol_amount_dec :Decimal = Decimal::from_f64(total_ol_amount).unwrap();

                transaction.query(r"
                    UPDATE customer
                    SET
                        c_delivery_cnt = c_delivery_cnt + 1,
                        c_balance = c_balance + $1
                    WHERE
                        c_id = $2
                        AND c_w_id = $3
                        AND c_d_id = $4;
                ", &[&total_ol_amount_dec, &customer_id, &warehouse_id, &district_id]).await?;
            }
        }
        transaction.commit().await?;

        Ok(start.elapsed().as_micros())
    }

    // The New-Order business transaction
    pub async fn new_order(client: &mut AsyncClient, warehouse_id :i32, start_id :u32, end_id :u32) -> Result<u128, Box<dyn std::error::Error>> {
        let district_id :i32 = rand::thread_rng()
            .gen_range(1..=10);
        let customer_id :i32 = rand::thread_rng()
            .gen_range(1..=3000);

        // Number of order_line entries
        let ol_cnt :i32 = rand::thread_rng()
            .gen_range(5..=15);
        let mut ol_all_local :i32 = 1;

        // Build order_lines
        let mut order_line_data = Vec::new();
        let mut item_ids = Vec::new();

        let mut ol_i_id :i32;

        // Generate 1% of rollback by setting up an invalid item id
        let mut rbk = rand::thread_rng()
            .gen_range(1..=100);

        for ol_number in 1..=ol_cnt {
            let mut ol_supply_w_id :i32 = warehouse_id;
            let ol_quantity :i32 = rand::thread_rng()
                .gen_range(1..=10);

            // Generate ol_i_id / item id
            loop {
                ol_i_id = rand::thread_rng()
                    .gen_range(1..=100_000);
                if !item_ids.contains(&ol_i_id) {
                    item_ids.push(ol_i_id.clone());
                    break;
                }
            }
            if rbk == 1 {
                ol_i_id = 999_999;
                rbk = 0;
            }

            // If we have more than one warehouse, then ol_supply_w_id can be different from
            // warehouse_id
            if (end_id - start_id) > 0 {
                let x :u8 = rand::thread_rng()
                    .gen_range(1..=100);
                if x == 1 {
                    ol_all_local = 0;
                    // Pickup random warehouse id different from warehouse_id
                    while ol_supply_w_id == warehouse_id {
                        ol_supply_w_id = rand::thread_rng()
                            .gen_range(start_id as i32..=end_id as i32);
                    }
                }
            }
            order_line_data.push((ol_number, ol_supply_w_id, ol_quantity, ol_i_id));
        }

        // Starting database transaction
        let start = Instant::now();
        let transaction = client.transaction().await?;

        transaction.query(r"
            SELECT w_tax FROM warehouse WHERE w_id = $1
        ", &[&warehouse_id]).await?;

        let row_district = transaction.query(r"
             UPDATE district
             SET d_next_o_id = d_next_o_id + 1
             WHERE
                d_w_id = $1
                AND d_id = $2
            RETURNING d_tax, d_next_o_id AS o_id
        ", &[&warehouse_id, &district_id]).await?;

        if row_district.len() == 0 {
            transaction.rollback().await?;
            return Err(Box::new(TPCCError("New-order transaction rollbacked. District not found.".into())));
        }

        let mut o_id :i32 = row_district[0].get("o_id");
        o_id -= 1;

        transaction.query(r"
            SELECT c_discount, c_last, c_credit
            FROM customer
            WHERE
                c_w_id = $1
                AND c_d_id = $2
                AND c_id = $3
        ", &[&warehouse_id, &district_id, &customer_id]).await?;

        // Inserting one new row into orders and new_order
        transaction.query(r"
            INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local)
            VALUES ($1, $2, $3, $4, NOW(), $5, $6)
        ", &[&o_id, &district_id, &warehouse_id, &customer_id, &ol_cnt, &ol_all_local]).await?;

        transaction.query(r"
            INSERT INTO new_order (no_o_id, no_d_id, no_w_id)
            VALUES ($1, $2, $3)
        ", &[&o_id, &district_id, &warehouse_id]).await?;

        let stock_query = format!("SELECT s_quantity, s_dist_{:0>2} AS s_dist, s_data FROM stock WHERE s_i_id = $1 AND s_w_id = $2", district_id);

        for (ol_number, ol_supply_w_id, ol_quantity, ol_i_id) in order_line_data {
            let row_item = transaction.query(r"
                SELECT i_price, i_name, i_data FROM item WHERE i_id = $1
            ", &[&ol_i_id]).await?;

            if row_item.len() == 0 {
                // Item not found then we must rollback the transaction
                transaction.rollback().await?;
                return Err(Box::new(TPCCError("New-order transaction rollbacked. Item not found.".into())));
            }

            let i_price :f32 = row_item[0].get("i_price");
            let ol_amount :f32 = i_price * ol_quantity as f32;

            // Execute stock query
            let row_stock = transaction.query(&stock_query, &[&ol_i_id, &ol_supply_w_id]).await?;
            let mut s_quantity :i32 = row_stock[0].get("s_quantity");
            let s_dist :String = row_stock[0].get("s_dist");

            // Update stock
            if (s_quantity - ol_quantity) > 10 {
                s_quantity = s_quantity - ol_quantity;
            }
            else {
                s_quantity = s_quantity - ol_quantity + 91;
            }
            let mut s_remote_cnt_inc :f32 = 0.0;
            if ol_supply_w_id != warehouse_id {
                s_remote_cnt_inc = 1.0;
            }
            let ol_quantity_dec = Decimal::from_i32(ol_quantity).unwrap();
            transaction.query(r"
                UPDATE stock SET
                    s_quantity = $3,
                    s_ytd = s_ytd + $4,
                    s_order_cnt = s_order_cnt + 1,
                    s_remote_cnt = s_remote_cnt + $5
                WHERE
                    s_i_id = $1
                    AND s_w_id = $2
            ", &[&ol_i_id, &ol_supply_w_id, &s_quantity, &ol_quantity_dec, &s_remote_cnt_inc]).await?;

            // Insert into order_line
            transaction.query(r"
                INSERT INTO order_line (
                    ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity,
                    ol_amount, ol_dist_info
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9
                )
            ", &[&o_id, &district_id, &warehouse_id, &ol_number, &ol_i_id, &ol_supply_w_id,
                 &ol_quantity, &ol_amount, &s_dist]).await?;
        }

        transaction.commit().await?;

        Ok(start.elapsed().as_micros())
    }

    // The Payment business transaction
    pub async fn payment(client: &mut AsyncClient, warehouse_id :i32, start_id :u32, end_id :u32) -> Result<u128, Box<dyn std::error::Error>> {
        let x :u8 = rand::thread_rng()
            .gen_range(1..=100);
        let y :u8 = rand::thread_rng()
            .gen_range(1..=100);

        let district_id :i32 = rand::thread_rng()
            .gen_range(1..=10);

        let c_d_id :i32;
        let mut c_w_id :i32;

        if x <= 85 {
            c_d_id = district_id;
            c_w_id = warehouse_id;
        }
        else {
            c_d_id = rand::thread_rng()
                .gen_range(1..=10);
             if (end_id - start_id) > 0 {
                // Pickup random warehouse id different from warehouse_id
                loop {
                    c_w_id = rand::thread_rng()
                        .gen_range(start_id as i32..=end_id as i32);
                    if c_w_id != warehouse_id {
                        break;
                    }
                }
            }
            else {
                c_w_id = warehouse_id;
            }
        }
        let mut c_last: String = "".to_string();
        let mut c_id :i32 = rand::thread_rng()
            .gen_range(1..=3_000);

        if y <= 60 {
            let t :u32 = rand::thread_rng()
                .gen_range(1..=1_000);
            c_last = TPCC::gen_last(t);
        }
        let h_amount :f32 = rand::thread_rng()
            .gen_range(1.00..=5_000.00);
        let h_amount_dec :Decimal = Decimal::from_f32(h_amount).unwrap();


        let start = Instant::now();
        let transaction = client.transaction().await?;

        let row_warehouse = transaction.query(r"
            UPDATE warehouse
            SET w_ytd = w_ytd + $1
            WHERE w_id = $2
            RETURNING w_name, w_street_1, w_street_2, w_city, w_state, w_zip
        ", &[&h_amount_dec, &warehouse_id]).await?;

        if row_warehouse.len() == 0 {
            transaction.rollback().await?;
            return Err(Box::new(TPCCError("Payment transaction rollbacked. Warehouse not found.".into())));
        }

        let w_name: String = row_warehouse[0].get("w_name");

        let row_district = transaction.query(r"
            UPDATE district
            SET d_ytd = d_ytd + $1
            WHERE
                d_w_id = $2
                AND d_id = $3
            RETURNING d_name, d_street_1, d_street_2, d_city, d_state, d_zip
        ", &[&h_amount_dec, &warehouse_id, &district_id]).await?;

        if row_district.len() == 0 {
            transaction.rollback().await?;
            return Err(Box::new(TPCCError("Payment transaction rollbacked. District not found.".into())));
        }

        let d_name: String = row_district[0].get("d_name");

        if y <= 60 {
            let row_c_id = transaction.query(r"
                SELECT c_id FROM customer
                WHERE c_w_id = $1 AND c_d_id = $2 AND c_last = $3
                ORDER BY c_first ASC
            ", &[&c_w_id, &c_d_id, &c_last]).await?;

            if row_c_id.len() == 0 {
                transaction.rollback().await?;
                return Err(Box::new(TPCCError("Payment transaction rollbacked. Customer not found (c_last).".into())));
            }

            let n = row_c_id.len();
            c_id = row_c_id[n / 2].get("c_id");
        }

        let row_customer = transaction.query(r"
            SELECT
                c_id, c_first, c_middle, c_street_1, c_street_2,
                c_city, c_state, c_zip, c_phone, c_since,
                c_credit, c_credit_lim, c_discount, c_balance
            FROM customer
            WHERE
                c_w_id = $1
                AND c_d_id = $2
                AND c_id = $3
        ", &[&c_w_id, &c_d_id, &c_id]).await?;

        if row_customer.len() == 0 {
            transaction.rollback().await?;
            return Err(Box::new(TPCCError("Payment transaction rollbacked. Customer not found.".into())));
        }

        let c_credit: String = row_customer[0].get("c_credit");

        if c_credit == "BC".to_string() {
            let pre_c_data = format!("{} {} {} {} {} {}", c_id, c_d_id, c_w_id, district_id, warehouse_id, h_amount_dec);
            transaction.query(r"
                UPDATE customer
                SET
                    c_balance = c_balance - $1,
                    c_ytd_payment = c_ytd_payment + 1,
                    c_data = substring($5||' '||c_data, 1, 500)
                WHERE
                    c_id = $2 AND c_d_id = $3 AND c_w_id = $4
            ", &[&h_amount_dec, &c_id, &c_d_id, &c_w_id, &pre_c_data]).await?;
        }
        else {
            transaction.query(r"
                UPDATE customer
                SET
                    c_balance = c_balance - $1,
                    c_ytd_payment = c_ytd_payment + 1
                WHERE
                    c_id = $2 AND c_d_id = $3 AND c_w_id = $4
            ", &[&h_amount_dec, &c_id, &c_d_id, &c_w_id]).await?;
        }
        transaction.query(r"
            INSERT INTO history
                (h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data)
            VALUES
                ($1, $2, $3, $4, $5, NOW(), $6, substring($7||'    '||$8, 1, 24))
        ", &[&c_id, &c_d_id, &c_w_id, &district_id, &warehouse_id, &h_amount, &w_name, &d_name]).await?;

        transaction.commit().await?;

        Ok(start.elapsed().as_micros())
    }

    // The Order-Status business transaction
    pub async fn order_status(client: &mut AsyncClient, warehouse_id :i32, _start_id :u32, _end_id :u32) -> Result<u128, Box<dyn std::error::Error>> {
        let y :u8 = rand::thread_rng()
            .gen_range(1..=100);

        let district_id :i32 = rand::thread_rng()
            .gen_range(1..=10);

        let mut c_last: String = "".to_string();
        let mut c_id :i32 = rand::thread_rng()
            .gen_range(1..=3_000);

        if y <= 60 {
            let t :u32 = rand::thread_rng()
                .gen_range(1..=999);
            c_last = TPCC::gen_last(t);
        }

        let start = Instant::now();
        let transaction = client.transaction().await?;

        if y <= 60 {
            let row_c_id = transaction.query(r"
                SELECT c_id FROM customer
                WHERE c_w_id = $1 AND c_d_id = $2 AND c_last = $3
                ORDER BY c_first ASC
            ", &[&warehouse_id, &district_id, &c_last]).await?;

            if row_c_id.len() == 0 {
                transaction.rollback().await?;
                return Err(Box::new(TPCCError("Order-Status transaction rollbacked. Customer not found (c_last).".into())));
            }

            let n = row_c_id.len();
            c_id = row_c_id[n / 2].get("c_id");
        }

        transaction.query(r"
            SELECT
                c_balance, c_first, c_middle, c_last
            FROM customer
            WHERE
                c_w_id = $1
                AND c_d_id = $2
                AND c_id = $3
        ", &[&warehouse_id, &district_id, &c_id]).await?;

        let row_order = transaction.query(r"
            SELECT
                o_id, o_w_id, o_d_id, o_entry_d, o_carrier_id
            FROM orders
            WHERE
                o_w_id = $1
                AND o_d_id = $2
                AND o_c_id = $3
            ORDER BY o_entry_d DESC LIMIT 1
        ", &[&warehouse_id, &district_id, &c_id]).await?;

        if row_order.len() == 0 {
            transaction.rollback().await?;
            return Err(Box::new(TPCCError("Order-Status transaction rollbacked. Order not found.".into())));
        }

        let o_id :i32 = row_order[0].get("o_id");

        transaction.query(r"
            SELECT
                ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d
            FROM order_line
            WHERE
                ol_w_id = $1
                AND ol_d_id = $2
                AND ol_o_id = $3
        ", &[&warehouse_id, &district_id, &o_id]).await?;

        transaction.commit().await?;
        Ok(start.elapsed().as_micros())
    }

    pub async fn stock_level(client: &mut AsyncClient, warehouse_id :i32, _start_id :u32, _end_id :u32) -> Result<u128, Box<dyn std::error::Error>> {
        let district_id :i32 = rand::thread_rng()
            .gen_range(1..=10);
        let threshold :i32 = rand::thread_rng()
            .gen_range(10..=20);

        let start = Instant::now();
        let transaction = client.transaction().await?;

        let row_district = transaction.query(r"
            SELECT d_next_o_id
            FROM district
            WHERE d_w_id = $1 AND d_id = $2
        ", &[&warehouse_id, &district_id]).await?;

        if row_district.len() == 0 {
            transaction.rollback().await?;
            return Err(Box::new(TPCCError("Stock-Level transaction rollbacked. District not found.".into())));
        }

        let d_next_o_id :i32 = row_district[0].get("d_next_o_id");

        let rows_order_line = transaction.query(r"
            SELECT
                DISTINCT ol_i_id
            FROM order_line
            WHERE
                ol_w_id = $1
                AND ol_d_id = $2
                AND ol_o_id < $3
                AND ol_o_id >= ($3 - 20)
        ", &[&warehouse_id, &district_id, &d_next_o_id]).await?;

        for row in rows_order_line {
            let ol_i_id :i32 = row.get("ol_i_id");
            transaction.query(r"
                SELECT s_quantity
                FROM stock
                WHERE
                    s_w_id = $1
                    AND s_i_id = $2
                    AND s_quantity < $3
            ", &[&warehouse_id, &ol_i_id, &threshold]).await?;
        }

        transaction.commit().await?;
        Ok(start.elapsed().as_micros())
    }

    // Returns a randomly generated alphanumeric string of length between min_length and max_length
    fn random_alpha_string(min_length :usize, max_length: usize) -> String {
        let mut string_length: usize = max_length;
        if min_length < max_length {
            string_length = rand::thread_rng()
                .gen_range(min_length..=max_length);
        }
        let string_val :String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(string_length)
            .map(char::from)
            .collect();

        string_val
    }

    // Returns a randomly generated Zip code
    fn random_zip() -> String {
        let part1 :u16 = rand::thread_rng()
            .gen_range(1..10000);

        format!("{:0>4}11111", part1)
    }

    // Returns a randomly generated state code (2 capital letters)
    fn random_state() -> String {
        let mut rng = &mut rand::thread_rng();
        let sample = "ABCDEFGHIJKLMNOPQRSTUVWXYZ".as_bytes();
        let val: Vec<u8> = sample.choose_multiple(&mut rng, 2).cloned().collect();

        String::from_utf8(val).unwrap()
    }

    // Generate customer's last name
    fn gen_last(customer_id :u32) -> String {
        let mut f_customer_id :u32 = customer_id;
        if customer_id >= 1000 {
            f_customer_id = rand::thread_rng()
                .gen_range(0..=999);
        }
        let syllables = vec!["BAR", "OUGHT", "ABLE", "PRIS", "PRES", "ESE", "ANTI",
            "CALLY", "ATION", "EING"];

        let f_customer_id_fmt = format!("{:0>3}", f_customer_id);

        let mut last :String = "".to_string();
        for c in f_customer_id_fmt.chars() {
            let i :u32 = c.to_digit(10).unwrap();
            last = format!("{}{}", last, syllables[i as usize]);
        }

        last
    }

    pub fn populate_item(client: &mut Client) -> Result<(), String> {
        // Populate the item table with 100_000 items
        let n_items = 100_000 as u32;
        // Number of lines submitted for each COPY operation
        let batch_size = 500 as u32;
        // Number of batch, based on the total number of items and batch size
        let n_batch = ((n_items as f64 / batch_size as f64) as f64).ceil() as u32;

        for b in 1..=n_batch {
            // Start a new copy from stdin op.
            let mut writer = match client.copy_in("COPY item FROM stdin") {
                Ok(w) => w,
                Err(e) => return Err(e.to_string()),
            };

            // Calculate items interval
            let batch_start = (b * batch_size - batch_size + 1) as u32;
            let mut batch_end = (b * batch_size) as u32;
            if batch_end > n_items {
                batch_end = n_items;
            }

            // Fill the write buffer batch_size items
            for i_id in batch_start..=batch_end {
                // Generate random data
                // Column i_name
                let i_name :String = TPCC::random_alpha_string(14, 24);
                // Column i_price
                let i_price :f64 = rand::thread_rng()
                    .gen_range(1.00..=100.00);
                // Column i_im_id
                let i_im_id :i32 = rand::thread_rng()
                    .gen_range(1..=10_000);
                // Column i_data
                let mut i_data :String = TPCC::random_alpha_string(26, 50);
                let i_data_length = i_data.len();
                let orig :u32 = rand::thread_rng()
                    .gen_range(1..=100);
                if orig <= 10 {
                    let pos :usize = rand::thread_rng()
                        .gen_range(1..(i_data_length - 8));
                    i_data.replace_range(pos..=(pos + 8), "ORIGINAL");
                }

                let line = format!("{}\t{}\t{}\t{:.2}\t{}\n", i_id, i_im_id, i_name, i_price, i_data);
                match writer.write_all(line.as_bytes()) {
                    Ok(_) => (),
                    Err(e) => return Err(e.to_string()),
                }
            }

            // Finishing the copy order for the current batch
            match writer.finish() {
                Ok(_) => (),
                Err(e) => return Err(e.to_string()),
            }
        }

        Ok(())
    }

    pub fn populate_warehouse(client: &mut Client, warehouse_id: u32) -> Result<(), String> {
        // Start a new copy from stdin op.
        let mut writer = match client.copy_in("COPY warehouse FROM stdin") {
            Ok(w) => w,
            Err(e) => return Err(e.to_string()),
        };

        // Generate random data
        let w_name :String = TPCC::random_alpha_string(6, 10);
        let w_street1 :String = TPCC::random_alpha_string(10, 20);
        let w_street2 :String = TPCC::random_alpha_string(10, 20);
        let w_city :String = TPCC::random_alpha_string(10, 20);
        let w_state: String = TPCC::random_state();
        let w_zip: String = TPCC::random_zip();
        let w_tax: f64 = rand::thread_rng()
            .gen_range(0.10..0.20);

        let line = format!("{}\t{}\t{}\t{}\t{}\t{}\t{}\t{:.4}\t300000.00\n", warehouse_id, w_name, w_street1, w_street2, w_city, w_state, w_zip, w_tax);

        match writer.write_all(line.as_bytes()) {
            Ok(_) => (),
            Err(e) => return Err(e.to_string()),
        }
        // Finishing the copy order
        match writer.finish() {
            Ok(_) => (),
            Err(e) => return Err(e.to_string()),
        }

        Ok(())
    }

    pub fn populate_district(client: &mut Client, warehouse_id: u32) -> Result<(), String> {
        // Calculate district ids interval
        let district_start = 1;
        let district_end = 10;

        // Start a new copy from stdin op.
        let mut writer = match client.copy_in("COPY district FROM stdin") {
            Ok(w) => w,
            Err(e) => return Err(e.to_string()),
        };

        // Fill the write buffer
        for district_id in district_start..=district_end {
            // Generate random data
            let d_name :String = TPCC::random_alpha_string(6, 10);
            let d_street1 :String = TPCC::random_alpha_string(10, 20);
            let d_street2 :String = TPCC::random_alpha_string(10, 20);
            let d_city :String = TPCC::random_alpha_string(10, 20);
            let d_state: String = TPCC::random_state();
            let d_zip: String = TPCC::random_zip();
            let d_tax: f64 = rand::thread_rng()
                .gen_range(0.10..0.20);

            let line = format!("{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{:.4}\t30000.00\t3001\n", district_id, warehouse_id, d_name, d_street1, d_street2, d_city, d_state, d_zip, d_tax);

            match writer.write_all(line.as_bytes()) {
                Ok(_) => (),
                Err(e) => return Err(e.to_string()),
            }
        }

        // Finishing the copy order
        match writer.finish() {
            Ok(_) => (),
            Err(e) => return Err(e.to_string()),
        }

        Ok(())
    }

    pub fn populate_customer(client: &mut Client, warehouse_id: u32) -> Result<(), String> {
        // Populate the customer table with 30_000 items per warehouse
        let n_items = 30_000 as u32;
        let n_customer_per_district = 3_000 as u32;

        let mut customer_id :u32 = 1;
        let mut district_id :u32 = 1;

        // Number of lines submitted for each COPY operation
        let batch_size = 500 as u32;
        // Number of batch, based on the number of items and batch size
        let n_batch = ((n_items as f64 / batch_size as f64) as f64).ceil() as u32;

        // Column since
        let c_since: String = format!("{}", Utc::now().format("%Y-%m-%d %H:%M:%S"));

        for b in 1..=n_batch {
            // Start a new copy from stdin op.
            let mut writer = match client.copy_in("COPY customer FROM stdin") {
                Ok(w) => w,
                Err(e) => return Err(e.to_string()),
            };

            // Calculate items interval
            let batch_start = (b * batch_size - batch_size + 1) as u32;
            let mut batch_end = (b * batch_size) as u32;
            if batch_end > n_items {
                batch_end = n_items;
            }

            // Fill the write buffer batch_size items
            for _ in batch_start..=batch_end {
                let c_first :String = TPCC::random_alpha_string(8, 16);
                let c_middle: String = "OE".to_string();
                let c_last: String = TPCC::gen_last(customer_id);
                let c_street1 :String = TPCC::random_alpha_string(10, 20);
                let c_street2 :String = TPCC::random_alpha_string(10, 20);
                let c_city :String = TPCC::random_alpha_string(10, 20);
                let c_state: String = TPCC::random_state();
                let c_zip: String = TPCC::random_zip();
                let c_phone: u64 = rand::thread_rng()
                    .gen_range(1000000000000000..=9999999999999999);
                let c_discount: f64 = rand::thread_rng()
                    .gen_range(0.00..=0.50);
                let c_data: String = TPCC::random_alpha_string(300, 500);
                // Column c_credit
                let mut c_credit: String = "GC".to_string();
                let i = rand::thread_rng()
                    .gen_range(1..=10);
                if i == 1 {
                    c_credit = "BC".to_string();
                }

                let line = format!("{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t50000.00\t{:.2}\t-10.00\t10.00\t1\t0\t{}\n", customer_id, district_id, warehouse_id, c_first, c_middle, c_last, c_street1, c_street2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_discount, c_data);

                match writer.write_all(line.as_bytes()) {
                    Ok(_) => (),
                    Err(e) => return Err(e.to_string()),
                }
                // Increment customer_id
                customer_id += 1;

                // Increment district_id and reset customer_id if we have populated
                // n_customer_per_district customers.
                if customer_id > n_customer_per_district {
                    district_id += 1;
                    customer_id = 1;
                }
            }
            // Finishing the copy order for the current batch
            match writer.finish() {
                Ok(_) => (),
                Err(e) => return Err(e.to_string()),
            }
        }
        Ok(())
    }

    pub fn populate_orders(client: &mut Client, warehouse_id: u32, o_entry_d: String) -> Result<(), String> {
        // Populate the orders table with 30_000 items per warehouse (1 per customer)
        let n_items = 30_000 as u32;
        // Number of lines submitted for each COPY operation
        let batch_size = 500 as u32;
        // Number of batch, based on the number of items and batch size
        let n_batch = ((n_items as f64 / batch_size as f64) as f64).ceil() as u32;

        let n_orders_per_district :u32 = 3_000;
        let mut orders_id :u32 = 1;
        let mut customer_id: u32 = 1;
        let mut district_id: u32 = 1;

        for b in 1..=n_batch {
            // Start a new copy from stdin op.
            let mut writer = match client.copy_in("COPY orders FROM stdin NULL AS ''") {
                Ok(w) => w,
                Err(e) => return Err(e.to_string()),
            };

            // Calculate items interval
            let batch_start = (b * batch_size - batch_size + 1) as u32;
            let mut batch_end = (b * batch_size) as u32;
            if batch_end > n_items {
                batch_end = n_items;
            }

            // Fill the write buffer batch_size items
            for _ in batch_start..=batch_end {
                // Column o_carrier_id
                let carrier_id :u32 = rand::thread_rng()
                    .gen_range(1..=10);
                let mut o_carrier_id :String = "".to_string();
                if orders_id < 2_101 {
                    o_carrier_id = format!("{}", carrier_id);
                }
                // Generate the number of order_line entries
                let o_ol_cnt :u32 = (orders_id * (orders_id + district_id + warehouse_id)) % 11 + 5;

                let line = format!("{}\t{}\t{}\t{}\t{}\t{}\t{}\t1\n", orders_id, district_id, warehouse_id, customer_id, o_entry_d, o_carrier_id, o_ol_cnt);

                match writer.write_all(line.as_bytes()) {
                    Ok(_) => (),
                    Err(e) => return Err(e.to_string()),
                }
                // Increment orders_id and customer_id
                orders_id += 1;
                customer_id += 1;
                // Increment district_id and reset orders_id and customer_id if we have populated
                // n_orders_per_district orders.
                if orders_id > n_orders_per_district {
                    district_id += 1;
                    orders_id = 1;
                    customer_id = 1;
                }
            }
            // Finishing the copy order for the current batch
            match writer.finish() {
                Ok(_) => (),
                Err(e) => return Err(e.to_string()),
            }
        }
        Ok(())
    }

    pub fn populate_history(client: &mut Client, warehouse_id: u32) -> Result<(), String> {
        // Populate the history table with 30_000 items per warehouse
        let n_items = 30_000 as u32;
        // Number of lines submitted for each COPY operation
        let batch_size = 500 as u32;
        // Number of batch, based on the number of items and batch size
        let n_batch = ((n_items as f64 / batch_size as f64) as f64).ceil() as u32;

        let n_customer_per_district :u32 = 3_000;
        let mut customer_id: u32 = 1;
        let mut district_id: u32 = 1;

        // Column date
        let h_date: String = format!("{}", Utc::now().format("%Y-%m-%d %H:%M:%S"));

        for b in 1..=n_batch {
            // Start a new copy from stdin op.
            let mut writer = match client.copy_in("COPY history FROM stdin") {
                Ok(w) => w,
                Err(e) => return Err(e.to_string()),
            };

            // Calculate items interval
            let batch_start = (b * batch_size - batch_size + 1) as u32;
            let mut batch_end = (b * batch_size) as u32;
            if batch_end > n_items {
                batch_end = n_items;
            }

            // Fill the write buffer batch_size items
            for _ in batch_start..=batch_end {
                // Column h_data
                let h_data :String = TPCC::random_alpha_string(12, 24);
                let line = format!("{}\t{}\t{}\t{}\t{}\t{}\t10.0\t{}\n", customer_id, district_id, warehouse_id, district_id, warehouse_id, h_date, h_data);

                match writer.write_all(line.as_bytes()) {
                    Ok(_) => (),
                    Err(e) => return Err(e.to_string()),
                }
                // Increment customer_id
                customer_id += 1;
                // Increment district_id and reset customer_id if we have populated
                // n_customer_per_district history.
                if customer_id > n_customer_per_district {
                    district_id += 1;
                    customer_id = 1;
                }
            }
            // Finishing the copy order for the current batch
            match writer.finish() {
                Ok(_) => (),
                Err(e) => return Err(e.to_string()),
            }
        }
        Ok(())
    }

    pub fn populate_stock(client: &mut Client, warehouse_id: u32) -> Result<(), String> {
        // Populate the stock table with 100_000 items per warehouse
        let n_items = 100_000 as u32;

        // Number of lines submitted for each COPY operation
        let batch_size = 500 as u32;
        // Number of batch, based on the number of items and batch size
        let n_batch = ((n_items as f64 / batch_size as f64) as f64).ceil() as u32;

        let mut item_id :u32 = 1;

        for b in 1..=n_batch {
            // Start a new copy from stdin op.
            let mut writer = match client.copy_in("COPY stock FROM stdin") {
                Ok(w) => w,
                Err(e) => return Err(e.to_string()),
            };

            // Calculate items interval
            let batch_start = (b * batch_size - batch_size + 1) as u32;
            let mut batch_end = (b * batch_size) as u32;
            if batch_end > n_items {
                batch_end = n_items;
            }

            // Fill the write buffer batch_size items
            for _ in batch_start..=batch_end {
                let s_dist_01 :String = TPCC::random_alpha_string(24, 24);
                let s_dist_02 :String = TPCC::random_alpha_string(24, 24);
                let s_dist_03 :String = TPCC::random_alpha_string(24, 24);
                let s_dist_04 :String = TPCC::random_alpha_string(24, 24);
                let s_dist_05 :String = TPCC::random_alpha_string(24, 24);
                let s_dist_06 :String = TPCC::random_alpha_string(24, 24);
                let s_dist_07 :String = TPCC::random_alpha_string(24, 24);
                let s_dist_08 :String = TPCC::random_alpha_string(24, 24);
                let s_dist_09 :String = TPCC::random_alpha_string(24, 24);
                let s_dist_10 :String = TPCC::random_alpha_string(24, 24);
                let s_quantity: u32 = rand::thread_rng()
                    .gen_range(10..=100);
                // Column s_data
                let mut s_data :String = TPCC::random_alpha_string(26, 50);
                let s_data_length = s_data.len();
                let orig :u32 = rand::thread_rng()
                    .gen_range(1..=100);
                if orig <= 10 {
                    let pos :usize = rand::thread_rng()
                        .gen_range(1..(s_data_length - 8));
                    s_data.replace_range(pos..=(pos + 8), "ORIGINAL");
                }

                let line = format!("{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t0\t0\t0\t{}\n", item_id, warehouse_id, s_quantity, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10, s_data);

                match writer.write_all(line.as_bytes()) {
                    Ok(_) => (),
                    Err(e) => return Err(e.to_string()),
                }
                // Increment item_id
                item_id += 1;
            }
            // Finishing the copy order for the current batch
            match writer.finish() {
                Ok(_) => (),
                Err(e) => return Err(e.to_string()),
            }
        }
        Ok(())
    }

    pub fn populate_new_order(client: &mut Client, warehouse_id: u32) -> Result<(), String> {
        // Populate the new_order table with 9_000 items per warehouse
        let n_items = 9_000 as u32;

        let orders_start = 2101;
        let orders_end = 3000;

        // Number of lines submitted for each COPY operation
        let batch_size = 500 as u32;
        // Number of batch, based on the number of items and batch size
        let n_batch = ((n_items as f64 / batch_size as f64) as f64).ceil() as u32;

        // Initialize orders_id and district_id
        let mut orders_id = orders_start;
        let mut district_id = 1;

        for b in 1..=n_batch {
            // Start a new copy from stdin op.
            let mut writer = match client.copy_in("COPY new_order FROM stdin") {
                Ok(w) => w,
                Err(e) => return Err(e.to_string()),
            };

            // Calculate items interval
            let batch_start = (b * batch_size - batch_size + 1) as u32;
            let mut batch_end = (b * batch_size) as u32;
            if batch_end > n_items {
                batch_end = n_items;
            }

            // Fill the write buffer batch_size items
            for _ in batch_start..=batch_end {
                let line = format!("{}\t{}\t{}\n", orders_id, district_id, warehouse_id);
                match writer.write_all(line.as_bytes()) {
                    Ok(_) => (),
                    Err(e) => return Err(e.to_string()),
                }
                // Increment orders_id
                orders_id += 1;

                // Check if we have reached the "orders" limit and then move to the next district
                if orders_id > orders_end {
                    orders_id = orders_start;
                    district_id += 1;
                }
            }
            // Finishing the copy order for the current batch
            match writer.finish() {
                Ok(_) => (),
                Err(e) => return Err(e.to_string()),
            }
        }
        Ok(())
    }

    pub fn populate_order_line(client: &mut Client, warehouse_id: u32, ol_delivery_d :String) -> Result<(), String> {
        // Populate the order_line table for 30_000 orders per warehouse
        // Each orders has between 5 and 15 order_line entries
        let n_orders = 30_000 as u32;
        // Number of lines submitted for each COPY operation
        // This is set to a low value because we expect to get *~10 more lines at each iteration of
        // the main loop. Batch size will be actually growing to around 500 lines.
        let batch_size = 50 as u32;
        // Number of batch, based on the total number of orders and batch size
        let n_batch = ((n_orders as f64 / batch_size as f64) as f64).ceil() as u32;

        let n_orders_per_district :u32 = 3_000;

        let mut orders_id :u32 = 1;
        let mut district_id: u32 = 1;

        for b in 1..=n_batch {
            // Start a new copy from stdin op.
            let mut writer = match client.copy_in("COPY order_line FROM stdin NULL AS ''") {
                Ok(w) => w,
                Err(e) => return Err(e.to_string()),
            };

            // Calculate items interval
            let batch_start = (b * batch_size - batch_size + 1) as u32;
            let mut batch_end = (b * batch_size) as u32;
            if batch_end > n_orders {
                batch_end = n_orders;
            }

            // Fill the write buffer batch_size items
            for _ in batch_start..=batch_end {
                // Generate the number of order_line entries
                let ol_cnt :u32 = (orders_id * (orders_id + district_id + warehouse_id)) % 11 + 5;

                // Generate the list of item ids we will need
                let mut rng = rand::thread_rng();
                let item_ids = rand::seq::index::sample(&mut rng, 100_000, ol_cnt as usize).into_vec();

                let mut cur_ol_amount: f64;
                let mut cur_ol_delivery_d: String;
                // Build one line per item id.
                for i in 1..=ol_cnt {
                    let item_id = item_ids[(i - 1) as usize] + 1;
                    // Build ol_amount and ol_delivery_d
                    if orders_id >= 2101 {
                        cur_ol_amount = rand::thread_rng()
                            .gen_range(0.01..9999.99);
                        cur_ol_delivery_d = "".to_string();
                    }
                    else {
                        cur_ol_amount = 0.00;
                        cur_ol_delivery_d = ol_delivery_d.clone();
                    }
                    // Column ol_dist_info
                    let ol_dist_info :String = TPCC::random_alpha_string(24, 24);

                    let line = format!("{}\t{}\t{}\t{}\t{}\t{}\t{}\t5\t{}\t{}\n", orders_id, district_id, warehouse_id, i, item_id, warehouse_id, cur_ol_delivery_d, cur_ol_amount, ol_dist_info);

                    match writer.write_all(line.as_bytes()) {
                        Ok(_) => (),
                        Err(e) => return Err(e.to_string()),
                    }
                }
                // Increment orders_id
                orders_id += 1;
                // Increment district_id and reset orders_id if we have populated n_orders_per_district
                // orders.
                if orders_id > n_orders_per_district {
                    district_id += 1;
                    orders_id = 1;
                }
            }
            // Finishing the copy order for the current batch
            match writer.finish() {
                Ok(_) => (),
                Err(e) => return Err(e.to_string()),
            }
        }
        Ok(())
    }
}

#[async_trait]
impl ReadWrite for TPCC {
    async fn execute_rw_transaction(&self, client :&mut AsyncClient, transaction :&BenchmarkTransaction) -> Result<u128, Box<dyn std::error::Error>> {
        // Generate the warehouse id we are going to hit
        // The used type is i32 because it matches with Postgres' int4 type.
        let warehouse_id :i32 = rand::thread_rng()
            .gen_range(self.start_id..=self.end_id) as i32;

        match transaction.id {
            1 => {
                match TPCC::delivery(client, warehouse_id, self.start_id, self.end_id).await {
                    Ok(duration) => return Ok(duration),
                    Err(e) => return Err(Box::new(TPCCError(e.to_string()))),
                }
            },
            2 => {
                match TPCC::new_order(client, warehouse_id, self.start_id, self.end_id).await {
                    Ok(duration) => return Ok(duration),
                    Err(e) => return Err(Box::new(TPCCError(e.to_string()))),
                }
            },
            3 => {
                match TPCC::payment(client, warehouse_id, self.start_id, self.end_id).await {
                    Ok(duration) => return Ok(duration),
                    Err(e) => return Err(Box::new(TPCCError(e.to_string()))),
                }
            },
            4 => {
                match TPCC::order_status(client, warehouse_id, self.start_id, self.end_id).await {
                    Ok(duration) => return Ok(duration),
                    Err(e) => return Err(Box::new(TPCCError(e.to_string()))),
                }
            },
            5 => {
                match TPCC::stock_level(client, warehouse_id, self.start_id, self.end_id).await {
                    Ok(duration) => return Ok(duration),
                    Err(e) => return Err(Box::new(TPCCError(e.to_string()))),
                }
            },
            0 | 6..=u16::MAX => todo!(),
        };
    }
}

impl InitializeSchema for TPCC {
    fn initialize_schema(&self, client: &mut Client) -> Result<u128, postgres::Error> {
        let start = Instant::now();

        let mut transaction = client.transaction()?;
        // Sequentially create tables
        for table_ddl in self.table_ddls.iter() {
            transaction.batch_execute(&table_ddl.sql)?;
        }
        transaction.commit()?;

        Ok(start.elapsed().as_micros())
    }
}

impl PreLoadData for TPCC {
    // On TPC-C-like benchmark, we need to:
    // - populate the item table with 100k randomly generated rows
    fn pre_load_data(&self, client: &mut Client) -> Result<u128, String> {
        let start = Instant::now();

        // Populate the item table
        TPCC::populate_item(client)?;

        Ok(start.elapsed().as_micros())
    }
}

impl LoadData for TPCC {
    fn load_data(&self, client: &mut Client, warehouse_ids: Vec<u32>) -> Result<u128, String> {
        let start = Instant::now();
        for warehouse_id in warehouse_ids {
            // Orders entry date
            let o_entry_d: String = format!("{}", Utc::now().format("%Y-%m-%d %H:%M:%S"));
            // Populate tables
            TPCC::populate_warehouse(client, warehouse_id)?;
            TPCC::populate_district(client, warehouse_id)?;
            TPCC::populate_stock(client, warehouse_id)?;
            TPCC::populate_customer(client, warehouse_id)?;
            TPCC::populate_history(client, warehouse_id)?;
            TPCC::populate_orders(client, warehouse_id, o_entry_d.clone())?;
            TPCC::populate_new_order(client, warehouse_id)?;
            TPCC::populate_order_line(client, warehouse_id, o_entry_d.clone())?;
        }
        Ok(start.elapsed().as_micros())
    }
}

impl AddPrimaryKeys for TPCC {
    fn add_primary_keys(&self, client: &mut Client, ddls: Vec<String>) -> Result<u128, postgres::Error> {
        let start = Instant::now();

        for ddl in ddls.iter() {
            let mut transaction = client.transaction()?;
            transaction.batch_execute(ddl)?;
            transaction.commit()?;
        }

        Ok(start.elapsed().as_micros())
    }
}

impl AddForeignKeys for TPCC {
    fn add_foreign_keys(&self, client: &mut Client, ddls: Vec<String>) -> Result<u128, postgres::Error> {
        let start = Instant::now();

        for ddl in ddls.iter() {
            let mut transaction = client.transaction()?;
            transaction.batch_execute(ddl)?;
            transaction.commit()?;
        }

        Ok(start.elapsed().as_micros())
    }
}

impl AddIndexes for TPCC {
    fn add_indexes(&self, client: &mut Client, ddls: Vec<String>) -> Result<u128, postgres::Error> {
        let start = Instant::now();

        for ddl in ddls.iter() {
            let mut transaction = client.transaction()?;
            transaction.batch_execute(ddl)?;
            transaction.commit()?;
        }

        Ok(start.elapsed().as_micros())
    }
}

impl PrintResultsSummary for TPCC {
    fn print_results_summary(&self, counters: HashMap<u16, Counter>, duration_ms: Duration) {
        println!("");
        println!("Benchmark results:");
        let mut data = Vec::new();
        for id in counters.keys().sorted() {
            if let Some(c) = counters.get(id) {
                data.push(
                    TPCCTransactionSummary::new(
                        self.get_transaction_description(*id),
                        (*c).n_commits,
                        (*c).n_total - (*c).n_commits,
                        ((*c).n_total - (*c).n_commits) as f64 / (*c).n_total as f64 * 100.0,
                        (*c).total_duration_ms / (*c).n_commits as f64,
                        ((*c).n_commits as f64 / duration_ms.as_secs() as f64 * 60.0) as u32
                    )
                );
            }
        }
        let mut table = Table::from_iter(&data);
        let style = Style::rounded();
        table
            .with(style)
            .with(
                Rows::first()
                    .modify()
                    .with(Alignment::center())
            )
            .with(
                Columns::single(1)
                    .not(Rows::first())
                    .modify()
                    .with(Alignment::right())
            )
            .with(
                Columns::single(2)
                    .not(Rows::first())
                    .modify()
                    .with(Alignment::right())
            )
            .with(
                Columns::single(3)
                    .not(Rows::first())
                    .modify()
                    .with(|s: &str| format!("{val:.*} %", 3,  val=s.parse::<f64>().unwrap()))
                    .with(Alignment::right())
            )
            .with(
                Columns::single(4)
                    .not(Rows::first())
                    .modify()
                    .with(|s: &str| format!("{val:.*} ms", 3, val=s.parse::<f64>().unwrap()))
                    .with(Alignment::right())
            )
            .with(
                Columns::single(5)
                    .not(Rows::first())
                    .modify()
                    .with(|s: &str| format!("{}", s.parse::<u32>().unwrap()))
                    .with(Alignment::right())
            );

        println!("{}", table);
    }
}

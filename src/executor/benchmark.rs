use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use postgres::Client;
use tokio_postgres::{Client as AsyncClient};

// Transaction specifications
#[derive(Clone)]
pub struct BenchmarkTransaction {
    // Must be unique and greater than 0
    pub id: u16,
    // Probability of the transaction to be executed. From 0 (never) to 100 (alaways executed).
    pub weight: u16,
    // Description of the transaction, useful for the report
    pub description: String,
}

#[derive(Clone)]
pub struct Counter {
    pub n_commits: u64,
    pub n_total: u64,
    pub total_duration_ms: f64,
}

#[derive(Clone)]
pub struct BenchmarkStmt {
    pub sql: String,
}

// ReadWrite trait for all benchmarks implementing read/write workload
#[async_trait]
pub trait ReadWrite {
    async fn execute_rw_transaction(&self, client :&mut AsyncClient, transaction :&BenchmarkTransaction) -> Result<u128, Box<dyn std::error::Error>>;
}

pub trait Benchmark:ReadWrite {
    fn initialize_schema(&self, client: &mut Client) -> Result<u128, postgres::Error>;
    fn pre_load_data(&self, client: &mut Client) -> Result<u128, String>;
    fn load_data(&self, client: &mut Client, ids: Vec<u32>) -> Result<u128, String>;
    fn print_results_summary(&self, counters: HashMap<u16, Counter>, duration_ms: Duration);
    fn get_default_max_id(&self, client: &mut Client) -> Result<u32, postgres::Error>;
    fn get_transactions_rw(&self) -> Vec<BenchmarkTransaction>;
    fn get_table_ddls(&self) -> Vec<BenchmarkStmt>;
    fn get_pkey_ddls(&self) -> Vec<BenchmarkStmt>;
    fn get_fkey_ddls(&self) -> Vec<BenchmarkStmt>;
    fn get_index_ddls(&self) -> Vec<BenchmarkStmt>;
    fn get_vacuum_stmts(&self) -> Vec<BenchmarkStmt>;
}

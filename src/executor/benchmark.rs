use postgres::Client;


// Type defining a pointer to a function in charge of executing transactions
type ExecuteBenchmarkTransaction = fn(&mut Client, i32, u32, u32) -> Result<u128, Box<dyn std::error::Error>>;

// Transaction specifications
pub struct BenchmarkTransaction {
    // Must be unique and greater than 0
    pub id: u16,
    // Probability of the transaction to be executed. From 0 (never) to 100 (alaways executed).
    pub weight: u16,
    // Description of the transaction, useful for the report
    pub description: String,
    // Pointer to the function in charge of executing the transaction
    pub execute: ExecuteBenchmarkTransaction,
}

pub struct BenchmarkDDL {
    pub sql: String,
}

// ReadWrite trait for all benchmarks implementing read/write workload
pub trait ReadWrite {
    fn execute_rw_transaction(&self, client :&mut Client, transaction :&BenchmarkTransaction) -> Result<u128, Box<dyn std::error::Error>>;
}

// InitializeSchema trait: tables re-creation
pub trait InitializeSchema {
    fn initialize_schema(&self, client: &mut Client) -> Result<u128, postgres::Error>;
}

// PreLoadData trait: execute various operations after the DDL exec. operation and before
// concurrently loading data into the database.
pub trait PreLoadData {
    fn pre_load_data(&self, client: &mut Client) -> Result<u128, String>;
}

pub trait LoadData {
    fn load_data(&self, client: &mut Client, ids: Vec<u32>) -> Result<u128, String>;
}

pub trait AddPrimaryKeys {
    fn add_primary_keys(&self, client: &mut Client, ddls: Vec<String>) -> Result<u128, postgres::Error>;
}

pub trait AddForeignKeys {
    fn add_foreign_keys(&self, client: &mut Client, ddls: Vec<String>) -> Result<u128, postgres::Error>;
}

pub trait AddIndexes {
    fn add_indexes(&self, client: &mut Client, ddls: Vec<String>) -> Result<u128, postgres::Error>;
}

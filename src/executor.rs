use std::time::{Duration, Instant};
use std::thread::{JoinHandle, sleep};
use std::thread;
use std::fs::File;
use std::io::prelude::*;
use std::io::LineWriter;
use std::collections::HashMap;

use chrono::Utc;
use crossbeam_channel::{Sender, Receiver, unbounded};
use postgres::{Client, NoTls};
use rand::prelude::*;
use tokio::runtime::Runtime;
use tokio_postgres::{NoTls as AsyncNoTls};

mod benchmark;
mod txmessage;
mod tpcc;

use benchmark::{
    AddPrimaryKeys,
    AddForeignKeys,
    InitializeSchema,
    LoadData,
    PreLoadData,
    ReadWrite,
};
use txmessage::{TXMessage, TXMessageKind};
use super::args::{RunArgs};

pub struct Counter {
    n_commits: u64,
    n_total: u64,
    total_duration_ms: f64,
}

pub struct Executor {
    dsn: String,
    benchmark_type: String,
    counters: HashMap<u16, Counter>,
    total_time_ms: u128,
}

impl Executor {
    pub fn new(dsn: String, benchmark_type: String) -> Executor {
        Executor { dsn: dsn, benchmark_type: benchmark_type, counters: HashMap::new(), total_time_ms: 0 }
    }

    // Execute read/write mixed workload
    pub fn run_benchmark(&mut self, args :RunArgs) -> &mut Self {
        let rampup_ms = args.rampup as u64 * 1000;
        let time_ms = args.time as u64 * 1000;
        // Nap time before starting a new client
        let sleep_ms = rampup_ms / args.client as u64;

        // Channels used to communicate transactions states: id, duration, committed?, etc..
        let (tx, rx): (Sender<TXMessage>, Receiver<TXMessage>) = unbounded();
        // Channels used to send back the counters once data collector has finished its work.
        let (tx_counters, rx_counters): (Sender<HashMap<u16, Counter>>, Receiver<HashMap<u16, Counter>>) = unbounded();

        let mut benchmark_clients = Vec::new();

        // Start data collector
        let dc_tx_counters = tx_counters.clone();
        let data_collector = self.start_data_collector("transaction.log".to_string(), "error.log".to_string(), rx, dc_tx_counters);

        // Track total execution time in ms
        let start = Instant::now();

        println!("Rampup duration (s): {}", args.rampup);
        println!("Starting {} client(s) ...", args.client);

        // Create the tokio runtime
        let rt = Runtime::new().unwrap();

        rt.block_on(async {
            // Start the clients
            for n in 1..=args.client {
                // Test duration calculated by taking in consideration the rampup time and the
                // remaining duration before the end of rampup stage.
                let duration_ms = time_ms + rampup_ms - n as u64 * sleep_ms;

                // Sleep accordingly to the rampup time and the number of clients
                sleep(Duration::from_millis(sleep_ms));

                // Start one new client
                let benchmark_client = self.start_rw_client(duration_ms, self.dsn.clone(), args.scalefactor.clone(), args.start_id.clone(), args.end_id.clone(), tx.clone()).await;

                benchmark_clients.push(benchmark_client);
            }
            println!("All clients started, running the workload for {} s ...", args.time);

            for benchmark_client in benchmark_clients {
                benchmark_client.await.expect("the client thread panicked");
            }
        });
        // Proceed total execution time
        self.total_time_ms = start.elapsed().as_millis();

        // Send termination message to the data collector
        tx.send(TXMessage::terminate_data_collector()).unwrap();
        // Wait for the end of the data collection thread
        data_collector.join().expect("the data collector thread panicked");

        // Receive counters from the data collector
        self.counters = rx_counters.recv().unwrap();

        self
    }

    // Prints benchmark results
    pub fn print_results(&mut self) -> &mut Self {
        let duration = Duration::from_millis(self.total_time_ms as u64);

        println!("");
        println!("Benchmark results:");
        for id in self.counters.keys() {
            if let Some(c) = self.counters.get(id) {
                println!("------------------------------------------");
                println!("-> Transaction {}: {}", id, (*c).n_commits);
                println!("-> Errors: {}", ((*c).n_total - (*c).n_commits));
                println!("-> Average response time (ms): {:.3}", ((*c).total_duration_ms / (*c).n_commits as f64));
                println!("-> Duration (s): {:.3}", duration.as_secs_f64());
                println!("-> Transaction Per Second rate: {:.3} TPS", (*c).n_commits as f64 / duration.as_secs() as f64);
            }

        }

        self
    }

    // Start a new read/write benchmark client in its own thread
    async fn start_rw_client(&mut self, duration_ms: u64, dsn: String, scalefactor: u32, start_id: u32, end_id: u32, tx: Sender<TXMessage>) -> tokio::task::JoinHandle<()>
    {
        let benchmark_type = self.benchmark_type.clone();

        tokio::spawn(async move  {
            // New database connection
            let (mut client, connection) = match tokio_postgres::connect(&dsn, AsyncNoTls).await {
                Ok((client, connection)) => (client, connection),
                Err(error) => {
                    eprintln!("ERROR: {}", error);
                    std::process::exit(1);
                }
            };

            // The connection object performs the actual communication with the database,
            // so spawn it off to run on its own.
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    eprintln!("ERROR: {}", e);
                    std::process::exit(1);
                }
            });

            // Used for tracking client execution time
            let start = Instant::now();
            // Create a new benchmark object by thread because we don't want to share a such
            // complex structure between all the client threads
            let benchmark_client = match benchmark_type.as_str() {
                "tpcc" => tpcc::TPCC::new(scalefactor, start_id, end_id),
                _ => tpcc::TPCC::new(scalefactor, start_id, end_id),
            };


            loop {
                // Pickup a transaction, randomly and weight based.
                let transaction = {
                    let mut rng = thread_rng();
                    benchmark_client
                        .transactions_rw
                        .choose_weighted(&mut rng, |item| item.weight).unwrap()
                };
                // Execute the database transactions
                match benchmark_client.execute_rw_transaction(&mut client, &transaction).await {
                    Ok(duration) => {
                        // Send committed message
                        let m = TXMessage::committed(transaction.id, Utc::now().timestamp(), duration);
                        tx.send(m).unwrap();
                    },
                    Err(error) => {
                        // Send error message
                        let m = TXMessage::error(transaction.id, Utc::now().timestamp(), format!("{}", error));
                        tx.send(m).unwrap();
                    },
                }
                // Break the loop if we reach the time limit
                if start.elapsed().as_millis() >= duration_ms.into() {
                    break;
                }
            }
        })
    }

    // Start the data collector thread. Data collector is in charge of storing transaction
    // informations into the log file and incrementing counters.
    // Once the data collector has received the shutdown order (message with id=0), then
    // the counters are sent back to the main process through the tx_counters channel.
    fn start_data_collector(&mut self, log_file_path: String, error_file_path: String, rx: Receiver<TXMessage>, tx_counters: Sender<HashMap<u16, Counter>>) -> JoinHandle<()> {
        thread::spawn(move || {
            // Create the file where transaction logs are written
            let log_file = match File::create(&log_file_path) {
                Ok(f) => f,
                Err(e) => {
                    eprintln!("ERROR: Could not create {}: {}", &log_file_path, e);
                    std::process::exit(1);
                },
            };
            let mut log_file = LineWriter::new(log_file);

            let error_file = match File::create(&error_file_path) {
                Ok(f) => f,
                Err(e) => {
                    eprintln!("ERROR: Could not create {}: {}", &error_file_path, e);
                    std::process::exit(1);
                },
            };
            let mut error_file = LineWriter::new(error_file);

            // Initialize the counters
            let mut counters: HashMap<u16, Counter> = HashMap::new();

            loop {
                // Wait for a new message coming from the clients
                let msg = rx.recv().unwrap();

                // Exit thread
                match msg.kind {
                    // Terminate data collector
                    TXMessageKind::TERMINATE => {
                        break;
                    },
                    // Committed transaction
                    TXMessageKind::COMMITTED => {
                        // Counters calculation
                        let duration_ms = msg.tx_duration_us as f64 / 1000 as f64;
                        if let Some(c) = counters.get_mut(&msg.tx_id) {
                            (*c).n_commits += 1;
                            (*c).n_total += 1;
                            (*c).total_duration_ms += duration_ms;
                        }
                        else {
                            counters.insert(msg.tx_id, Counter {n_commits: 1, n_total: 1, total_duration_ms: duration_ms});
                        }

                        // Format and write the line to the log file
                        let line = format!("{} {} {}\n", msg.tx_timestamp, msg.tx_id, duration_ms);
                        log_file.write_all(line.as_bytes()).expect("Failed to write");
                    },
                    TXMessageKind::ERROR => {
                        // Counters calculation
                        if let Some(c) = counters.get_mut(&msg.tx_id) {
                            (*c).n_total += 1;
                        }
                        else {
                            counters.insert(msg.tx_id, Counter {n_commits: 0, n_total: 1, total_duration_ms: 0.0});
                        }

                        // Format and write the line to the log file
                        let line = format!("{} {} {}\n", msg.tx_timestamp, msg.tx_id, msg.error);
                        error_file.write_all(line.as_bytes()).expect("Failed to write");
                    },
                    TXMessageKind::DEFAULT => {
                        // Should not happen
                    },
                }
            }
            // Send counters
            tx_counters.send(counters).unwrap();
        })
    }

    // Open a new connection to the database and returns a Client
    fn connect(dsn: String) -> Client {
        Client::connect(&dsn, NoTls).unwrap_or_else(|err| {
            eprintln!("ERROR: {}", err);
            std::process::exit(1);
        })
    }

    // Initialize database schemabenchmark: create tables
    pub fn init_db_schema(&mut self) -> &mut Self {
        println!("Execute database DDLs...");
        // New database connection
        let mut client = Executor::connect(self.dsn.clone());

        // Load the corresponding benchmark client
        let benchmark_client = match self.benchmark_type.as_str() {
            "tpcc" => tpcc::TPCC::new(0, 0, 0),
            _ => tpcc::TPCC::new(0, 0, 0),
        };

        // Initialize the database model/schema
        let duration_us = match benchmark_client.initialize_schema(&mut client) {
            Ok(d) => d,
            Err(e) => {
                eprintln!("{}", e);
                std::process::exit(1);
            }
        };
        let duration_ms = duration_us as f64 / 1000 as f64;
        println!("duration (ms)={:.3}", duration_ms);

        self
    }

    // Generate benchmark data
    pub fn load_data(&mut self, scalefactor: u32, n_jobs: u32) -> &mut Self {
        // New database connection
        let mut client = Executor::connect(self.dsn.clone());

        // Load the corresponding benchmark client
        let benchmark_client = match self.benchmark_type.as_str() {
            "tpcc" => tpcc::TPCC::new(scalefactor, 0, 0),
            _ => tpcc::TPCC::new(scalefactor, 0, 0),
        };

        // Execute PreLoadData
        println!("Execute data pre-loading operation...");
        let duration_us = match benchmark_client.pre_load_data(&mut client) {
            Ok(d) => d,
            Err(e) => {
                eprintln!("{}", e);
                std::process::exit(1);
            }
        };
        let duration_ms = duration_us as f64 / 1000 as f64;
        println!("duration (ms)={:.3}", duration_ms);

        // Execute LoadData using multiple concurrent jobs
        let mut jobs = Vec::new();

        // Build the scalefactor ids matrix as follow (considering 3 jobs and 12 ids):
        // [1, 4, 7, 10]
        // [2, 5, 8, 11]
        // [3, 6, 9, 12]
        // We want to get one line per jobs and the ids balanced across the lines.
        let mut ids = Vec::with_capacity(n_jobs as usize);
        for _ in 0..n_jobs {
            ids.push(Vec::new());
        }
        for n in 1..=scalefactor {
            ids[(n % n_jobs) as usize].push(n);
        }

        println!("Execute data loading operation using {} jobs...", n_jobs);
        for j in 1..=n_jobs {
            // Cloning values before passing them to the thread
            let job_ids = ids[(j - 1) as usize].clone();
            let dsn = self.dsn.clone();

            // Load the corresponding benchmark client
            let job_benchmark_client = match self.benchmark_type.as_str() {
                "tpcc" => tpcc::TPCC::new(scalefactor, 0, 0),
                _ => tpcc::TPCC::new(scalefactor, 0, 0),
            };

            // Starting a new job into its dedicated thread
            let job = thread::spawn(move || {
                // New database connection
                let mut job_client = Executor::connect(dsn);

                let duration_us = match job_benchmark_client.load_data(&mut job_client, job_ids) {
                    Ok(d) => d,
                    Err(e) => {
                        eprintln!("{}", e);
                        std::process::exit(1);
                    }
                };

                let duration_ms = duration_us as f64 / 1000 as f64;
                println!("job #{} duration (ms)={:.3}", j, duration_ms);
            });

            jobs.push(job);
        }

        // Wait for the end of all jobs
        for job in jobs {
            job.join().expect("the client thread panicked");
        }

        self
    }

    pub fn add_primary_keys(&mut self, n_jobs: u32) -> &mut Self {
        // Load the corresponding benchmark
        let benchmark = match self.benchmark_type.as_str() {
            "tpcc" => tpcc::TPCC::new(0, 0, 0),
            _ => tpcc::TPCC::new(0, 0, 0),
        };

        // Execute AddContraints using multiple concurrent jobs
        let mut jobs = Vec::new();

        // We want to get one line per jobs and the ids balanced across the lines.
        let mut ddls = Vec::with_capacity(n_jobs as usize);
        for _ in 0..n_jobs {
            ddls.push(Vec::new());
        }
        let mut n = 1;
        for ddl in benchmark.pkey_ddls.iter() {
            ddls[(n % n_jobs) as usize].push(ddl.sql.clone());
            n += 1;
        }

        println!("Adding primary keys using {} jobs...", n_jobs);
        for j in 1..=n_jobs {
            // Cloning values before passing them to the thread
            let job_ddls = ddls[(j - 1) as usize].clone();
            let dsn = self.dsn.clone();

            // Load the corresponding benchmark client
            let job_benchmark_client = match self.benchmark_type.as_str() {
                "tpcc" => tpcc::TPCC::new(0, 0, 0),
                _ => tpcc::TPCC::new(0, 0, 0),
            };

            // Starting a new job into its dedicated thread
            let job = thread::spawn(move || {
                // New database connection
                let mut job_client = Executor::connect(dsn);

                let duration_us = match job_benchmark_client.add_primary_keys(&mut job_client, job_ddls) {
                    Ok(d) => d,
                    Err(e) => {
                        eprintln!("{}", e);
                        std::process::exit(1);
                    }
                };

                let duration_ms = duration_us as f64 / 1000 as f64;
                println!("job #{} duration (ms)={:.3}", j, duration_ms);
            });

            jobs.push(job);
        }

        // Wait for the end of all jobs
        for job in jobs {
            job.join().expect("the client thread panicked");
        }

        self
    }

    pub fn add_foreign_keys(&mut self, n_jobs: u32) -> &mut Self {
        // Load the corresponding benchmark
        let benchmark = match self.benchmark_type.as_str() {
            "tpcc" => tpcc::TPCC::new(0, 0, 0),
            _ => tpcc::TPCC::new(0, 0, 0),
        };

        // Execute AddForeigneKeys using multiple concurrent jobs
        let mut jobs = Vec::new();

        // We want to get one line per jobs and the ids balanced across the lines.
        let mut ddls = Vec::with_capacity(n_jobs as usize);
        for _ in 0..n_jobs {
            ddls.push(Vec::new());
        }
        let mut n = 1;
        for ddl in benchmark.fkey_ddls.iter() {
            ddls[(n % n_jobs) as usize].push(ddl.sql.clone());
            n += 1;
        }

        println!("Adding foreign keys using {} jobs...", n_jobs);
        for j in 1..=n_jobs {
            // Cloning values before passing them to the thread
            let job_ddls = ddls[(j - 1) as usize].clone();
            let dsn = self.dsn.clone();

            // Load the corresponding benchmark client
            let job_benchmark_client = match self.benchmark_type.as_str() {
                "tpcc" => tpcc::TPCC::new(0, 0, 0),
                _ => tpcc::TPCC::new(0, 0, 0),
            };

            // Starting a new job into its dedicated thread
            let job = thread::spawn(move || {
                // New database connection
                let mut job_client = Executor::connect(dsn);

                let duration_us = match job_benchmark_client.add_foreign_keys(&mut job_client, job_ddls) {
                    Ok(d) => d,
                    Err(e) => {
                        eprintln!("{}", e);
                        std::process::exit(1);
                    }
                };

                let duration_ms = duration_us as f64 / 1000 as f64;
                println!("job #{} duration (ms)={:.3}", j, duration_ms);
            });

            jobs.push(job);
        }

        // Wait for the end of all jobs
        for job in jobs {
            job.join().expect("the client thread panicked");
        }

        self
    }

    pub fn add_indexes(&mut self, n_jobs: u32) -> &mut Self {
        // Load the corresponding benchmark
        let benchmark = match self.benchmark_type.as_str() {
            "tpcc" => tpcc::TPCC::new(0, 0, 0),
            _ => tpcc::TPCC::new(0, 0, 0),
        };

        // Execute AddIndexes using multiple concurrent jobs
        let mut jobs = Vec::new();

        // We want to get one line per jobs and the ids balanced across the lines.
        let mut ddls = Vec::with_capacity(n_jobs as usize);
        for _ in 0..n_jobs {
            ddls.push(Vec::new());
        }
        let mut n = 1;
        for ddl in benchmark.index_ddls.iter() {
            ddls[(n % n_jobs) as usize].push(ddl.sql.clone());
            n += 1;
        }

        println!("Creating additional indexes using {} jobs...", n_jobs);
        for j in 1..=n_jobs {
            // Cloning values before passing them to the thread
            let job_ddls = ddls[(j - 1) as usize].clone();
            let dsn = self.dsn.clone();

            // Load the corresponding benchmark client
            let job_benchmark_client = match self.benchmark_type.as_str() {
                "tpcc" => tpcc::TPCC::new(0, 0, 0),
                _ => tpcc::TPCC::new(0, 0, 0),
            };

            // Starting a new job into its dedicated thread
            let job = thread::spawn(move || {
                // New database connection
                let mut job_client = Executor::connect(dsn);

                let duration_us = match job_benchmark_client.add_foreign_keys(&mut job_client, job_ddls) {
                    Ok(d) => d,
                    Err(e) => {
                        eprintln!("{}", e);
                        std::process::exit(1);
                    }
                };

                let duration_ms = duration_us as f64 / 1000 as f64;
                println!("job #{} duration (ms)={:.3}", j, duration_ms);
            });

            jobs.push(job);
        }

        // Wait for the end of all jobs
        for job in jobs {
            job.join().expect("the client thread panicked");
        }

        self
    }
}

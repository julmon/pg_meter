use std::time::{Duration, Instant};
use std::thread::{JoinHandle, sleep};
use std::thread;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::collections::{HashMap, BTreeMap};

use chrono::Utc;
use crossbeam_channel::{Sender, Receiver, unbounded};
use postgres::{Client, NoTls};
use rand::prelude::*;
use tokio::runtime::Runtime;
use sqlx::PgConnection;
use sqlx::Connection;
use itoa;
use ryu;

mod benchmark;
mod txmessage;
mod tpcc;
mod terminal;

use benchmark::{
    Benchmark,
    BenchmarkStmt,
    BenchmarkTransaction,
    Counter,
    ReadWrite,
};
use txmessage::{TXMessage, TXMessageKind};
use super::args::{RunArgs};

pub struct Executor {
    dsn: String,
    benchmark_type: String,
    counters: HashMap<u16, Counter>,
    rampup_time_ms: u128,
    total_time_ms: u128,
}

impl Executor {
    pub fn new(dsn: String, benchmark_type: String) -> Executor {
        Executor {
            dsn: dsn,
            benchmark_type: benchmark_type,
            counters: HashMap::new(),
            total_time_ms: 0,
            rampup_time_ms: 0
        }
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
        let command = "RUN";

        // Let's find the maximum object id if --max-id is set to 0 (default behavior)
        let max_id :u32 = match args.max_id {
            0 => {
                terminal::start_msg(command, "Fetching maximum ID value");
                // New database connection
                let mut client = Executor::connect(self.dsn.clone());
                let benchmark_client = self.get_benchmark(0, 0, 0);

                let max_id = match benchmark_client.get_default_max_id(&mut client) {
                    Ok(max_id) => max_id,
                    Err(error) => {
                        terminal::err_msg(format!("{}", error).as_str());
                        std::process::exit(1);
                    }
                };
                terminal::done_msg(start.elapsed().as_micros() as f64 / 1000 as f64);

                max_id
            },
            _default => args.max_id.clone(),
        };

        let message = format!("Starting {} client(s) in {} seconds", args.client, args.rampup);
        terminal::start_msg(command, message.as_str());

        // Create the tokio runtime
        let rt = Runtime::new().unwrap();

        rt.block_on(async {
            // Start the clients
            for client_id in 1..=args.client {
                // Test duration calculated by taking in consideration the rampup time and the
                // remaining duration before the end of rampup stage.
                let duration_ms = time_ms + rampup_ms - client_id as u64 * sleep_ms;

                // Sleep accordingly to the rampup time and the number of clients
                sleep(Duration::from_millis(sleep_ms));

                // Start one new client
                let benchmark_client = self.start_rw_client(duration_ms, self.dsn.clone(), args.min_id.clone(), max_id.clone(), tx.clone(), client_id as u32).await;

                benchmark_clients.push(benchmark_client);
            }
            // All clients have been started
            terminal::done_msg(start.elapsed().as_micros() as f64 / 1000 as f64);

            let message2 = format!("Running the workload for {} seconds", args.time);
            terminal::start_msg(command, message2.as_str());
            let start2 = Instant::now();

            // Send end-of-rampup message to the data collector
            tx.send(TXMessage::end_of_rampup()).unwrap();
            self.rampup_time_ms = start.elapsed().as_millis();

            for benchmark_client in benchmark_clients {
                benchmark_client.await.expect("the client thread panicked");
            }

            terminal::done_msg(start2.elapsed().as_micros() as f64 / 1000 as f64);
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
        let duration_ms = Duration::from_millis((self.total_time_ms - self.rampup_time_ms) as u64);
        // Load the corresponding benchmark
        let benchmark = self.get_benchmark(0, 0, 0);

        benchmark.print_results_summary(self.counters.clone(), duration_ms);

        self
    }

    // Start a new read/write benchmark client in its own thread
    async fn start_rw_client(&mut self, duration_ms: u64, dsn: String, min_id: u32, max_id: u32, tx: Sender<TXMessage>, client_id: u32) -> tokio::task::JoinHandle<()>
    {
        // Create a new benchmark object by thread because we don't want to share a such
        // complex structure between all the client threads
        let benchmark_client = self.get_benchmark(0, min_id, max_id);

        tokio::spawn(async move {
            // New database connection
            let mut connection = match PgConnection::connect(&dsn).await {
                Ok(connection) => connection,
                Err(error) => {
                    terminal::err_msg(format!("{}", error).as_str());
                    std::process::exit(1);
                }
            };
            let transactions = benchmark_client.get_transactions_rw();

            /*
            // The connection object performs the actual communication with the database,
            // so spawn it off to run on its own.
            tokio::spawn(async move {
                if let Err(error) = connection.await {
                    terminal::err_msg(format!("{}", error).as_str());
                    std::process::exit(1);
                }
            });
            */

            // Used for tracking client execution time
            let start = Instant::now();
            let mut transaction: &BenchmarkTransaction;
            loop {
                // Pickup a transaction, randomly and weight based.
                transaction = {
                    let mut rng = thread_rng();

                    transactions.choose_weighted(&mut rng, |item| item.weight).unwrap()
                };
                // Execute the database transactions
                match benchmark_client.execute_rw_transaction(&mut connection, &transaction).await {
                    Ok(duration) => {
                        // Send committed message
                        let m = TXMessage::committed(transaction.id, client_id, Utc::now().timestamp(), duration);
                        tx.send(m).unwrap();
                    },
                    Err(error) => {
                        // Send error message
                        let m = TXMessage::error(transaction.id, client_id, Utc::now().timestamp(), format!("{}", error));
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
            let mut log_file = BufWriter::new(log_file);
            // Create the error log file
            let error_file = match File::create(&error_file_path) {
                Ok(f) => f,
                Err(e) => {
                    eprintln!("ERROR: Could not create {}: {}", &error_file_path, e);
                    std::process::exit(1);
                },
            };
            let mut error_file = BufWriter::new(error_file);

            // Initialize the counters
            let mut counters: HashMap<u16, Counter> = HashMap::new();
            let mut client_ids = BTreeMap::new();

            let mut ramping_up :bool = true;
            let mut buffer_i = itoa::Buffer::new();
            let mut buffer_f = ryu::Buffer::new();

            let mut n_client: u32 = 0;
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
                        // Keep a track of the client_ids and increment n_client if this is a new client_id
                        n_client = match client_ids.get(&msg.client_id) {
                            None => {
                                client_ids.insert(msg.client_id, true);
                                n_client += 1;
                                n_client
                            },
                            Some(_) => n_client,
                        };
                        let duration_ms = msg.tx_duration_us as f64 / 1000 as f64;
                        // Counters calculation
                        // Update counters only if the rampup stage is over
                        if !ramping_up {
                            if let Some(c) = counters.get_mut(&msg.tx_id) {
                                (*c).n_commits += 1;
                                (*c).n_total += 1;
                                (*c).total_duration_ms += duration_ms;
                            }
                            else {
                                counters.insert(msg.tx_id, Counter {n_commits: 1, n_total: 1, total_duration_ms: duration_ms});
                            }
                        }

                        // Format and write the line to the log file
                        log_file.write(&buffer_i.format(msg.tx_timestamp).as_bytes()).expect("Failed to write");
                        log_file.write(b" ").expect("Failed to write");
                        log_file.write(&buffer_i.format(n_client).as_bytes()).expect("Failed to write");
                        log_file.write(b" ").expect("Failed to write");
                        log_file.write(&buffer_i.format(msg.tx_id).as_bytes()).expect("Failed to write");
                        log_file.write(b" ").expect("Failed to write");
                        log_file.write(&buffer_f.format(duration_ms).as_bytes()).expect("Failed to write");
                        log_file.write(b"\n").expect("Failed to write");
                    },
                    TXMessageKind::ERROR => {
                        // Keep a track of the client_ids and increment n_client if this is a new client_id
                        n_client = match client_ids.get(&msg.client_id) {
                            None => {
                                client_ids.insert(msg.client_id, true);
                                n_client += 1;
                                n_client
                            },
                            Some(_) => n_client,
                        };
                        // Counters calculation
                        if !ramping_up {
                            if let Some(c) = counters.get_mut(&msg.tx_id) {
                                (*c).n_total += 1;
                            }
                            else {
                                counters.insert(msg.tx_id, Counter {n_commits: 0, n_total: 1, total_duration_ms: 0.0});
                            }
                        }

                        // Format and write the line to the log file
                        error_file.write(&buffer_i.format(msg.tx_timestamp).as_bytes()).expect("Failed to write");
                        error_file.write(b" ").expect("Failed to write");
                        error_file.write(&buffer_i.format(n_client).as_bytes()).expect("Failed to write");
                        error_file.write(b" ").expect("Failed to write");
                        error_file.write(&buffer_i.format(msg.tx_id).as_bytes()).expect("Failed to write");
                        error_file.write(b" ").expect("Failed to write");
                        error_file.write(msg.error.as_bytes()).expect("Failed to write");
                        error_file.write(b"\n").expect("Failed to write");
                    },
                    TXMessageKind::ENDOFRAMPUP => {
                        ramping_up = false;
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
            terminal::err_msg(format!("{}", err).as_str());
            std::process::exit(1);
        })
    }

    // Initialize database schemabenchmark: create tables
    pub fn init_db_schema(&mut self) -> &mut Self {
        let command = "INIT";
        let message = "Executing database DDLs";

        terminal::start_msg(command, message);

        // New database connection
        let mut client = Executor::connect(self.dsn.clone());

        // Load the corresponding benchmark client
        let benchmark_client = self.get_benchmark(0, 0, 0);

        // Initialize the database model/schema
        let duration_us = match benchmark_client.initialize_schema(&mut client) {
            Ok(duration) => duration,
            Err(error) => {
                terminal::err_msg(format!("{}", error).as_str());
                std::process::exit(1);
            }
        };
        let duration_ms = duration_us as f64 / 1000 as f64;

        terminal::done_msg(duration_ms);

        self
    }

    // Generate benchmark data
    pub fn load_data(&mut self, scalefactor: u32, n_jobs: u32) -> &mut Self {
        // New database connection
        let mut client = Executor::connect(self.dsn.clone());

        // Load the corresponding benchmark client
        let benchmark_client = self.get_benchmark(scalefactor, 0, 0);

        let command = "INIT";
        let message = "Pre-loading operations";

        terminal::start_msg(command, message);

        // Execute PreLoadData
        let duration_us = match benchmark_client.pre_load_data(&mut client) {
            Ok(duration) => duration,
            Err(error) => {
                terminal::err_msg(format!("{}", error).as_str());
                std::process::exit(1);
            }
        };
        let duration_ms = duration_us as f64 / 1000 as f64;

        terminal::done_msg(duration_ms);

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

        let message2 = format!("Data loading using {} jobs", n_jobs);
        terminal::start_msg(command, message2.as_str());

        let start = Instant::now();

        for j in 1..=n_jobs {
            // Cloning values before passing them to the thread
            let job_ids = ids[(j - 1) as usize].clone();
            let dsn = self.dsn.clone();

            // Load the corresponding benchmark client
            let job_benchmark_client = self.get_benchmark(scalefactor, 0, 0);

            // Starting a new job into its dedicated thread
            let job = thread::spawn(move || {
                // New database connection
                let mut job_client = Executor::connect(dsn);

                let _duration_us = match job_benchmark_client.load_data(&mut job_client, job_ids) {
                    Ok(duration) => duration,
                    Err(error) => {
                        terminal::err_msg(format!("{}", error).as_str());
                        std::process::exit(1);
                    }
                };
            });

            jobs.push(job);
        }

        // Wait for the end of all jobs
        for job in jobs {
            job.join().expect("the client thread panicked");
        }
        let duration_ms = start.elapsed().as_micros() as f64 / 1000 as f64;
        terminal::done_msg(duration_ms);

        self
    }

    // Execute database multiple statements (DDLs, admin query, etc..) using n_jobs threads.
    pub fn exec_stmts(&mut self, n_jobs: u32, stmts: Vec<BenchmarkStmt>, use_transaction: bool) {
        // We want to get one row per job and the ids balanced across the rowss.
        let mut rows = Vec::with_capacity(n_jobs as usize);
        for _ in 0..n_jobs {
            rows.push(Vec::new());
        }
        let mut n = 1;
        for stmt in stmts.iter() {
            rows[(n % n_jobs) as usize].push(stmt.sql.clone());
            n += 1;
        }
        let mut jobs = Vec::new();

        for j in 1..=n_jobs {
            // Cloning values before passing them to the thread
            let job_stmts = rows[(j - 1) as usize].clone();
            let dsn = self.dsn.clone();

            // Starting a new job into its dedicated thread
            let job = thread::spawn(move || {
                // New database connection
                let mut client = Executor::connect(dsn);

                for stmt in job_stmts.iter() {
                    if use_transaction {
                        let mut transaction = match client.transaction() {
                            Ok(t) => t,
                            Err(error) => {
                                terminal::err_msg(format!("{}", error).as_str());
                                std::process::exit(1);
                            }
                        };
                        match transaction.batch_execute(stmt) {
                            Ok(_) => (),
                            Err(error) => {
                                terminal::err_msg(format!("{}", error).as_str());
                                std::process::exit(1);
                            }
                        }
                        match transaction.commit() {
                            Ok(_) => (),
                            Err(error) => {
                                terminal::err_msg(format!("{}", error).as_str());
                                std::process::exit(1);
                            }
                        }
                    }
                    // No transaction
                    else {
                        match client.batch_execute(stmt) {
                            Ok(_) => (),
                            Err(error) => {
                                terminal::err_msg(format!("{}", error).as_str());
                                std::process::exit(1);
                            }
                        }
                    }
                }
            });

            jobs.push(job);
        }

        // Wait for the end of all jobs
        for job in jobs {
            job.join().expect("the client thread panicked");
        }
    }

    fn get_benchmark(&mut self, scalefactor: u32, min_id: u32, max_id: u32) -> impl Benchmark {
        let benchmark = match self.benchmark_type.as_str() {
            "tpcc" => tpcc::TPCC::new(scalefactor, min_id, max_id),
            _ => tpcc::TPCC::new(scalefactor, min_id, max_id),
        };

        benchmark
    }

    // Execute primary keys DDLs using multiple concurrent jobs
    pub fn add_primary_keys(&mut self, n_jobs: u32) -> &mut Self {
        // Load the corresponding benchmark
        let benchmark = self.get_benchmark(0, 0, 0);
        let start = Instant::now();

        terminal::start_msg("INIT", "Primary keys creation");
        self.exec_stmts(n_jobs, benchmark.get_pkey_ddls(), true);
        terminal::done_msg(start.elapsed().as_micros() as f64 / 1000 as f64);

        self
    }

    // Execute foreign keys DDLs using multiple concurrent jobs
    pub fn add_foreign_keys(&mut self, n_jobs: u32, no_fkey: bool) -> &mut Self {
        // Don't do anything if no_fkey is true
        if no_fkey {
            return self;
        }

        // Load the corresponding benchmark
        let benchmark = self.get_benchmark(0, 0, 0);
        let start = Instant::now();

        terminal::start_msg("INIT", "Foreign keys creation");
        self.exec_stmts(n_jobs, benchmark.get_fkey_ddls(), true);
        terminal::done_msg(start.elapsed().as_micros() as f64 / 1000 as f64);

        self
    }

    // Execute additional indexes DDLs using multiple concurrent jobs
    pub fn add_indexes(&mut self, n_jobs: u32) -> &mut Self {
        // Load the corresponding benchmark
        let benchmark = self.get_benchmark(0, 0, 0);
        let start = Instant::now();

        terminal::start_msg("INIT", "Additional indexes creation");
        self.exec_stmts(n_jobs, benchmark.get_index_ddls(), true);
        terminal::done_msg(start.elapsed().as_micros() as f64 / 1000 as f64);

        self
    }

    // Execute VACUUM statementsusing multiple concurrent jobs
    pub fn vacuum(&mut self, n_jobs: u32) -> &mut Self {
        // Load the corresponding benchmark
        let benchmark = self.get_benchmark(0, 0, 0);
        let start = Instant::now();

        terminal::start_msg("INIT", "Vacuuming tables");
        self.exec_stmts(n_jobs, benchmark.get_vacuum_stmts(), false);
        terminal::done_msg(start.elapsed().as_micros() as f64 / 1000 as f64);

        self
    }

    // Force a checkpoint
    pub fn checkpoint(&mut self) -> &mut Self {
        let start = Instant::now();

        terminal::start_msg("INIT", "Forcing checkpoint");
        let mut client = Executor::connect(self.dsn.clone());

        match client.batch_execute("CHECKPOINT") {
            Ok(_) => (),
            Err(error) => {
                terminal::err_msg(format!("{}", error).as_str());
                std::process::exit(1);
            }
        }

        terminal::done_msg(start.elapsed().as_micros() as f64 / 1000 as f64);

        self
    }
}

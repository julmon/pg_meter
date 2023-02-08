use polars::prelude::*;
use super::benchmark::BenchmarkTransaction;

// Aggregates collected transaction data (response time, throughput) and saves it as CSV files.
pub fn aggregate_tpcc_data(logfile: &str, transactions: &Vec<BenchmarkTransaction>) -> Result<(), Box<dyn std::error::Error>>{
    // Transaction log file parsing
    let df = LazyCsvReader::new(logfile)
        .with_delimiter(b' ')
        .has_header(false)
        .finish()?;

    // Produce aggregated data for each type of transaction
    for transaction in transactions {
        // Calculate transaction throughput: number of transaction over a period of time
        let mut tpm_df = df
            .clone()
            .filter(
                col("column_3").eq(transaction.id as i64)
            )
            // Group by timestamp (in second) and transaction_id
            .groupby([col("column_1"), col("column_3")])
            // We want to calculate the number of transaction per minute
            .agg([col("column_4").count() * lit(60)])
            .select([
                col("column_1").alias("time_s") - col("column_1").min(),
                col("column_4").alias("tpm"),
            ])
            .sort("time_s", Default::default())
            .collect()?;

        // Save data as a CSV file
        let mut file = std::fs::File::create(format!("pgmtr-tpm-{}.csv", transaction.name))?;
        CsvWriter::new(&mut file).finish(&mut tpm_df)?;

        // Calculate the average (mean) response time over a period of time
        let mut response_time_df = df
            .clone()
            .filter(
                col("column_3").eq(transaction.id as i64)
            )
            // Group by timestamp (in second) and transaction_id
            .groupby([col("column_1"), col("column_3")])
            .agg([col("column_4").mean()])
            .select([
                col("column_1").alias("time_s") - col("column_1").min(),
                col("column_4").alias("response_time_ms"),
            ])
            .sort("time_s", Default::default())
            .collect()?;

        // Response times statistics
        let mut stats = df
            .clone()
            .filter(
                col("column_3").eq(transaction.id as i64)
            )
            .select([
                col("column_4").alias("response_time_ms"),
            ])
            .collect()?
            .describe(Some(&[0.95, 0.99]))
            .select(["describe", "response_time_ms"])?;

        // Save data as a CSV file
        let mut file = std::fs::File::create(format!("pgmtr-response-time-{}.csv", transaction.name))?;
        CsvWriter::new(&mut file).finish(&mut response_time_df)?;
        // Save statistics
        let mut file_stats = std::fs::File::create(format!("pgmtr-stats-{}.csv", transaction.name))?;
        CsvWriter::new(&mut file_stats).finish(&mut stats)?;
    }

    // Produce total TPM data, including data from all transaction types
    let mut tpm_all_df = df
        .clone()
        // Group by timestamp (in second) only
        .groupby([col("column_1")])
        .agg([col("column_4").count() * lit(60)])
        .select([
            col("column_1").alias("time_s") - col("column_1").min(),
            col("column_4").alias("tpm"),
        ])
        .sort("time_s", Default::default())
        .collect()?;

    // Save data as a CSV file
    let mut file = std::fs::File::create("pgmtr-tpm-all.csv")?;
    CsvWriter::new(&mut file).finish(&mut tpm_all_df)?;

    Ok(())
}

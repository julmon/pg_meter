mod executor;
mod args;

fn main() {
    // Parse command arguments
    let env = args::PgMtrArgs::new(args::get_os_username(), args::get_pg_password());
    let dsn = args::get_dsn(&env);

    match &*env.action {
        "run" => {
            executor::Executor::new(dsn, env.benchmark_type)
                .run_benchmark(env.run_args)
                .print_results();
        },
        "init" => {
            executor::Executor::new(dsn, env.benchmark_type)
                .init_db_schema()
                .load_data(env.init_args.scalefactor, env.init_args.jobs)
                .add_primary_keys(env.init_args.jobs)
                .add_foreign_keys(env.init_args.jobs)
                .add_indexes(env.init_args.jobs)
                .vacuum(env.init_args.jobs);
        },
        _ => todo!(),
    }
}

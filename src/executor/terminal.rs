use std::io::{self, Write};

use console::style;

pub fn start_msg(command: &str, message: &str) {
    let length :u16 = 80;
    let msg_length :u16 = (command.len() + 1 + message.len()).try_into().unwrap();
    let m = format!("{} {} ", style(command).bold().dim(), message);
    io::stdout().write_all(m.as_bytes()).unwrap();
    for _ in msg_length..=length {
        io::stdout().write_all(".".as_bytes()).unwrap();
    }
    io::stdout().write_all(" ".as_bytes()).unwrap();
    io::stdout().flush().unwrap();
}

pub fn done_msg(duration_ms: f64) {
    let m_done = format!("{} ({:.3} ms)\n", style("done").green(), duration_ms);
    io::stdout().write_all(m_done.as_bytes()).unwrap();
    io::stdout().flush().unwrap();
}

pub fn err_msg(error: &str) {
    let m_err = format!("{}\n", style("failed").red());
    let error = format!("{}\n", style(error).red());
    io::stdout().write_all(m_err.as_bytes()).unwrap();
    io::stdout().flush().unwrap();
    io::stderr().write_all(error.as_bytes()).unwrap();
    io::stderr().flush().unwrap();
}

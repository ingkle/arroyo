use std::io;
use std::io::Write;
use std::process::{Command, ExitStatus};
use tracing::debug;

pub trait CommandExt {
    fn inherit(&mut self);

    // wait and check
    fn wait_and_check(&mut self);

    // just wait
    fn wait(&mut self);

    fn print(&mut self) -> &mut Self;

    fn rust_log(&mut self, log_option: Option<&str>) -> &mut Self;
}

impl CommandExt for Command {
    /// execute and ensure command has been executed ok
    fn inherit(&mut self) {
        use std::process::Stdio;

        self.print();

        let output = self
            .stdout(Stdio::inherit())
            .output()
            .expect("execution failed");

        if !output.status.success() {
            io::stderr().write_all(&output.stderr).unwrap();
        }

        output.status.check();
    }

    /// execute and ensure command has been executed ok
    fn wait_and_check(&mut self) {
        self.print();

        let output = self.output().expect("execution failed");

        io::stdout().write_all(&output.stdout).unwrap();
        io::stderr().write_all(&output.stderr).unwrap();

        output.status.check();
    }

    /// execute and wait, ignore error
    fn wait(&mut self) {
        self.print();
        let output = self.output().expect("execution failed");

        io::stdout().write_all(&output.stdout).unwrap();
        io::stderr().write_all(&output.stderr).unwrap();
    }

    fn print(&mut self) -> &mut Self {
        use std::env;

        debug!("> {}", format!("{:?}", self).replace("\"", ""));
        if env::var_os("FLV_CMD").is_some() {
            println!(">> {}", format!("{:?}", self).replace("\"", ""));
        }

        self
    }

    fn rust_log(&mut self, log_option: Option<&str>) -> &mut Self {
        if let Some(log) = log_option {
            println!("setting rust log: {}", log);
            self.env("RUST_LOG", log);
        }

        self
    }
}

trait StatusExt {
    fn check(&self);
}

impl StatusExt for ExitStatus {
    fn check(&self) {
        if !self.success() {
            match self.code() {
                Some(code) => println!("Exited with status code: {}", code),
                None => println!("Process terminated by signal"),
            }
            unreachable!()
        }
    }
}

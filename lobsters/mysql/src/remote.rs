extern crate chrono;
extern crate clap;
extern crate failure;
extern crate rusoto_core;
extern crate rusoto_sts;
extern crate tsunami;

use chrono::prelude::*;
use clap::{App, Arg};
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::io::BufReader;
use std::net::SocketAddr;
use std::path::Path;
use std::{fmt, thread, time};
use tsunami::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Backend {
    Soup,
    RockySoup,
}

impl fmt::Display for Backend {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match *self {
            Backend::Soup => write!(f, "soup"),
            Backend::RockySoup => write!(f, "rocksdb_soup"),
        }
    }
}

fn main() {
    let args = App::new("trawler-mysql remote orchestrator")
        .about("Run the MySQL trawler benchmark on remote servers")
        .arg(
            Arg::with_name("SCALE")
                .help("Run the given scale(s).")
                .multiple(true),
        )
        .arg(
            Arg::with_name("key-path")
                .long("key-path")
                .required(true)
                .takes_value(true)
                .help("Path to the private SSH key"),
        )
        .arg(
            Arg::with_name("username")
                .long("username")
                .required(true)
                .takes_value(true)
                .help("Username to SSH with"),
        )
        .arg(
            Arg::with_name("server-address")
                .long("server-address")
                .required(true)
                .takes_value(true)
                .help("Server address"),
        )
        .arg(
            Arg::with_name("trawler-address")
                .long("trawler-address")
                .required(true)
                .takes_value(true)
                .help("Trawler address"),
        )
        .arg(
            Arg::with_name("server-ip")
                .long("server-ip")
                .required(true)
                .takes_value(true)
                .help("Server IP address"),
        )
        .get_matches();

    let username = args.value_of("username").unwrap();
    let server_ip = args.value_of("server-ip").unwrap();
    let key_path = Path::new(args.value_of("key-path").unwrap());
    let server_addr: SocketAddr = args.value_of("server-address").unwrap().parse().unwrap();
    let trawler_addr: SocketAddr = args.value_of("trawler-address").unwrap().parse().unwrap();

    let scales: Box<Iterator<Item = usize>> = args.values_of("SCALE")
        .map(|it| Box::new(it.map(|s| s.parse().unwrap())) as Box<_>)
        .unwrap_or(Box::new(
            [
                100, 200, 400, 800, 1000usize, 1250, 1500, 2000, 2500, 3000, 3500, 4000,
            ].into_iter()
                .map(|&s| s),
        ) as Box<_>);

    let mut load = if args.is_present("SCALE") {
        OpenOptions::new()
            .write(true)
            .truncate(false)
            .append(true)
            .create(true)
            .open("load.log")
            .unwrap()
    } else {
        let mut f = File::create("load.log").unwrap();
        f.write_all(b"#reqscale backend sload1 sload5 cload1 cload5\n")
            .unwrap();
        f
    };

    // Local shim IP:
    let mysql_url = "mysql://lobsters@127.0.0.1:3307/lobsters";
    let server = Session::connect(username, server_addr, key_path).unwrap();
    let trawler = Session::connect(username, trawler_addr, key_path).unwrap();
    let backends = [Backend::Soup, Backend::RockySoup];

    // allow reuse of time-wait ports
    trawler
        .cmd("bash -c 'echo 1 | sudo tee /proc/sys/net/ipv4/tcp_tw_reuse'")
        .unwrap();

    for scale in scales {
        for backend in &backends {
            eprintln!("==> benchmark {} w/ {}x load", backend, scale);
            clear_state(&server, &trawler);
            eprintln!(" -> starting server and shim");

            // start server again
            let durability = match backend {
                Backend::Soup => "memory",
                Backend::RockySoup => "ephemeral",
            };

            server
                .cmd(&format!(
                    "cd lobsters && bash -c 'nohup \
                     env RUST_BACKTRACE=1 \
                     ../distributary/target/release/souplet \
                     --deployment trawler \
                     --durability {} \
                     --no-reuse \
                     --persistence-threads 8 \
                     --address {} \
                     --readers 48 -w 6 \
                     --shards 0 \
                     &> souplet.log &'",
                    durability, server_ip,
                ))
                .unwrap();

            eprintln!(" -> started the server");

            // start the shim (which will block until soup is available)
            trawler
                .cmd(&format!(
                    "cd lobsters && bash -c 'nohup \
                     env RUST_BACKTRACE=1 \
                     ../distributary-mysql/target/release/distributary-mysql \
                     --deployment trawler \
                     --no-sanitize --no-static-responses \
                     -z {}:2181 \
                     -p 3307 \
                     &> shim.log &'",
                    server_ip,
                ))
                .unwrap();

            eprintln!(" -> started the shim");

            // give soup a chance to start
            thread::sleep(time::Duration::from_secs(5));

            // run priming
            eprintln!(" -> priming at {}", Local::now().time().format("%H:%M:%S"));
            trawler
                .cmd(&format!(
                    "cd lobsters && env RUST_BACKTRACE=1 \
                     ../soup-benchmarks/lobsters/mysql/target/release/trawler-mysql \
                     --warmup 0 \
                     --runtime 0 \
                     --issuers 24 \
                     --prime \
                     \"{}\"",
                    mysql_url
                ))
                .map(|out| {
                    let out = out.trim_right();
                    if !out.is_empty() {
                        eprintln!(" -> priming finished...\n{}", out);
                    }
                })
                .unwrap();

            eprintln!(" -> warming at {}", Local::now().time().format("%H:%M:%S"));
            trawler
                .cmd(&format!(
                    "cd lobsters && env RUST_BACKTRACE=1 \
                     ../soup-benchmarks/lobsters/mysql/target/release/trawler-mysql \
                     --reqscale 3000 \
                     --warmup 120 \
                     --runtime 0 \
                     --issuers 24 \
                     \"{}\"",
                    mysql_url,
                ))
                .map(|out| {
                    let out = out.trim_right();
                    if !out.is_empty() {
                        eprintln!(" -> warming finished...\n{}", out);
                    }
                })
                .unwrap();

            eprintln!(" -> started at {}", Local::now().time().format("%H:%M:%S"));

            let prefix = format!("lobsters-{}-{}", backend, scale);
            let mut output = File::create(format!("{}.log", prefix)).unwrap();
            trawler
                .cmd_raw(&format!(
                    "cd lobsters && env RUST_BACKTRACE=1 \
                     ../soup-benchmarks/lobsters/mysql/target/release/trawler-mysql \
                     --reqscale {} \
                     --warmup 20 \
                     --runtime 30 \
                     --issuers 24 \
                     \"{}\"",
                    scale, mysql_url
                ))
                .and_then(|out| Ok(output.write_all(&out[..]).unwrap()))
                .unwrap();

            drop(output);
            eprintln!(" -> finished at {}", Local::now().time().format("%H:%M:%S"));

            // gather server load
            let sload = server.cmd("awk '{print $1\" \"$2}' /proc/loadavg").unwrap();
            let sload = sload.trim_right();

            // gather client load
            let cload = trawler
                .cmd("awk '{print $1\" \"$2}' /proc/loadavg")
                .unwrap();
            let cload = cload.trim_right();

            load.write_all(format!("{} {} ", scale, backend).as_bytes())
                .unwrap();
            load.write_all(sload.as_bytes()).unwrap();
            load.write_all(b" ").unwrap();
            load.write_all(cload.as_bytes()).unwrap();
            load.write_all(b"\n").unwrap();

            // let mut hist = File::create(format!("{}.hist", prefix))?;
            // trawler
            //     .ssh
            //     .as_mut()
            //     .unwrap()
            //     .cmd_raw(&format!("cat lobsters-{}-{}.hist", backend, scale))
            //     .and_then(|out| Ok(hist.write_all(&out[..]).map(|_| ())?))?;

            // stop old server
            server
                .cmd("bash -c 'pkill -f souplet 2>&1'")
                .map(|out| {
                    let out = out.trim_right();
                    if !out.is_empty() {
                        eprintln!(" -> stopped soup...\n{}", out);
                    }
                })
                .unwrap();
            trawler
                .cmd("bash -c 'pkill -f distributary-mysql 2>&1'")
                .map(|out| {
                    let out = out.trim_right();
                    if !out.is_empty() {
                        eprintln!(" -> stopped shim...\n{}", out);
                    }
                })
                .unwrap();

            // give it some time
            thread::sleep(time::Duration::from_secs(2));

            // stop iterating through scales for this backend if it's not keeping up
            let sload: f64 = sload
                .split_whitespace()
                .next()
                .and_then(|l| l.parse().ok())
                .unwrap_or(0.0);
            let cload: f64 = cload
                .split_whitespace()
                .next()
                .and_then(|l| l.parse().ok())
                .unwrap_or(0.0);

            eprintln!(" -> backend load: s: {}/16, c: {}/48", sload, cload);

            // also parse achived ops/s to check that we're *really* keeping up
            let log = File::open(format!("{}.log", prefix)).unwrap();
            let log = BufReader::new(log);
            let mut target = None;
            let mut actual = None;
            for line in log.lines() {
                let line = line.unwrap();
                if line.starts_with("# target ops/s") {
                    target = Some(line.rsplitn(2, ' ').next().unwrap().parse::<f64>().unwrap());
                } else if line.starts_with("# achieved ops/s") {
                    actual = Some(line.rsplitn(2, ' ').next().unwrap().parse::<f64>().unwrap());
                }
                match (target, actual) {
                    (Some(target), Some(actual)) => {
                        eprintln!(" -> achieved {} ops/s (target: {})", actual, target);
                        if actual < target * 3.0 / 4.0 {
                            eprintln!(" -> backend is really not keeping up");
                        }
                        break;
                    }
                    _ => {}
                }
            }
        }
    }
}

fn clear_state(server: &Session, trawler: &Session) {
    server
        .cmd(
            "bash -c 'rm -rf lobsters/* && \
             rm -rf /flash/soup/lobsters/* && \
             pkill -9 -f souplet 2>&1'",
        )
        .map(|out| {
            let out = out.trim_right();
            if !out.is_empty() {
                eprintln!(" -> force stopped soup...\n{}", out);
            }
        })
        .unwrap();

    trawler
        .cmd("bash -c 'pkill -9 -f distributary-mysql 2>&1'")
        .map(|out| {
            let out = out.trim_right();
            if !out.is_empty() {
                eprintln!(" -> force stopped shim...\n{}", out);
            }
        })
        .unwrap();

    eprintln!(" -> killed existing servers");

    thread::sleep(time::Duration::from_secs(2));
    server
        .cmd(
            "distributary/target/release/zk-util \
             --clean --deployment trawler",
        )
        .map(|out| {
            let out = out.trim_right();
            if !out.is_empty() {
                eprintln!(" -> wiped soup state...\n{}", out);
            }
        })
        .unwrap();

    // Don't hit Soup listening timeout think
    thread::sleep(time::Duration::from_secs(10));
}

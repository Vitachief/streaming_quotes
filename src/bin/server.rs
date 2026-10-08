use std::collections::HashSet;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream, UdpSocket, SocketAddr};
use std::thread;
use std::time::{Duration, Instant};
use rand::Rng;
use parking_lot::Mutex;

use streaming_quotes::quote::StockQuote;
use streaming_quotes::protocol::{parse_command, format_ok, format_err, Command};
use streaming_quotes::tickers::load_tickers;

struct Subscriber {
    tx: std::sync::mpsc::Sender<StockQuote>,
    udp_addr: SocketAddr,
    tickers: HashSet<String>,
    last_ping: Instant,
}

const PING_TIMEOUT_SEC: u64 = 10;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let tcp_port = args.get(1).map(|s| s.as_str()).unwrap_or("7878");
    let ping_port = args.get(2).map(|s| s.as_str()).unwrap_or("7879");
    let tickers_file = args.get(3).map(|s| s.as_str()).unwrap_or("assets/tickers.txt");

    let valid_tickers = std::sync::Arc::new(load_tickers(tickers_file).expect("Failed to load tickers"));
    let subscribers: std::sync::Arc<Mutex<Vec<Subscriber>>> = std::sync::Arc::new(Mutex::new(Vec::new()));

    // 1. Поток приёма PING
    let ping_subs = std::sync::Arc::clone(&subscribers);
    let ping_addr = format!("0.0.0.0:{}", ping_port);
    let ping_socket = UdpSocket::bind(&ping_addr).expect("Failed to bind ping socket");
    println!("[Server] PING listener started on {}", ping_addr);

    thread::spawn(move || {
        let mut buf = [0; 1024];
        loop {
            if let Ok((len, addr)) = ping_socket.recv_from(&mut buf) {
                let msg = String::from_utf8_lossy(&buf[..len]);
                if msg.trim() == "PING" {
                    let mut subs = ping_subs.lock();
                    for sub in subs.iter_mut() {
                        if sub.udp_addr == addr {
                            sub.last_ping = Instant::now();
                        }
                    }
                }
            }
        }
    });

    // 2. Поток генератора котировок
    let gen_subs = std::sync::Arc::clone(&subscribers);
    let gen_valid_tickers = std::sync::Arc::clone(&valid_tickers);
    thread::spawn(move || {
        let mut rng = rand::rng();
        let tickers_vec: Vec<String> = gen_valid_tickers.iter().cloned().collect();

        loop {
            thread::sleep(Duration::from_millis(500));
            
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;

            let ticker = tickers_vec[rng.random_range(0..tickers_vec.len())].clone();
            let volume = match ticker.as_str() {
                "AAPL" | "MSFT" | "TSLA" => rng.random_range(1000..6000),
                _ => rng.random_range(100..1100),
            };
            let price = rng.random_range(10.0..500.0);

            let quote = StockQuote {
                ticker,
                price,
                volume,
                timestamp_ms: now_ms,
            };

            let mut subs = gen_subs.lock();
            
            subs.retain(|sub| sub.last_ping.elapsed() < Duration::from_secs(PING_TIMEOUT_SEC));

            for sub in subs.iter() {
                if sub.tickers.contains(&quote.ticker) {
                    let _ = sub.tx.send(quote.clone());
                }
            }
        }
    });

    // 3. TCP Listener
    let tcp_addr = format!("0.0.0.0:{}", tcp_port);
    let listener = TcpListener::bind(&tcp_addr).expect("Failed to bind TCP");
    println!("[Server] TCP listener started on {}", tcp_addr);

    for stream in listener.incoming() {
        let stream = stream.expect("Failed to accept connection");
        let subs = std::sync::Arc::clone(&subscribers);
        let valid_tickers = std::sync::Arc::clone(&valid_tickers);

        thread::spawn(move || {
            handle_client(stream, subs, valid_tickers);
        });
    }
}

fn handle_client(
    stream: TcpStream,
    subscribers: std::sync::Arc<Mutex<Vec<Subscriber>>>,
    valid_tickers: std::sync::Arc<HashSet<String>>,
) {
    let mut reader = BufReader::new(stream);
    let mut cmd_str = String::new();
    
    if reader.read_line(&mut cmd_str).is_err() {
        return;
    }
    
    let mut stream = reader.into_inner();

    match parse_command(&cmd_str) {
        Ok(Command::Stream { udp_addr, tickers }) => {
            let mut filter = HashSet::new();
            for t in &tickers {
                if valid_tickers.contains(t) {
                    filter.insert(t.clone());
                }
            }

            if filter.is_empty() {
                let _ = stream.write_all(format_err("No valid tickers provided").as_bytes());
                return;
            }

            let _ = stream.write_all(format_ok().as_bytes());

            let (tx, rx) = std::sync::mpsc::channel::<StockQuote>();

            {
                let mut subs = subscribers.lock();
                subs.push(Subscriber {
                    tx,
                    udp_addr,
                    tickers: filter,
                    last_ping: Instant::now(),
                });
            }

            let udp_socket = UdpSocket::bind("0.0.0.0:0").expect("Failed to bind ephemeral UDP socket");
            
            thread::spawn(move || {
                while let Ok(quote) = rx.recv() {
                    let line = format!("{}\n", quote.to_wire_line());
                    if let Err(e) = udp_socket.send_to(line.as_bytes(), udp_addr) {
                        eprintln!("[Server] Failed to send quote to {}: {}", udp_addr, e);
                    }
                }
            });
        }
        Err(e) => {
            let _ = stream.write_all(format_err(&e).as_bytes());
        }
    }
}
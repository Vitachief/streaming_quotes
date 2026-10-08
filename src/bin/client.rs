use std::io::{Read, Write};
use std::net::{TcpStream, UdpSocket, SocketAddr};
use std::time::{Duration, Instant};
use streaming_quotes::quote::StockQuote;
use streaming_quotes::tickers::load_tickers;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 5 {
        eprintln!("Usage: {} <server_tcp_addr> <server_ping_addr> <local_udp_port> <tickers_file>", args[0]);
        eprintln!("Example: cargo run --bin client -- 127.0.0.1:7878 127.0.0.1:7879 9000 assets/tickers.txt");
        return;
    }

    let server_tcp_addr = &args[1];
    let server_ping_addr: SocketAddr = args[2].parse().expect("Invalid server ping address");
    let local_udp_port = &args[3];
    let tickers_file = &args[4];

    let tickers = load_tickers(tickers_file).expect("Failed to load tickers");
    let tickers_str: Vec<String> = tickers.into_iter().collect();
    let tickers_cmd = tickers_str.join(",");

    let local_addr_str = format!("127.0.0.1:{}", local_udp_port);
    let udp_socket = UdpSocket::bind(&local_addr_str).expect("Failed to bind local UDP");
    let local_addr = udp_socket.local_addr().expect("Failed to get local addr");

    let mut stream = TcpStream::connect(server_tcp_addr).expect("Failed to connect TCP");

    let cmd = format!("STREAM {} {}\n", local_addr, tickers_cmd);
    stream.write_all(cmd.as_bytes()).expect("Failed to send STREAM command");

    let mut buf = [0; 1024];
    let n = stream.read(&mut buf).expect("Failed to read response");
    let response = String::from_utf8_lossy(&buf[..n]);

    if response.trim() != "OK" {
        eprintln!("[Client] Server rejected subscription: {}", response.trim());
        return;
    }

    println!("[Client] Subscribed successfully. Receiving quotes on {}...", local_addr);

    udp_socket.set_read_timeout(Some(Duration::from_secs(2))).expect("Failed to set read timeout");

    let mut quote_buf = [0; 2048];
    let mut last_ping = Instant::now();

    loop {
        if last_ping.elapsed() >= Duration::from_secs(2) {
            if let Err(e) = udp_socket.send_to(b"PING\n", server_ping_addr) {
                eprintln!("[Client] Failed to send PING: {}", e);
            }
            last_ping = Instant::now();
        }

        match udp_socket.recv_from(&mut quote_buf) {
            Ok((len, _addr)) => {
                let line = String::from_utf8_lossy(&quote_buf[..len]);
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }

                match StockQuote::from_wire_line(line) {
                    Ok(quote) => {
                        println!("{:<8} | ${:<8.2} | Vol: {:<6} | TS: {}", 
                            quote.ticker, quote.price, quote.volume, quote.timestamp_ms);
                    }
                    Err(e) => {
                        eprintln!("[Client] Parse error: {} on line: {}", e, line);
                    }
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock || e.kind() == std::io::ErrorKind::TimedOut => {
                continue;
            }
            Err(e) => {
                eprintln!("[Client] Fatal UDP receive error: {}", e);
                break;
            }
        }
    }
}
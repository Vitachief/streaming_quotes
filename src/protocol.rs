use std::net::SocketAddr;

#[derive(Debug, PartialEq)]
pub enum Command {
    Stream {
        udp_addr: SocketAddr,
        tickers: Vec<String>,
    },
}

pub fn parse_command(line: &str) -> Result<Command, String> {
    let line = line.trim();
    if !line.starts_with("STREAM ") {
        return Err("Unknown command. Expected STREAM".to_string());
    }

    let payload = line[7..].trim();
    let parts: Vec<&str> = payload.split_whitespace().collect();

    if parts.len() < 2 {
        return Err("Invalid STREAM format. Expected: STREAM <udp_addr> <tickers>".to_string());
    }

    let udp_addr: SocketAddr = parts[0]
        .parse()
        .map_err(|_| "Invalid UDP address format".to_string())?;

    let tickers: Vec<String> = parts[1]
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    if tickers.is_empty() {
        return Err("Empty tickers list".to_string());
    }

    Ok(Command::Stream { udp_addr, tickers })
}

pub fn format_ok() -> String {
    "OK\n".to_string()
}

pub fn format_err(msg: &str) -> String {
    format!("ERR {}\n", msg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_stream() {
        let cmd = parse_command("STREAM 127.0.0.1:9000 AAPL,TSLA\n").unwrap();
        let Command::Stream { udp_addr, tickers } = cmd;
        assert_eq!(udp_addr.to_string(), "127.0.0.1:9000");
        assert_eq!(tickers, vec!["AAPL", "TSLA"]);
    }

    #[test]
    fn test_invalid_command() {
        assert!(parse_command("SUBSCRIBE 127.0.0.1:9000 AAPL").is_err());
        assert!(parse_command("STREAM invalid_addr AAPL").is_err());
        assert!(parse_command("STREAM 127.0.0.1:9000").is_err());
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct StockQuote {
    pub ticker: String,
    pub price: f64,
    pub volume: u32,
    pub timestamp_ms: u64,
}

impl StockQuote {
    /// Строка без завершающего `\n`.
    pub fn to_wire_line(&self) -> String {
        format!("{}|{}|{}|{}", self.ticker, self.price, self.volume, self.timestamp_ms)
    }

    /// Разбор полезной нагрузки без `\n`.
    pub fn from_wire_line(line: &str) -> Result<Self, String> {
        let parts: Vec<&str> = line.split('|').collect();
        if parts.len() != 4 {
            return Err(format!("Invalid number of fields: expected 4, got {}", parts.len()));
        }
        
        Ok(Self {
            ticker: parts[0].to_string(),
            price: parts[1].parse().map_err(|_| "Invalid price format")?,
            volume: parts[2].parse().map_err(|_| "Invalid volume format")?,
            timestamp_ms: parts[3].parse().map_err(|_| "Invalid timestamp format")?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip() {
        let quote = StockQuote {
            ticker: "AAPL".to_string(),
            price: 150.5,
            volume: 1000,
            timestamp_ms: 1690000000000,
        };
        let wire = quote.to_wire_line();
        let parsed = StockQuote::from_wire_line(&wire).unwrap();
        assert_eq!(quote, parsed);
    }

    #[test]
    fn test_invalid_parse() {
        assert!(StockQuote::from_wire_line("AAPL|150.5|1000").is_err()); // Мало полей
        assert!(StockQuote::from_wire_line("AAPL|abc|1000|123").is_err()); // Неверный тип
    }
}
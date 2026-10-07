use std::collections::HashSet;
use std::fs;

pub fn load_tickers(path: &str) -> Result<HashSet<String>, String> {
    let content = fs::read_to_string(path)
        .map_err(|e| format!("Failed to read file '{}': {}", path, e))?;
        
    let mut tickers = HashSet::new();
    for line in content.lines() {
        let trimmed = line.trim();
        if !trimmed.is_empty() {
            tickers.insert(trimmed.to_string());
        }
    }
    
    if tickers.is_empty() {
        return Err("Ticker file is empty or contains no valid entries".to_string());
    }
    
    Ok(tickers)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_load_tickers() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "AAPL\n  MSFT  \n\nTSLA").unwrap();
        
        let tickers = load_tickers(file.path().to_str().unwrap()).unwrap();
        assert_eq!(tickers.len(), 3);
        assert!(tickers.contains("AAPL"));
        assert!(tickers.contains("MSFT"));
        assert!(tickers.contains("TSLA"));
    }
}
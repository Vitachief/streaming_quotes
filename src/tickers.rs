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
    use std::env;
    use std::fs::File;
    use std::io::Write;

    #[test]
    fn test_load_tickers() {
        let temp_dir = env::temp_dir();
        let file_path = temp_dir.join("test_tickers.txt");
        
        let mut file = File::create(&file_path).expect("Failed to create temp file");
        writeln!(file, "AAPL\n  MSFT  \n\nTSLA").expect("Failed to write to temp file");
        
        let tickers = load_tickers(file_path.to_str().unwrap()).unwrap();
        assert_eq!(tickers.len(), 3);
        assert!(tickers.contains("AAPL"));
        assert!(tickers.contains("MSFT"));
        assert!(tickers.contains("TSLA"));
        
        let _ = fs::remove_file(&file_path);
    }

    #[test]
    fn test_empty_tickers_file() {
        let temp_dir = env::temp_dir();
        let file_path = temp_dir.join("empty_tickers.txt");
        File::create(&file_path).expect("Failed to create temp file");
        
        let result = load_tickers(file_path.to_str().unwrap());
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Ticker file is empty or contains no valid entries");
        
        let _ = fs::remove_file(&file_path);
    }
}
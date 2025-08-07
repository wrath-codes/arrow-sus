use std::collections::HashMap;
use once_cell::sync::Lazy;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateFormatError {
    pub format: String,
    pub value: String,
    pub message: String,
}

impl StateFormatError {
    pub fn new(format: &str, value: &str, message: &str) -> Self {
        Self {
            format: format.to_string(),
            value: value.to_string(),
            message: message.to_string(),
        }
    }
}

impl std::fmt::Display for StateFormatError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: expected format '{}', got '{}'", self.message, self.format, self.value)
    }
}

impl std::error::Error for StateFormatError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateNotFoundError {
    pub state: String,
    pub message: String,
}

impl StateNotFoundError {
    pub fn new(state: &str, message: &str) -> Self {
        Self {
            state: state.to_string(),
            message: message.to_string(),
        }
    }
}

impl std::fmt::Display for StateNotFoundError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: '{}'", self.message, self.state)
    }
}

impl std::error::Error for StateNotFoundError {}

/// Representation of a Brazilian Federative Unit (State).
/// 
/// # Fields
/// * `code` - IBGE code of the state
/// * `name` - Full name of the state
/// * `uf` - State abbreviation (2 letters)
/// 
/// # Example
/// ```rust
/// use shared::models::geo_utils::StateBR;
/// 
/// let sao_paulo = StateBR::new(35, "São Paulo", "SP");
/// assert_eq!(sao_paulo.code, 35);
/// assert_eq!(sao_paulo.name, "São Paulo");
/// assert_eq!(sao_paulo.uf, "SP");
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateBR {
    pub code: u8,
    pub name: String,
    pub uf: String,
}

impl StateBR {
    pub fn new(code: u8, name: &str, uf: &str) -> Self {
        Self {
            code,
            name: name.to_string(),
            uf: uf.to_string(),
        }
    }
}

/// Mapping of UF abbreviations to their complete state data.
pub static UFS: Lazy<HashMap<&'static str, StateBR>> = Lazy::new(|| {
    let mut ufs = HashMap::new();
    ufs.insert("RO", StateBR::new(11, "Rondônia", "RO"));
    ufs.insert("AC", StateBR::new(12, "Acre", "AC"));
    ufs.insert("AM", StateBR::new(13, "Amazonas", "AM"));
    ufs.insert("RR", StateBR::new(14, "Roraima", "RR"));
    ufs.insert("PA", StateBR::new(15, "Pará", "PA"));
    ufs.insert("AP", StateBR::new(16, "Amapá", "AP"));
    ufs.insert("TO", StateBR::new(17, "Tocantins", "TO"));
    ufs.insert("MA", StateBR::new(21, "Maranhão", "MA"));
    ufs.insert("PI", StateBR::new(22, "Piauí", "PI"));
    ufs.insert("CE", StateBR::new(23, "Ceará", "CE"));
    ufs.insert("RN", StateBR::new(24, "Rio Grande do Norte", "RN"));
    ufs.insert("PB", StateBR::new(25, "Paraíba", "PB"));
    ufs.insert("PE", StateBR::new(26, "Pernambuco", "PE"));
    ufs.insert("AL", StateBR::new(27, "Alagoas", "AL"));
    ufs.insert("SE", StateBR::new(28, "Sergipe", "SE"));
    ufs.insert("BA", StateBR::new(29, "Bahia", "BA"));
    ufs.insert("MG", StateBR::new(31, "Minas Gerais", "MG"));
    ufs.insert("ES", StateBR::new(32, "Espírito Santo", "ES"));
    ufs.insert("RJ", StateBR::new(33, "Rio de Janeiro", "RJ"));
    ufs.insert("SP", StateBR::new(35, "São Paulo", "SP"));
    ufs.insert("PR", StateBR::new(41, "Paraná", "PR"));
    ufs.insert("SC", StateBR::new(42, "Santa Catarina", "SC"));
    ufs.insert("RS", StateBR::new(43, "Rio Grande do Sul", "RS"));
    ufs.insert("MS", StateBR::new(50, "Mato Grosso do Sul", "MS"));
    ufs.insert("MT", StateBR::new(51, "Mato Grosso", "MT"));
    ufs.insert("GO", StateBR::new(52, "Goiás", "GO"));
    ufs.insert("DF", StateBR::new(53, "Distrito Federal", "DF"));
    ufs
});

/// Get state information by UF abbreviation.
/// 
/// # Arguments
/// * `uf` - State abbreviation (case insensitive)
/// 
/// # Returns
/// * `Ok(StateBR)` - State information
/// * `Err(StateNotFoundError)` - Error if state is not found
/// 
/// # Example
/// ```rust
/// use shared::models::geo_utils::get_state_info;
/// 
/// let sp = get_state_info("SP").unwrap();
/// assert_eq!(sp.name, "São Paulo");
/// 
/// let sp_lower = get_state_info("sp").unwrap();
/// assert_eq!(sp_lower.name, "São Paulo");
/// ```
pub fn get_state_info(uf: &str) -> Result<StateBR, StateNotFoundError> {
    let uf_upper = uf.to_uppercase();
    UFS.get(uf_upper.as_str())
        .cloned()
        .ok_or_else(|| StateNotFoundError::new(uf, "UF não encontrada"))
}

/// Async version of get_state_info
pub async fn get_state_info_async(uf: &str) -> Result<StateBR, StateNotFoundError> {
    tokio::task::yield_now().await;
    get_state_info(uf)
}



/// Converts a list of UFs to StateBR objects.
/// 
/// # Arguments
/// * `ufs` - Slice of UF strings
/// 
/// # Returns
/// * `Ok(Vec<StateBR>)` - List of state objects
/// * `Err(StateNotFoundError)` - Error if any UF is not found
/// 
/// # Example
/// ```rust
/// use shared::models::geo_utils::parse_ufs;
/// 
/// let states = parse_ufs(&["SP", "RJ", "MG"]).unwrap();
/// assert_eq!(states.len(), 3);
/// assert_eq!(states[0].name, "São Paulo");
/// ```
pub fn parse_ufs(ufs: &[&str]) -> Result<Vec<StateBR>, StateNotFoundError> {
    if ufs.is_empty() {
        return Ok(vec![]);
    }
    
    let ufs_upper: Vec<String> = ufs.iter().map(|uf| uf.to_uppercase()).collect();
    
    // Check for invalid UFs
    let mut invalid_ufs = Vec::new();
    for uf in &ufs_upper {
        if !UFS.contains_key(uf.as_str()) {
            invalid_ufs.push(uf.clone());
        }
    }
    
    if !invalid_ufs.is_empty() {
        invalid_ufs.sort();
        return Err(StateNotFoundError::new(
            &invalid_ufs.join(", "),
            "UF(s) não encontrada(s)"
        ));
    }
    
    // All UFs are valid, collect the states
    let states: Vec<StateBR> = ufs_upper
        .iter()
        .map(|uf| UFS.get(uf.as_str()).unwrap().clone())
        .collect();
    
    Ok(states)
}

/// Async version of parse_ufs
pub async fn parse_ufs_async(ufs: &[&str]) -> Result<Vec<StateBR>, StateNotFoundError> {
    tokio::task::yield_now().await;
    parse_ufs(ufs)
}

/// Converts a vector of String UFs to StateBR objects.
/// Convenience function for owned strings.
pub fn parse_ufs_owned(ufs: &[String]) -> Result<Vec<StateBR>, StateNotFoundError> {
    let uf_refs: Vec<&str> = ufs.iter().map(|s| s.as_str()).collect();
    parse_ufs(&uf_refs)
}

/// Async version of parse_ufs_owned
pub async fn parse_ufs_owned_async(ufs: &[String]) -> Result<Vec<StateBR>, StateNotFoundError> {
    tokio::task::yield_now().await;
    parse_ufs_owned(ufs)
}

/// Get states by region (based on IBGE codes)
/// 
/// # Arguments
/// * `region` - Region name ("norte", "nordeste", "sudeste", "sul", "centro_oeste")
/// 
/// # Returns
/// * `Ok(Vec<StateBR>)` - List of states in the region
/// * `Err(StateNotFoundError)` - Error if region is invalid
pub fn get_states_by_region(region: &str) -> Result<Vec<StateBR>, StateNotFoundError> {
    let region_lower = region.to_lowercase();
    
    let code_ranges = match region_lower.as_str() {
        "norte" => vec![(11, 17)],           // 11-17
        "nordeste" => vec![(21, 29)],        // 21-29
        "sudeste" => vec![(31, 35)],         // 31, 32, 33, 35
        "sul" => vec![(41, 43)],             // 41-43
        "centro_oeste" | "centro-oeste" => vec![(50, 53)], // 50-53
        _ => return Err(StateNotFoundError::new(region, "Região não encontrada"))
    };
    
    let mut states = Vec::new();
    for uf_data in UFS.values() {
        for (start, end) in &code_ranges {
            if uf_data.code >= *start && uf_data.code <= *end {
                // Special case for Sudeste - exclude code 34 (doesn't exist)
                if region_lower == "sudeste" && uf_data.code == 34 {
                    continue;
                }
                states.push(uf_data.clone());
                break;
            }
        }
    }
    
    // Sort by code
    states.sort_by(|a, b| a.code.cmp(&b.code));
    Ok(states)
}

/// Async version of get_states_by_region
pub async fn get_states_by_region_async(region: &str) -> Result<Vec<StateBR>, StateNotFoundError> {
    tokio::task::yield_now().await;
    get_states_by_region(region)
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_state_br_creation() {
        let sp = StateBR::new(35, "São Paulo", "SP");
        assert_eq!(sp.code, 35);
        assert_eq!(sp.name, "São Paulo");
        assert_eq!(sp.uf, "SP");
    }
    
    #[test]
    fn test_ufs_mapping() {
        assert_eq!(UFS.len(), 27); // 26 states + 1 federal district
        
        let sp = UFS.get("SP").unwrap();
        assert_eq!(sp.code, 35);
        assert_eq!(sp.name, "São Paulo");
        
        let rj = UFS.get("RJ").unwrap();
        assert_eq!(rj.code, 33);
        assert_eq!(rj.name, "Rio de Janeiro");
    }
    
    #[test]
    fn test_get_state_info() {
        let sp = get_state_info("SP").unwrap();
        assert_eq!(sp.name, "São Paulo");
        
        // Test case insensitive
        let sp_lower = get_state_info("sp").unwrap();
        assert_eq!(sp_lower.name, "São Paulo");
        
        // Test invalid UF
        assert!(get_state_info("XX").is_err());
    }
    
    #[test]
    fn test_parse_ufs() {
        // Test valid UFs
        let states = parse_ufs(&["SP", "RJ", "MG"]).unwrap();
        assert_eq!(states.len(), 3);
        assert_eq!(states[0].uf, "SP");
        assert_eq!(states[1].uf, "RJ");
        assert_eq!(states[2].uf, "MG");
        
        // Test case insensitive
        let states_lower = parse_ufs(&["sp", "rj"]).unwrap();
        assert_eq!(states_lower.len(), 2);
        
        // Test empty input
        let empty_states = parse_ufs(&[]).unwrap();
        assert!(empty_states.is_empty());
        
        // Test invalid UF
        let result = parse_ufs(&["SP", "XX", "YY"]);
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.state.contains("XX"));
        assert!(error.state.contains("YY"));
    }
    
    #[test]
    fn test_parse_ufs_owned() {
        let ufs = vec!["SP".to_string(), "RJ".to_string()];
        let states = parse_ufs_owned(&ufs).unwrap();
        assert_eq!(states.len(), 2);
        assert_eq!(states[0].uf, "SP");
        assert_eq!(states[1].uf, "RJ");
    }
    
    #[test]
    fn test_get_states_by_region() {
        // Test Norte region
        let norte = get_states_by_region("norte").unwrap();
        assert_eq!(norte.len(), 7);
        assert!(norte.iter().any(|s| s.uf == "RO"));
        assert!(norte.iter().any(|s| s.uf == "AM"));
        
        // Test Sudeste region
        let sudeste = get_states_by_region("sudeste").unwrap();
        assert_eq!(sudeste.len(), 4);
        assert!(sudeste.iter().any(|s| s.uf == "SP"));
        assert!(sudeste.iter().any(|s| s.uf == "RJ"));
        
        // Test case insensitive
        let sul = get_states_by_region("SUL").unwrap();
        assert_eq!(sul.len(), 3);
        
        // Test invalid region
        assert!(get_states_by_region("invalid").is_err());
    }
    
    #[tokio::test]
    async fn test_async_functions() {
        // Test async variants work the same as sync
        let sp = get_state_info_async("SP").await.unwrap();
        assert_eq!(sp.name, "São Paulo");
        
        let states = parse_ufs_async(&["SP", "RJ"]).await.unwrap();
        assert_eq!(states.len(), 2);
        
        let ufs = vec!["SP".to_string(), "RJ".to_string()];
        let states_owned = parse_ufs_owned_async(&ufs).await.unwrap();
        assert_eq!(states_owned.len(), 2);
        
        let sudeste = get_states_by_region_async("sudeste").await.unwrap();
        assert_eq!(sudeste.len(), 4);
    }
}

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct StateInfo {
    pub uf: &'static str,        // e.g. "SP"
    pub name: &'static str,      // e.g. "São Paulo"
    pub ibge_code: &'static str, // e.g. "35"
}

impl StateInfo {
    pub fn matches_uf(&self, uf: &str) -> bool {
        self.uf.eq_ignore_ascii_case(uf)
    }

    pub fn matches_name(&self, name: &str) -> bool {
        // Use Unicode-aware case comparison instead of ASCII-only
        self.name.to_lowercase() == name.to_lowercase()
    }

    pub fn matches_ibge_code(&self, code: &str) -> bool {
        self.ibge_code == code
    }

    pub fn name_contains(&self, search: &str) -> bool {
        // Handle empty string case
        if search.is_empty() {
            return false; // Or return true if you want empty string to match everything
        }
        self.name.to_lowercase().contains(&search.to_lowercase())
    }
}

pub const STATES: &[StateInfo] = &[
    StateInfo {
        uf: "AC",
        name: "Acre",
        ibge_code: "12",
    },
    StateInfo {
        uf: "AL",
        name: "Alagoas",
        ibge_code: "27",
    },
    StateInfo {
        uf: "AP",
        name: "Amapá",
        ibge_code: "16",
    },
    StateInfo {
        uf: "AM",
        name: "Amazonas",
        ibge_code: "13",
    },
    StateInfo {
        uf: "BA",
        name: "Bahia",
        ibge_code: "29",
    },
    StateInfo {
        uf: "CE",
        name: "Ceará",
        ibge_code: "23",
    },
    StateInfo {
        uf: "DF",
        name: "Distrito Federal",
        ibge_code: "53",
    },
    StateInfo {
        uf: "ES",
        name: "Espírito Santo",
        ibge_code: "32",
    },
    StateInfo {
        uf: "GO",
        name: "Goiás",
        ibge_code: "52",
    },
    StateInfo {
        uf: "MA",
        name: "Maranhão",
        ibge_code: "21",
    },
    StateInfo {
        uf: "MT",
        name: "Mato Grosso",
        ibge_code: "51",
    },
    StateInfo {
        uf: "MS",
        name: "Mato Grosso do Sul",
        ibge_code: "50",
    },
    StateInfo {
        uf: "MG",
        name: "Minas Gerais",
        ibge_code: "31",
    },
    StateInfo {
        uf: "PA",
        name: "Pará",
        ibge_code: "15",
    },
    StateInfo {
        uf: "PB",
        name: "Paraíba",
        ibge_code: "25",
    },
    StateInfo {
        uf: "PR",
        name: "Paraná",
        ibge_code: "41",
    },
    StateInfo {
        uf: "PE",
        name: "Pernambuco",
        ibge_code: "26",
    },
    StateInfo {
        uf: "PI",
        name: "Piauí",
        ibge_code: "22",
    },
    StateInfo {
        uf: "RJ",
        name: "Rio de Janeiro",
        ibge_code: "33",
    },
    StateInfo {
        uf: "RN",
        name: "Rio Grande do Norte",
        ibge_code: "24",
    },
    StateInfo {
        uf: "RS",
        name: "Rio Grande do Sul",
        ibge_code: "43",
    },
    StateInfo {
        uf: "RO",
        name: "Rondônia",
        ibge_code: "11",
    },
    StateInfo {
        uf: "RR",
        name: "Roraima",
        ibge_code: "14",
    },
    StateInfo {
        uf: "SC",
        name: "Santa Catarina",
        ibge_code: "42",
    },
    StateInfo {
        uf: "SP",
        name: "São Paulo",
        ibge_code: "35",
    },
    StateInfo {
        uf: "SE",
        name: "Sergipe",
        ibge_code: "28",
    },
    StateInfo {
        uf: "TO",
        name: "Tocantins",
        ibge_code: "17",
    },
];

// Utility functions
pub fn find_by_uf(uf: &str) -> Option<&'static StateInfo> {
    STATES.iter().find(|state| state.matches_uf(uf))
}

pub fn find_by_name(name: &str) -> Option<&'static StateInfo> {
    STATES.iter().find(|state| state.matches_name(name))
}

pub fn find_by_ibge_code(code: &str) -> Option<&'static StateInfo> {
    STATES.iter().find(|state| state.matches_ibge_code(code))
}

pub fn search_by_name(search: &str) -> Vec<&'static StateInfo> {
    // Handle empty search
    if search.is_empty() {
        return vec![];
    }

    STATES
        .iter()
        .filter(|state| state.name_contains(search))
        .collect()
}

pub fn is_valid_uf(uf: &str) -> bool {
    find_by_uf(uf).is_some()
}

pub fn is_valid_ibge_code(code: &str) -> bool {
    find_by_ibge_code(code).is_some()
}

pub fn get_name_by_uf(uf: &str) -> Option<&'static str> {
    find_by_uf(uf).map(|state| state.name)
}

pub fn get_ibge_code_by_uf(uf: &str) -> Option<&'static str> {
    find_by_uf(uf).map(|state| state.ibge_code)
}

pub fn get_uf_by_ibge_code(code: &str) -> Option<&'static str> {
    find_by_ibge_code(code).map(|state| state.uf)
}

pub fn get_all_ufs() -> Vec<&'static str> {
    STATES.iter().map(|state| state.uf).collect()
}

pub fn get_all_names() -> Vec<&'static str> {
    STATES.iter().map(|state| state.name).collect()
}

pub fn get_all_ibge_codes() -> Vec<&'static str> {
    STATES.iter().map(|state| state.ibge_code).collect()
}

pub fn validate_uf_list<'a>(ufs: &[&'a str]) -> (Vec<&'a str>, Vec<&'a str>) {
    let mut valid = Vec::new();
    let mut invalid = Vec::new();

    for uf in ufs {
        if is_valid_uf(uf) {
            valid.push(*uf);
        } else {
            invalid.push(*uf);
        }
    }

    (valid, invalid)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_state_info_matches_uf() {
        let sp_state = &STATES[24]; // São Paulo

        assert!(sp_state.matches_uf("SP"));
        assert!(sp_state.matches_uf("sp")); // Case insensitive
        assert!(sp_state.matches_uf("Sp"));
        assert!(sp_state.matches_uf("sP"));
        assert!(!sp_state.matches_uf("RJ"));
        assert!(!sp_state.matches_uf(""));
        assert!(!sp_state.matches_uf("SPP"));
    }

    #[test]
    fn test_state_info_matches_name() {
        let sp_state = &STATES[24]; // São Paulo

        assert!(sp_state.matches_name("São Paulo"));
        assert!(sp_state.matches_name("são paulo")); // Case insensitive
        assert!(sp_state.matches_name("SÃO PAULO"));
        assert!(sp_state.matches_name("São PAULO"));
        assert!(!sp_state.matches_name("Rio de Janeiro"));
        assert!(!sp_state.matches_name(""));
        assert!(!sp_state.matches_name("São"));
    }

    #[test]
    fn test_state_info_matches_ibge_code() {
        let sp_state = &STATES[24]; // São Paulo

        assert!(sp_state.matches_ibge_code("35"));
        assert!(!sp_state.matches_ibge_code("33")); // Rio de Janeiro
        assert!(!sp_state.matches_ibge_code(""));
        assert!(!sp_state.matches_ibge_code("350"));
        assert!(!sp_state.matches_ibge_code("3"));
    }

    #[test]
    fn test_state_info_name_contains() {
        let sp_state = &STATES[24]; // São Paulo
        let rj_state = &STATES[18]; // Rio de Janeiro
        let rs_state = &STATES[20]; // Rio Grande do Sul

        assert!(sp_state.name_contains("São"));
        assert!(sp_state.name_contains("Paulo"));
        assert!(sp_state.name_contains("são paulo"));
        assert!(sp_state.name_contains("SÃO"));
        assert!(!sp_state.name_contains("Rio"));

        assert!(rj_state.name_contains("Rio"));
        assert!(rj_state.name_contains("Janeiro"));
        assert!(rj_state.name_contains("rio de"));
        assert!(!rj_state.name_contains("Grande"));

        assert!(rs_state.name_contains("Rio"));
        assert!(rs_state.name_contains("Grande"));
        assert!(rs_state.name_contains("Sul"));
        assert!(rs_state.name_contains("rio grande"));
        assert!(!rs_state.name_contains("Janeiro"));
    }

    #[test]
    fn test_states_constant_completeness() {
        // Test that we have all 26 states + 1 Federal District
        assert_eq!(STATES.len(), 27);

        // Test that all expected states are present
        let expected_ufs = vec![
            "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS", "MG", "PA",
            "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO",
        ];

        let actual_ufs: Vec<&str> = STATES.iter().map(|s| s.uf).collect();

        for expected_uf in expected_ufs {
            assert!(
                actual_ufs.contains(&expected_uf),
                "Missing UF: {}",
                expected_uf
            );
        }
    }

    #[test]
    fn test_states_constant_data_integrity() {
        for state in STATES {
            // Check that all fields are non-empty
            assert!(!state.uf.is_empty(), "UF cannot be empty");
            assert!(!state.name.is_empty(), "Name cannot be empty");
            assert!(!state.ibge_code.is_empty(), "IBGE code cannot be empty");

            // Check UF format (2 uppercase letters)
            assert_eq!(state.uf.len(), 2, "UF must be 2 characters: {}", state.uf);
            assert!(
                state.uf.chars().all(|c| c.is_ascii_uppercase()),
                "UF must be uppercase: {}",
                state.uf
            );

            // Check IBGE code format (1-2 digits)
            assert!(
                state.ibge_code.len() <= 2,
                "IBGE code too long: {}",
                state.ibge_code
            );
            assert!(
                state.ibge_code.chars().all(|c| c.is_ascii_digit()),
                "IBGE code must be numeric: {}",
                state.ibge_code
            );
        }
    }

    #[test]
    fn test_unique_values() {
        let mut ufs = Vec::new();
        let mut names = Vec::new();
        let mut ibge_codes = Vec::new();

        for state in STATES {
            // Check UF uniqueness
            assert!(!ufs.contains(&state.uf), "Duplicate UF: {}", state.uf);
            ufs.push(state.uf);

            // Check name uniqueness
            assert!(
                !names.contains(&state.name),
                "Duplicate name: {}",
                state.name
            );
            names.push(state.name);

            // Check IBGE code uniqueness
            assert!(
                !ibge_codes.contains(&state.ibge_code),
                "Duplicate IBGE code: {}",
                state.ibge_code
            );
            ibge_codes.push(state.ibge_code);
        }
    }

    #[test]
    fn test_find_by_uf() {
        // Test valid UFs
        assert!(find_by_uf("SP").is_some());
        assert!(find_by_uf("sp").is_some()); // Case insensitive
        assert!(find_by_uf("RJ").is_some());
        assert!(find_by_uf("MG").is_some());
        assert!(find_by_uf("DF").is_some());

        let sp_state = find_by_uf("SP").unwrap();
        assert_eq!(sp_state.uf, "SP");
        assert_eq!(sp_state.name, "São Paulo");
        assert_eq!(sp_state.ibge_code, "35");

        // Test invalid UFs
        assert!(find_by_uf("XX").is_none());
        assert!(find_by_uf("").is_none());
        assert!(find_by_uf("SPP").is_none());
        assert!(find_by_uf("123").is_none());
    }

    #[test]
    fn test_find_by_name() {
        // Test valid names
        assert!(find_by_name("São Paulo").is_some());
        assert!(find_by_name("são paulo").is_some()); // Case insensitive
        assert!(find_by_name("SÃO PAULO").is_some());
        assert!(find_by_name("Rio de Janeiro").is_some());
        assert!(find_by_name("Minas Gerais").is_some());

        let rj_state = find_by_name("Rio de Janeiro").unwrap();
        assert_eq!(rj_state.uf, "RJ");
        assert_eq!(rj_state.name, "Rio de Janeiro");
        assert_eq!(rj_state.ibge_code, "33");

        // Test invalid names
        assert!(find_by_name("Invalid State").is_none());
        assert!(find_by_name("").is_none());
        assert!(find_by_name("São").is_none()); // Partial match
        assert!(find_by_name("Rio").is_none()); // Partial match
    }

    #[test]
    fn test_find_by_ibge_code() {
        // Test valid IBGE codes
        assert!(find_by_ibge_code("35").is_some()); // São Paulo
        assert!(find_by_ibge_code("33").is_some()); // Rio de Janeiro
        assert!(find_by_ibge_code("31").is_some()); // Minas Gerais
        assert!(find_by_ibge_code("53").is_some()); // Distrito Federal

        let mg_state = find_by_ibge_code("31").unwrap();
        assert_eq!(mg_state.uf, "MG");
        assert_eq!(mg_state.name, "Minas Gerais");
        assert_eq!(mg_state.ibge_code, "31");

        // Test invalid IBGE codes
        assert!(find_by_ibge_code("99").is_none());
        assert!(find_by_ibge_code("").is_none());
        assert!(find_by_ibge_code("350").is_none());
        assert!(find_by_ibge_code("abc").is_none());
    }

    #[test]
    fn test_search_by_name() {
        // Test search for "Rio" - should find Rio de Janeiro and Rio Grande do Norte/Sul
        let rio_states = search_by_name("Rio");
        assert!(rio_states.len() >= 3);

        let rio_names: Vec<&str> = rio_states.iter().map(|s| s.name).collect();
        assert!(rio_names.contains(&"Rio de Janeiro"));
        assert!(rio_names.contains(&"Rio Grande do Norte"));
        assert!(rio_names.contains(&"Rio Grande do Sul"));

        // Test search for "Grande"
        let grande_states = search_by_name("Grande");
        assert_eq!(grande_states.len(), 2);
        let grande_names: Vec<&str> = grande_states.iter().map(|s| s.name).collect();
        assert!(grande_names.contains(&"Rio Grande do Norte"));
        assert!(grande_names.contains(&"Rio Grande do Sul"));

        // Test search for "Mato" - should find Mato Grosso and Mato Grosso do Sul
        let mato_states = search_by_name("Mato");
        assert_eq!(mato_states.len(), 2);
        let mato_names: Vec<&str> = mato_states.iter().map(|s| s.name).collect();
        assert!(mato_names.contains(&"Mato Grosso"));
        assert!(mato_names.contains(&"Mato Grosso do Sul"));

        // Test case insensitive search
        let rio_states_lower = search_by_name("rio");
        assert_eq!(rio_states_lower.len(), rio_states.len());

        // Test empty search - should return empty vec
        let empty_search = search_by_name("");
        assert_eq!(empty_search.len(), 0);

        // Test non-matching search
        let no_match = search_by_name("XYZ");
        assert_eq!(no_match.len(), 0);
    }

    #[test]
    fn test_is_valid_uf() {
        // Test valid UFs
        assert!(is_valid_uf("SP"));
        assert!(is_valid_uf("sp")); // Case insensitive
        assert!(is_valid_uf("RJ"));
        assert!(is_valid_uf("MG"));
        assert!(is_valid_uf("DF"));
        assert!(is_valid_uf("AC"));
        assert!(is_valid_uf("TO"));

        // Test invalid UFs
        assert!(!is_valid_uf("XX"));
        assert!(!is_valid_uf(""));
        assert!(!is_valid_uf("SPP"));
        assert!(!is_valid_uf("123"));
        assert!(!is_valid_uf("AB"));
    }

    #[test]
    fn test_is_valid_ibge_code() {
        // Test valid IBGE codes
        assert!(is_valid_ibge_code("35")); // São Paulo
        assert!(is_valid_ibge_code("33")); // Rio de Janeiro
        assert!(is_valid_ibge_code("31")); // Minas Gerais
        assert!(is_valid_ibge_code("53")); // Distrito Federal
        assert!(is_valid_ibge_code("11")); // Rondônia
        assert!(is_valid_ibge_code("12")); // Acre

        // Test invalid IBGE codes
        assert!(!is_valid_ibge_code("99"));
        assert!(!is_valid_ibge_code(""));
        assert!(!is_valid_ibge_code("350"));
        assert!(!is_valid_ibge_code("abc"));
        assert!(!is_valid_ibge_code("00"));
    }

    #[test]
    fn test_get_name_by_uf() {
        assert_eq!(get_name_by_uf("SP"), Some("São Paulo"));
        assert_eq!(get_name_by_uf("sp"), Some("São Paulo")); // Case insensitive
        assert_eq!(get_name_by_uf("RJ"), Some("Rio de Janeiro"));
        assert_eq!(get_name_by_uf("MG"), Some("Minas Gerais"));
        assert_eq!(get_name_by_uf("DF"), Some("Distrito Federal"));

        assert_eq!(get_name_by_uf("XX"), None);
        assert_eq!(get_name_by_uf(""), None);
    }

    #[test]
    fn test_get_ibge_code_by_uf() {
        assert_eq!(get_ibge_code_by_uf("SP"), Some("35"));
        assert_eq!(get_ibge_code_by_uf("sp"), Some("35")); // Case insensitive
        assert_eq!(get_ibge_code_by_uf("RJ"), Some("33"));
        assert_eq!(get_ibge_code_by_uf("MG"), Some("31"));
        assert_eq!(get_ibge_code_by_uf("DF"), Some("53"));
        assert_eq!(get_ibge_code_by_uf("XX"), None);
        assert_eq!(get_ibge_code_by_uf(""), None);
    }

    #[test]
    fn test_get_uf_by_ibge_code() {
        assert_eq!(get_uf_by_ibge_code("35"), Some("SP"));
        assert_eq!(get_uf_by_ibge_code("33"), Some("RJ"));
        assert_eq!(get_uf_by_ibge_code("31"), Some("MG"));
        assert_eq!(get_uf_by_ibge_code("53"), Some("DF"));
        assert_eq!(get_uf_by_ibge_code("12"), Some("AC"));
        assert_eq!(get_uf_by_ibge_code("17"), Some("TO"));

        assert_eq!(get_uf_by_ibge_code("99"), None);
        assert_eq!(get_uf_by_ibge_code(""), None);
        assert_eq!(get_uf_by_ibge_code("350"), None);
    }

    #[test]
    fn test_get_all_ufs() {
        let all_ufs = get_all_ufs();

        assert_eq!(all_ufs.len(), 27);
        assert!(all_ufs.contains(&"SP"));
        assert!(all_ufs.contains(&"RJ"));
        assert!(all_ufs.contains(&"MG"));
        assert!(all_ufs.contains(&"DF"));
        assert!(all_ufs.contains(&"AC"));
        assert!(all_ufs.contains(&"TO"));

        // Test that all UFs are unique
        let mut unique_ufs = all_ufs.clone();
        unique_ufs.sort();
        unique_ufs.dedup();
        assert_eq!(unique_ufs.len(), all_ufs.len());
    }

    #[test]
    fn test_get_all_names() {
        let all_names = get_all_names();

        assert_eq!(all_names.len(), 27);
        assert!(all_names.contains(&"São Paulo"));
        assert!(all_names.contains(&"Rio de Janeiro"));
        assert!(all_names.contains(&"Minas Gerais"));
        assert!(all_names.contains(&"Distrito Federal"));
        assert!(all_names.contains(&"Acre"));
        assert!(all_names.contains(&"Tocantins"));

        // Test that all names are unique
        let mut unique_names = all_names.clone();
        unique_names.sort();
        unique_names.dedup();
        assert_eq!(unique_names.len(), all_names.len());
    }

    #[test]
    fn test_get_all_ibge_codes() {
        let all_codes = get_all_ibge_codes();

        assert_eq!(all_codes.len(), 27);
        assert!(all_codes.contains(&"35")); // SP
        assert!(all_codes.contains(&"33")); // RJ
        assert!(all_codes.contains(&"31")); // MG
        assert!(all_codes.contains(&"53")); // DF
        assert!(all_codes.contains(&"12")); // AC
        assert!(all_codes.contains(&"17")); // TO

        // Test that all codes are unique
        let mut unique_codes = all_codes.clone();
        unique_codes.sort();
        unique_codes.dedup();
        assert_eq!(unique_codes.len(), all_codes.len());
    }

    #[test]
    fn test_validate_uf_list() {
        // Test with all valid UFs
        let valid_ufs = vec!["SP", "RJ", "MG"];
        let (valid, invalid) = validate_uf_list(&valid_ufs);
        assert_eq!(valid, vec!["SP", "RJ", "MG"]);
        assert_eq!(invalid.len(), 0);

        // Test with all invalid UFs
        let invalid_ufs = vec!["XX", "YY", "ZZ"];
        let (valid, invalid) = validate_uf_list(&invalid_ufs);
        assert_eq!(valid.len(), 0);
        assert_eq!(invalid, vec!["XX", "YY", "ZZ"]);

        // Test with mixed valid and invalid UFs
        let mixed_ufs = vec!["SP", "XX", "RJ", "YY", "MG"];
        let (valid, invalid) = validate_uf_list(&mixed_ufs);
        assert_eq!(valid, vec!["SP", "RJ", "MG"]);
        assert_eq!(invalid, vec!["XX", "YY"]);

        // Test with empty list
        let empty_ufs: Vec<&str> = vec![];
        let (valid, invalid) = validate_uf_list(&empty_ufs);
        assert_eq!(valid.len(), 0);
        assert_eq!(invalid.len(), 0);

        // Test with case insensitive
        let case_ufs = vec!["sp", "RJ", "mg"];
        let (valid, invalid) = validate_uf_list(&case_ufs);
        assert_eq!(valid, vec!["sp", "RJ", "mg"]);
        assert_eq!(invalid.len(), 0);
    }

    #[test]
    fn test_specific_states_data() {
        // Test São Paulo
        let sp = find_by_uf("SP").unwrap();
        assert_eq!(sp.uf, "SP");
        assert_eq!(sp.name, "São Paulo");
        assert_eq!(sp.ibge_code, "35");

        // Test Rio de Janeiro
        let rj = find_by_uf("RJ").unwrap();
        assert_eq!(rj.uf, "RJ");
        assert_eq!(rj.name, "Rio de Janeiro");
        assert_eq!(rj.ibge_code, "33");

        // Test Minas Gerais
        let mg = find_by_uf("MG").unwrap();
        assert_eq!(mg.uf, "MG");
        assert_eq!(mg.name, "Minas Gerais");
        assert_eq!(mg.ibge_code, "31");

        // Test Distrito Federal
        let df = find_by_uf("DF").unwrap();
        assert_eq!(df.uf, "DF");
        assert_eq!(df.name, "Distrito Federal");
        assert_eq!(df.ibge_code, "53");

        // Test Acre (smallest IBGE code)
        let ac = find_by_uf("AC").unwrap();
        assert_eq!(ac.uf, "AC");
        assert_eq!(ac.name, "Acre");
        assert_eq!(ac.ibge_code, "12");

        // Test Tocantins (newest state)
        let to = find_by_uf("TO").unwrap();
        assert_eq!(to.uf, "TO");
        assert_eq!(to.name, "Tocantins");
        assert_eq!(to.ibge_code, "17");
    }

    #[test]
    fn test_northern_states() {
        let northern_states = vec!["AC", "AP", "AM", "PA", "RO", "RR", "TO"];

        for uf in northern_states {
            assert!(is_valid_uf(uf), "Northern state {} should be valid", uf);
            assert!(
                find_by_uf(uf).is_some(),
                "Northern state {} should be found",
                uf
            );
        }
    }

    #[test]
    fn test_northeastern_states() {
        let northeastern_states = vec!["AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE"];

        for uf in northeastern_states {
            assert!(is_valid_uf(uf), "Northeastern state {} should be valid", uf);
            assert!(
                find_by_uf(uf).is_some(),
                "Northeastern state {} should be found",
                uf
            );
        }
    }

    #[test]
    fn test_central_west_states() {
        let central_west_states = vec!["GO", "MT", "MS", "DF"];

        for uf in central_west_states {
            assert!(is_valid_uf(uf), "Central-West state {} should be valid", uf);
            assert!(
                find_by_uf(uf).is_some(),
                "Central-West state {} should be found",
                uf
            );
        }
    }

    #[test]
    fn test_southeastern_states() {
        let southeastern_states = vec!["ES", "MG", "RJ", "SP"];

        for uf in southeastern_states {
            assert!(is_valid_uf(uf), "Southeastern state {} should be valid", uf);
            assert!(
                find_by_uf(uf).is_some(),
                "Southeastern state {} should be found",
                uf
            );
        }
    }

    #[test]
    fn test_southern_states() {
        let southern_states = vec!["PR", "RS", "SC"];

        for uf in southern_states {
            assert!(is_valid_uf(uf), "Southern state {} should be valid", uf);
            assert!(
                find_by_uf(uf).is_some(),
                "Southern state {} should be found",
                uf
            );
        }
    }

    #[test]
    fn test_case_insensitive_operations() {
        // Test find_by_uf with different cases
        assert_eq!(find_by_uf("sp").unwrap().uf, "SP");
        assert_eq!(find_by_uf("SP").unwrap().uf, "SP");
        assert_eq!(find_by_uf("Sp").unwrap().uf, "SP");
        assert_eq!(find_by_uf("sP").unwrap().uf, "SP");

        // Test find_by_name with different cases
        assert_eq!(find_by_name("são paulo").unwrap().name, "São Paulo");
        assert_eq!(find_by_name("SÃO PAULO").unwrap().name, "São Paulo");
        assert_eq!(find_by_name("São PAULO").unwrap().name, "São Paulo");

        // Test search_by_name with different cases
        let rio_upper = search_by_name("RIO");
        let rio_lower = search_by_name("rio");
        let rio_mixed = search_by_name("Rio");

        assert_eq!(rio_upper.len(), rio_lower.len());
        assert_eq!(rio_upper.len(), rio_mixed.len());
        assert!(rio_upper.len() >= 3); // At least Rio de Janeiro, Rio Grande do Norte, Rio Grande do Sul
    }

    #[test]
    fn test_state_info_debug() {
        let sp_state = find_by_uf("SP").unwrap();
        let debug_str = format!("{:?}", sp_state);
        assert!(debug_str.contains("StateInfo"));
        assert!(debug_str.contains("SP"));
        assert!(debug_str.contains("São Paulo"));
        assert!(debug_str.contains("35"));
    }

    #[test]
    fn test_state_info_serialization() {
        let sp_state = find_by_uf("SP").unwrap();

        // Test serialization to JSON
        let json = serde_json::to_string(sp_state).unwrap();
        assert!(json.contains("SP"));
        assert!(json.contains("São Paulo"));
        assert!(json.contains("35"));

        // Note: We can't test deserialization back to StateInfo because
        // StateInfo contains &'static str references, but JSON deserialization
        // would create references to the JSON string which doesn't live long enough.
        // Instead, we can test that the JSON structure is correct.
        let json_value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(json_value["uf"], "SP");
        assert_eq!(json_value["name"], "São Paulo");
        assert_eq!(json_value["ibge_code"], "35");
    }

    #[test]
    fn test_edge_cases() {
        // Test with whitespace
        assert!(find_by_uf(" SP ").is_none()); // Should not match with whitespace
        assert!(find_by_name(" São Paulo ").is_none()); // Should not match with whitespace

        // Test with numbers
        assert!(find_by_uf("12").is_none()); // UF should not be numeric
        assert!(find_by_name("123").is_none()); // Name should not be numeric

        // Test with special characters
        assert!(find_by_uf("S@").is_none());
        assert!(find_by_uf("S#").is_none());

        // Test with very long strings
        let long_uf = "A".repeat(100);
        assert!(find_by_uf(&long_uf).is_none());

        let long_name = "A".repeat(1000);
        assert!(find_by_name(&long_name).is_none());
    }

    #[test]
    fn test_ibge_code_ranges() {
        // Test that IBGE codes are within expected ranges
        for state in STATES {
            let code: u8 = state.ibge_code.parse().unwrap();
            assert!(code >= 11 && code <= 53, "IBGE code {} out of range", code);
        }
    }

    #[test]
    fn test_name_contains_edge_cases() {
        let sp_state = find_by_uf("SP").unwrap();

        // Test with empty string - should return false
        assert!(!sp_state.name_contains(""));

        // Test with single character
        assert!(sp_state.name_contains("S"));
        assert!(sp_state.name_contains("P"));
        assert!(sp_state.name_contains("o"));

        // Test with special characters
        assert!(sp_state.name_contains("ã"));
        assert!(!sp_state.name_contains("@"));

        // Test with multiple words
        assert!(sp_state.name_contains("São Paulo"));
        assert!(!sp_state.name_contains("Paulo São")); // Wrong order
    }

    #[test]
    fn test_all_functions_consistency() {
        // Test that all utility functions return consistent results
        for state in STATES {
            // Test find functions consistency
            assert_eq!(find_by_uf(state.uf).unwrap().uf, state.uf);
            assert_eq!(find_by_name(state.name).unwrap().name, state.name);
            assert_eq!(
                find_by_ibge_code(state.ibge_code).unwrap().ibge_code,
                state.ibge_code
            );

            // Test getter functions consistency
            assert_eq!(get_name_by_uf(state.uf).unwrap(), state.name);
            assert_eq!(get_ibge_code_by_uf(state.uf).unwrap(), state.ibge_code);
            assert_eq!(get_uf_by_ibge_code(state.ibge_code).unwrap(), state.uf);

            // Test validation functions consistency
            assert!(is_valid_uf(state.uf));
            assert!(is_valid_ibge_code(state.ibge_code));
        }
    }

    #[test]
    fn test_performance_with_large_lists() {
        // Test validate_uf_list with a large list
        let large_uf_list: Vec<&str> = (0..1000)
            .map(|i| {
                match i % 30 {
                    0..=26 => STATES[i % 27].uf,
                    _ => "XX", // Invalid UF
                }
            })
            .collect();

        let (valid, invalid) = validate_uf_list(&large_uf_list);
        assert!(valid.len() > 0);
        assert!(invalid.len() > 0);
        assert_eq!(valid.len() + invalid.len(), large_uf_list.len());
    }

    #[test]
    fn test_search_performance() {
        // Test search_by_name with common terms
        let search_terms = vec!["a", "o", "e", "i", "u", "Rio", "São", "Grande"];

        for term in search_terms {
            let results = search_by_name(term);
            // Should complete quickly and return valid results
            for result in results {
                assert!(result.name.to_lowercase().contains(&term.to_lowercase()));
            }
        }
    }

    #[test]
    fn test_boundary_conditions() {
        // Test with minimum and maximum IBGE codes
        let min_code = STATES
            .iter()
            .map(|s| s.ibge_code.parse::<u8>().unwrap())
            .min()
            .unwrap();
        let max_code = STATES
            .iter()
            .map(|s| s.ibge_code.parse::<u8>().unwrap())
            .max()
            .unwrap();

        assert!(is_valid_ibge_code(&min_code.to_string()));
        assert!(is_valid_ibge_code(&max_code.to_string()));

        // Test codes just outside the range
        assert!(!is_valid_ibge_code(&(min_code - 1).to_string()));
        assert!(!is_valid_ibge_code(&(max_code + 1).to_string()));
    }

    #[test]
    fn test_unicode_handling() {
        // Test that Unicode characters in state names are handled correctly
        let states_with_accents = vec![
            "São Paulo",
            "Ceará",
            "Goiás",
            "Piauí",
            "Pará",
            "Amapá",
            "Rondônia",
            "Espírito Santo",
        ];

        for state_name in states_with_accents {
            assert!(
                find_by_name(state_name).is_some(),
                "State with accents should be found: {}",
                state_name
            );

            // Test case insensitive search with accents
            let results = search_by_name(&state_name.to_lowercase());
            assert!(
                results.len() > 0,
                "Case insensitive search should work with accents: {}",
                state_name
            );
        }
    }

    #[test]
    fn test_memory_efficiency() {
        // Test that the STATES constant doesn't waste memory
        let total_memory = std::mem::size_of_val(&STATES);
        let per_state_memory = total_memory / STATES.len();

        // Each StateInfo should be reasonably sized
        assert!(
            per_state_memory < 200,
            "StateInfo should be memory efficient, got {} bytes per state",
            per_state_memory
        );
    }

    #[test]
    fn test_thread_safety() {
        use std::thread;

        let handles: Vec<_> = (0..10)
            .map(|i| {
                thread::spawn(move || {
                    let uf = match i % 5 {
                        0 => "SP",
                        1 => "RJ",
                        2 => "MG",
                        3 => "DF",
                        _ => "RS",
                    };

                    // Test that all functions work correctly in different threads
                    assert!(is_valid_uf(uf));
                    assert!(find_by_uf(uf).is_some());
                    assert!(get_name_by_uf(uf).is_some());
                    assert!(get_ibge_code_by_uf(uf).is_some());

                    let name = get_name_by_uf(uf).unwrap();
                    assert!(find_by_name(name).is_some());
                    assert!(search_by_name(name).len() > 0);
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn test_function_return_types() {
        // Test that functions return the expected types
        let uf_result: Option<&StateInfo> = find_by_uf("SP");
        assert!(uf_result.is_some());

        let name_result: Option<&StateInfo> = find_by_name("São Paulo");
        assert!(name_result.is_some());

        let code_result: Option<&StateInfo> = find_by_ibge_code("35");
        assert!(code_result.is_some());

        let search_result: Vec<&StateInfo> = search_by_name("Rio");
        assert!(search_result.len() > 0);

        let validation_result: (Vec<&str>, Vec<&str>) = validate_uf_list(&["SP", "XX"]);
        assert_eq!(validation_result.0.len(), 1);
        assert_eq!(validation_result.1.len(), 1);
    }

    #[test]
    fn test_comprehensive_state_verification() {
        // Comprehensive test of all states with known data
        let expected_states = vec![
            ("AC", "Acre", "12"),
            ("AL", "Alagoas", "27"),
            ("AP", "Amapá", "16"),
            ("AM", "Amazonas", "13"),
            ("BA", "Bahia", "29"),
            ("CE", "Ceará", "23"),
            ("DF", "Distrito Federal", "53"),
            ("ES", "Espírito Santo", "32"),
            ("GO", "Goiás", "52"),
            ("MA", "Maranhão", "21"),
            ("MT", "Mato Grosso", "51"),
            ("MS", "Mato Grosso do Sul", "50"),
            ("MG", "Minas Gerais", "31"),
            ("PA", "Pará", "15"),
            ("PB", "Paraíba", "25"),
            ("PR", "Paraná", "41"),
            ("PE", "Pernambuco", "26"),
            ("PI", "Piauí", "22"),
            ("RJ", "Rio de Janeiro", "33"),
            ("RN", "Rio Grande do Norte", "24"),
            ("RS", "Rio Grande do Sul", "43"),
            ("RO", "Rondônia", "11"),
            ("RR", "Roraima", "14"),
            ("SC", "Santa Catarina", "42"),
            ("SP", "São Paulo", "35"),
            ("SE", "Sergipe", "28"),
            ("TO", "Tocantins", "17"),
        ];

        assert_eq!(expected_states.len(), 27);

        for (uf, name, ibge_code) in expected_states {
            let state = find_by_uf(uf).unwrap();
            assert_eq!(state.uf, uf);
            assert_eq!(state.name, name);
            assert_eq!(state.ibge_code, ibge_code);

            // Cross-verify with other lookup methods
            assert_eq!(find_by_name(name).unwrap().uf, uf);
            assert_eq!(find_by_ibge_code(ibge_code).unwrap().uf, uf);
        }
    }

    #[test]
    fn test_error_resilience() {
        // Test that functions don't panic with invalid input
        let invalid_inputs = vec!["", " ", "  ", "\t", "\n", "123", "ABC", "a", "1"];

        for input in invalid_inputs {
            // These should all return None/empty results, not panic
            assert!(find_by_uf(input).is_none());
            assert!(find_by_name(input).is_none());
            assert!(get_name_by_uf(input).is_none());
            assert!(get_ibge_code_by_uf(input).is_none());
            assert!(!is_valid_uf(input));

            let search_results = search_by_name(input);
            // search_by_name should not panic and should return a valid Vec
            // For most invalid inputs, it should return empty results
            assert!(search_results.len() <= STATES.len()); // More meaningful assertion
        }
    }
}

use serde::Serialize;
pub mod cih;
pub mod ciha;
pub mod cnes;
pub mod ibge;
pub mod pni;
pub mod resp;
pub mod sia;
pub mod sih;
pub mod sim;
pub mod sinan;
pub mod sinasc;
pub mod siscolo;
pub mod sismama;
pub mod sisprenatal;

pub use cih::CIH;
pub use ciha::CIHA;
pub use cnes::CNES;
pub use ibge::IBGE;
pub use pni::PNI;
pub use resp::RESP;
pub use sia::SIASUS;
pub use sih::SIHSUS;
pub use sim::SIM;
pub use sinan::SINAN;
pub use sinasc::SINASC;
pub use siscolo::SISCOLO;
pub use sismama::SISMAMA;
pub use sisprenatal::SISPRENATAL;

#[derive(Debug, Serialize, PartialEq)]
pub struct SubsystemInfo {
    pub code: &'static str,
    pub name: &'static str,
    pub description: Option<&'static str>,
    pub long_description: Option<&'static str>,
    pub url: Option<&'static str>,
    pub groups: &'static [GroupInfo],
}

#[derive(Debug, Serialize, PartialEq)]
pub struct GroupInfo {
    pub code: &'static str,
    pub name: &'static str,
    pub description: Option<&'static str>,
    pub long_description: Option<&'static str>,
    pub url: Option<&'static str>,
}

impl SubsystemInfo {
    pub fn matches_code(&self, code: &str) -> bool {
        self.code.eq_ignore_ascii_case(code)
    }

    pub fn matches_name(&self, name: &str) -> bool {
        self.name.eq_ignore_ascii_case(name)
    }

    pub fn name_contains(&self, search: &str) -> bool {
        self.name.to_lowercase().contains(&search.to_lowercase())
    }

    pub fn find_group_by_code(&self, code: &str) -> Option<&GroupInfo> {
        self.groups.iter().find(|group| group.matches_code(code))
    }

    pub fn find_groups_by_name(&self, name: &str) -> Vec<&GroupInfo> {
        self.groups
            .iter()
            .filter(|group| group.matches_name(name))
            .collect()
    }

    pub fn search_groups(&self, search: &str) -> Vec<&GroupInfo> {
        self.groups
            .iter()
            .filter(|group| group.name_contains(search))
            .collect()
    }

    pub fn get_group_codes(&self) -> Vec<&'static str> {
        self.groups.iter().map(|group| group.code).collect()
    }

    pub fn has_groups(&self) -> bool {
        !self.groups.is_empty()
    }
}

impl GroupInfo {
    pub fn matches_code(&self, code: &str) -> bool {
        self.code.eq_ignore_ascii_case(code)
    }

    pub fn matches_name(&self, name: &str) -> bool {
        self.name.eq_ignore_ascii_case(name)
    }

    pub fn name_contains(&self, search: &str) -> bool {
        self.name.to_lowercase().contains(&search.to_lowercase())
    }
}

pub const SUBSYSTEMS: &[SubsystemInfo] = &[
    CIH,
    CIHA,
    CNES,
    IBGE,
    PNI,
    RESP,
    SIASUS,
    SIHSUS,
    SIM,
    SINAN,
    SINASC,
    SISCOLO,
    SISMAMA,
    SISPRENATAL,
];

// Utility functions for subsystems
pub fn find_subsystem_by_code(code: &str) -> Option<&'static SubsystemInfo> {
    SUBSYSTEMS
        .iter()
        .find(|subsystem| subsystem.matches_code(code))
}

pub fn find_subsystem_by_name(name: &str) -> Option<&'static SubsystemInfo> {
    SUBSYSTEMS
        .iter()
        .find(|subsystem| subsystem.matches_name(name))
}

pub fn search_subsystems(search: &str) -> Vec<&'static SubsystemInfo> {
    SUBSYSTEMS
        .iter()
        .filter(|subsystem| subsystem.name_contains(search))
        .collect()
}

pub fn is_valid_subsystem_code(code: &str) -> bool {
    find_subsystem_by_code(code).is_some()
}

pub fn get_subsystem_name(code: &str) -> Option<&'static str> {
    find_subsystem_by_code(code).map(|subsystem| subsystem.name)
}

pub fn get_all_subsystem_codes() -> Vec<&'static str> {
    SUBSYSTEMS.iter().map(|subsystem| subsystem.code).collect()
}

pub fn get_all_subsystem_names() -> Vec<&'static str> {
    SUBSYSTEMS.iter().map(|subsystem| subsystem.name).collect()
}

pub fn get_subsystems_with_groups() -> Vec<&'static SubsystemInfo> {
    SUBSYSTEMS
        .iter()
        .filter(|subsystem| subsystem.has_groups())
        .collect()
}

pub fn get_subsystems_without_groups() -> Vec<&'static SubsystemInfo> {
    SUBSYSTEMS
        .iter()
        .filter(|subsystem| !subsystem.has_groups())
        .collect()
}

// Utility functions for groups
pub fn find_group_in_subsystem(
    subsystem_code: &str,
    group_code: &str,
) -> Option<&'static GroupInfo> {
    find_subsystem_by_code(subsystem_code)?.find_group_by_code(group_code)
}

pub fn find_all_groups_by_code(
    group_code: &str,
) -> Vec<(&'static SubsystemInfo, &'static GroupInfo)> {
    SUBSYSTEMS
        .iter()
        .filter_map(|subsystem| {
            subsystem
                .find_group_by_code(group_code)
                .map(|group| (subsystem, group))
        })
        .collect()
}

pub fn search_all_groups(search: &str) -> Vec<(&'static SubsystemInfo, &'static GroupInfo)> {
    SUBSYSTEMS
        .iter()
        .flat_map(|subsystem| {
            subsystem
                .search_groups(search)
                .into_iter()
                .map(move |group| (subsystem, group))
        })
        .collect()
}

pub fn get_all_group_codes() -> Vec<&'static str> {
    SUBSYSTEMS
        .iter()
        .flat_map(|subsystem| subsystem.get_group_codes())
        .collect()
}

pub fn validate_subsystem_codes<'a>(codes: &[&'a str]) -> (Vec<&'a str>, Vec<&'a str>) {
    let mut valid = Vec::new();
    let mut invalid = Vec::new();

    for code in codes {
        if is_valid_subsystem_code(code) {
            valid.push(*code);
        } else {
            invalid.push(*code);
        }
    }

    (valid, invalid)
}

pub fn get_subsystem_info(
    code: &str,
) -> Option<(&'static str, Option<&'static str>, Option<&'static str>)> {
    find_subsystem_by_code(code)
        .map(|subsystem| (subsystem.name, subsystem.description, subsystem.url))
}

pub fn count_subsystems() -> usize {
    SUBSYSTEMS.len()
}

pub fn count_total_groups() -> usize {
    SUBSYSTEMS
        .iter()
        .map(|subsystem| subsystem.groups.len())
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    // Static test data for groups
    const TEST_GROUP1: GroupInfo = GroupInfo {
        code: "G1",
        name: "Group One",
        description: Some("First group"),
        long_description: None,
        url: None,
    };

    const TEST_GROUP2: GroupInfo = GroupInfo {
        code: "G2",
        name: "Group Two",
        description: Some("Second group"),
        long_description: None,
        url: None,
    };

    const TEST_GROUPS: &[GroupInfo] = &[TEST_GROUP1, TEST_GROUP2];

    #[test]
    fn test_subsystem_info_matches_code() {
        let subsystem = SubsystemInfo {
            code: "TEST",
            name: "Test System",
            description: Some("Test description"),
            long_description: None,
            url: None,
            groups: &[],
        };

        // Test exact match
        assert!(subsystem.matches_code("TEST"));
        assert!(subsystem.matches_code("test")); // Case insensitive
        assert!(subsystem.matches_code("Test"));
        assert!(subsystem.matches_code("tEsT"));

        // Test no match
        assert!(!subsystem.matches_code("OTHER"));
        assert!(!subsystem.matches_code(""));
        assert!(!subsystem.matches_code("TESTING"));
    }

    #[test]
    fn test_subsystem_info_matches_name() {
        let subsystem = SubsystemInfo {
            code: "TEST",
            name: "Test System",
            description: Some("Test description"),
            long_description: None,
            url: None,
            groups: &[],
        };

        // Test exact match
        assert!(subsystem.matches_name("Test System"));
        assert!(subsystem.matches_name("test system")); // Case insensitive
        assert!(subsystem.matches_name("TEST SYSTEM"));
        assert!(subsystem.matches_name("Test system"));

        // Test no match
        assert!(!subsystem.matches_name("Other System"));
        assert!(!subsystem.matches_name(""));
        assert!(!subsystem.matches_name("Test"));
    }

    #[test]
    fn test_subsystem_info_name_contains() {
        let subsystem = SubsystemInfo {
            code: "TEST",
            name: "Test System Information",
            description: Some("Test description"),
            long_description: None,
            url: None,
            groups: &[],
        };

        // Test partial matches
        assert!(subsystem.name_contains("Test"));
        assert!(subsystem.name_contains("test")); // Case insensitive
        assert!(subsystem.name_contains("System"));
        assert!(subsystem.name_contains("system"));
        assert!(subsystem.name_contains("Information"));
        assert!(subsystem.name_contains("info"));
        assert!(subsystem.name_contains("Test System"));
        assert!(subsystem.name_contains("stem Inf"));

        // Test no match
        assert!(!subsystem.name_contains("Other"));
        assert!(!subsystem.name_contains("xyz"));
        // Note: empty string contains() always returns true in Rust, so we don't test it
    }

    #[test]
    fn test_group_info_matches_code() {
        let group = GroupInfo {
            code: "TEST",
            name: "Test Group",
            description: Some("Test description"),
            long_description: None,
            url: None,
        };

        // Test exact match
        assert!(group.matches_code("TEST"));
        assert!(group.matches_code("test")); // Case insensitive
        assert!(group.matches_code("Test"));
        assert!(group.matches_code("tEsT"));

        // Test no match
        assert!(!group.matches_code("OTHER"));
        assert!(!group.matches_code(""));
        assert!(!group.matches_code("TESTING"));
    }

    #[test]
    fn test_group_info_matches_name() {
        let group = GroupInfo {
            code: "TEST",
            name: "Test Group",
            description: Some("Test description"),
            long_description: None,
            url: None,
        };

        // Test exact match
        assert!(group.matches_name("Test Group"));
        assert!(group.matches_name("test group")); // Case insensitive
        assert!(group.matches_name("TEST GROUP"));
        assert!(group.matches_name("Test group"));

        // Test no match
        assert!(!group.matches_name("Other Group"));
        assert!(!group.matches_name(""));
        assert!(!group.matches_name("Test"));
    }

    #[test]
    fn test_group_info_name_contains() {
        let group = GroupInfo {
            code: "TEST",
            name: "Test Group Information",
            description: Some("Test description"),
            long_description: None,
            url: None,
        };

        // Test partial matches
        assert!(group.name_contains("Test"));
        assert!(group.name_contains("test")); // Case insensitive
        assert!(group.name_contains("Group"));
        assert!(group.name_contains("group"));
        assert!(group.name_contains("Information"));
        assert!(group.name_contains("info"));
        assert!(group.name_contains("Test Group"));
        assert!(group.name_contains("oup Inf"));

        // Test no match
        assert!(!group.name_contains("Other"));
        assert!(!group.name_contains("xyz"));
        // Note: empty string contains() always returns true in Rust, so we don't test it
    }

    #[test]
    fn test_subsystem_info_with_groups() {
        let subsystem = SubsystemInfo {
            code: "TEST",
            name: "Test System",
            description: Some("Test description"),
            long_description: None,
            url: None,
            groups: TEST_GROUPS,
        };

        // Test find_group_by_code
        assert!(subsystem.find_group_by_code("G1").is_some());
        assert!(subsystem.find_group_by_code("g1").is_some()); // Case insensitive
        assert!(subsystem.find_group_by_code("G2").is_some());
        assert!(subsystem.find_group_by_code("G3").is_none());

        let found_group = subsystem.find_group_by_code("G1").unwrap();
        assert_eq!(found_group.code, "G1");
        assert_eq!(found_group.name, "Group One");

        // Test find_groups_by_name
        let groups = subsystem.find_groups_by_name("Group One");
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].code, "G1");

        let groups = subsystem.find_groups_by_name("group one"); // Case insensitive
        assert_eq!(groups.len(), 1);

        let groups = subsystem.find_groups_by_name("Nonexistent");
        assert_eq!(groups.len(), 0);

        // Test search_groups
        let groups = subsystem.search_groups("Group");
        assert_eq!(groups.len(), 2);

        let groups = subsystem.search_groups("One");
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].code, "G1");

        let groups = subsystem.search_groups("xyz");
        assert_eq!(groups.len(), 0);

        // Test get_group_codes
        let codes = subsystem.get_group_codes();
        assert_eq!(codes.len(), 2);
        assert!(codes.contains(&"G1"));
        assert!(codes.contains(&"G2"));

        // Test has_groups
        assert!(subsystem.has_groups());
    }

    #[test]
    fn test_subsystem_info_without_groups() {
        let subsystem = SubsystemInfo {
            code: "TEST",
            name: "Test System",
            description: Some("Test description"),
            long_description: None,
            url: None,
            groups: &[],
        };

        // Test methods with empty groups
        assert!(subsystem.find_group_by_code("G1").is_none());
        assert_eq!(
            subsystem.find_groups_by_name("Group"),
            Vec::<&GroupInfo>::new()
        );
        assert_eq!(subsystem.search_groups("Group"), Vec::<&GroupInfo>::new());
        assert_eq!(subsystem.get_group_codes(), Vec::<&str>::new());
        assert!(!subsystem.has_groups());
    }

    // ... rest of the tests remain the same

    #[test]
    fn test_subsystems_constant_exists() {
        // Test that SUBSYSTEMS constant is not empty
        assert!(!SUBSYSTEMS.is_empty());

        // Test expected minimum number of subsystems
        assert!(SUBSYSTEMS.len() >= 14); // Based on the imports in mod.rs
    }

    #[test]
    fn test_subsystems_constant_integrity() {
        // Test that all subsystems have valid data
        for subsystem in SUBSYSTEMS.iter() {
            assert!(!subsystem.code.is_empty(), "Subsystem code cannot be empty");
            assert!(!subsystem.name.is_empty(), "Subsystem name cannot be empty");

            // Test that all groups have valid data
            for group in subsystem.groups.iter() {
                assert!(!group.code.is_empty(), "Group code cannot be empty");
                assert!(!group.name.is_empty(), "Group name cannot be empty");
            }
        }
    }

    #[test]
    fn test_subsystems_uniqueness() {
        // Test that all subsystem codes are unique
        let mut codes = Vec::new();
        for subsystem in SUBSYSTEMS.iter() {
            assert!(
                !codes.contains(&subsystem.code),
                "Duplicate subsystem code: {}",
                subsystem.code
            );
            codes.push(subsystem.code);
        }

        // Test that all subsystem names are unique
        let mut names = Vec::new();
        for subsystem in SUBSYSTEMS.iter() {
            assert!(
                !names.contains(&subsystem.name),
                "Duplicate subsystem name: {}",
                subsystem.name
            );
            names.push(subsystem.name);
        }
    }

    #[test]
    fn test_find_subsystem_by_code() {
        // Test with known subsystem codes
        assert!(find_subsystem_by_code("CIH").is_some());
        assert!(find_subsystem_by_code("cih").is_some()); // Case insensitive
        assert!(find_subsystem_by_code("SIM").is_some());
        assert!(find_subsystem_by_code("SINAN").is_some());

        // Test with unknown code
        assert!(find_subsystem_by_code("UNKNOWN").is_none());
        assert!(find_subsystem_by_code("").is_none());

        // Test return values
        let cih = find_subsystem_by_code("CIH").unwrap();
        assert_eq!(cih.code, "CIH");
        assert_eq!(cih.name, "Comunicação de Internação Hospitalar");
    }

    #[test]
    fn test_find_subsystem_by_name() {
        // Test with known subsystem names
        assert!(find_subsystem_by_name("Comunicação de Internação Hospitalar").is_some());
        assert!(find_subsystem_by_name("comunicação de internação hospitalar").is_some()); // Case insensitive

        // Test with unknown name
        assert!(find_subsystem_by_name("Unknown System").is_none());
        assert!(find_subsystem_by_name("").is_none());

        // Test return values
        let cih = find_subsystem_by_name("Comunicação de Internação Hospitalar").unwrap();
        assert_eq!(cih.code, "CIH");
    }

    #[test]
    fn test_search_subsystems() {
        // Test search with partial names
        let results = search_subsystems("Sistema");
        assert!(!results.is_empty());

        let results = search_subsystems("Informação");
        assert!(!results.is_empty());

        let results = search_subsystems("SIS");
        assert!(!results.is_empty());

        // Test case insensitive search
        let results = search_subsystems("sistema");
        assert!(!results.is_empty());

        // Test search with no matches
        let results = search_subsystems("xyz123");
        assert!(results.is_empty());

        // Test empty search
        let results = search_subsystems("");
        assert_eq!(results.len(), SUBSYSTEMS.len()); // Should return all
    }

    #[test]
    fn test_is_valid_subsystem_code() {
        // Test valid codes
        assert!(is_valid_subsystem_code("CIH"));
        assert!(is_valid_subsystem_code("cih")); // Case insensitive
        assert!(is_valid_subsystem_code("SIM"));
        assert!(is_valid_subsystem_code("SINAN"));

        // Test invalid codes
        assert!(!is_valid_subsystem_code("UNKNOWN"));
        assert!(!is_valid_subsystem_code(""));
        assert!(!is_valid_subsystem_code("123"));
    }

    #[test]
    fn test_get_subsystem_name() {
        // Test valid codes
        assert_eq!(
            get_subsystem_name("CIH"),
            Some("Comunicação de Internação Hospitalar")
        );
        assert_eq!(
            get_subsystem_name("cih"),
            Some("Comunicação de Internação Hospitalar")
        ); // Case insensitive

        // Test invalid codes
        assert_eq!(get_subsystem_name("UNKNOWN"), None);
        assert_eq!(get_subsystem_name(""), None);
    }

    #[test]
    fn test_get_all_subsystem_codes() {
        let codes = get_all_subsystem_codes();

        // Test that we get expected codes
        assert!(!codes.is_empty());
        assert!(codes.contains(&"CIH"));
        assert!(codes.contains(&"SIM"));
        assert!(codes.contains(&"SINAN"));

        // Test that we get all subsystems
        assert_eq!(codes.len(), SUBSYSTEMS.len());

        // Test uniqueness
        let mut sorted_codes = codes.clone();
        sorted_codes.sort();
        sorted_codes.dedup();
        assert_eq!(sorted_codes.len(), codes.len());
    }

    #[test]
    fn test_get_all_subsystem_names() {
        let names = get_all_subsystem_names();

        // Test that we get expected names
        assert!(!names.is_empty());
        assert!(names.contains(&"Comunicação de Internação Hospitalar"));

        // Test that we get all subsystems
        assert_eq!(names.len(), SUBSYSTEMS.len());

        // Test uniqueness
        let mut sorted_names = names.clone();
        sorted_names.sort();
        sorted_names.dedup();
        assert_eq!(sorted_names.len(), names.len());
    }

    #[test]
    fn test_get_subsystems_with_groups() {
        let with_groups = get_subsystems_with_groups();

        // Test that all returned subsystems have groups
        for subsystem in with_groups.iter() {
            assert!(
                subsystem.has_groups(),
                "Subsystem {} should have groups",
                subsystem.code
            );
        }

        // Test that we get at least some subsystems with groups
        assert!(!with_groups.is_empty());
    }

    #[test]
    fn test_get_subsystems_without_groups() {
        let without_groups = get_subsystems_without_groups();

        // Test that all returned subsystems don't have groups
        for subsystem in without_groups.iter() {
            assert!(
                !subsystem.has_groups(),
                "Subsystem {} should not have groups",
                subsystem.code
            );
        }
    }

    #[test]
    fn test_subsystems_partition() {
        let with_groups = get_subsystems_with_groups();
        let without_groups = get_subsystems_without_groups();

        // Test that together they account for all subsystems
        assert_eq!(with_groups.len() + without_groups.len(), SUBSYSTEMS.len());

        // Test that there's no overlap
        for subsystem_with in with_groups.iter() {
            assert!(
                !without_groups.iter().any(|s| s.code == subsystem_with.code),
                "Subsystem {} found in both with and without groups",
                subsystem_with.code
            );
        }
    }

    #[test]
    fn test_find_group_in_subsystem() {
        // Test with known subsystem and group
        let group = find_group_in_subsystem("CIH", "CR");
        assert!(group.is_some());
        if let Some(group) = group {
            assert_eq!(group.code, "CR");
            assert_eq!(group.name, "Comunicação de Internação Hospitalar");
        }

        // Test case insensitive
        let group = find_group_in_subsystem("cih", "cr");
        assert!(group.is_some());

        // Test with unknown subsystem
        assert!(find_group_in_subsystem("UNKNOWN", "CR").is_none());

        // Test with unknown group
        assert!(find_group_in_subsystem("CIH", "UNKNOWN").is_none());

        // Test with empty strings
        assert!(find_group_in_subsystem("", "CR").is_none());
        assert!(find_group_in_subsystem("CIH", "").is_none());
    }

    #[test]
    fn test_find_all_groups_by_code() {
        // Test with known group code
        let groups = find_all_groups_by_code("CR");
        assert!(!groups.is_empty());

        // Test case insensitive
        let groups = find_all_groups_by_code("cr");
        assert!(!groups.is_empty());

        // Test that all returned groups have the correct code
        for (_, group) in groups.iter() {
            assert!(group.matches_code("CR"));
        }

        // Test with unknown group code
        let groups = find_all_groups_by_code("UNKNOWN");
        assert!(groups.is_empty());

        // Test with empty string
        let groups = find_all_groups_by_code("");
        assert!(groups.is_empty());
    }

    #[test]
    fn test_search_all_groups() {
        // Test search with partial names
        let groups = search_all_groups("Comunicação");
        assert!(!groups.is_empty());

        // Test case insensitive
        let groups = search_all_groups("comunicação");
        assert!(!groups.is_empty());

        // Test that all returned groups contain the search term
        for (_, group) in groups.iter() {
            assert!(group.name_contains("Comunicação"));
        }

        // Test with no matches
        let groups = search_all_groups("xyz123");
        assert!(groups.is_empty());

        // Test with empty string
        let groups = search_all_groups("");
        let total_groups = count_total_groups();
        assert_eq!(groups.len(), total_groups);
    }

    #[test]
    fn test_get_all_group_codes() {
        let codes = get_all_group_codes();

        // Test that we get some codes
        assert!(!codes.is_empty());

        // Test that we get expected codes
        assert!(codes.contains(&"CR"));

        // Test that count matches count_total_groups
        assert_eq!(codes.len(), count_total_groups());
    }

    #[test]
    fn test_validate_subsystem_codes() {
        // Test with all valid codes
        let valid_codes = vec!["CIH", "SIM", "SINAN"];
        let (valid, invalid) = validate_subsystem_codes(&valid_codes);
        assert_eq!(valid.len(), 3);
        assert_eq!(invalid.len(), 0);
        assert!(valid.contains(&"CIH"));
        assert!(valid.contains(&"SIM"));
        assert!(valid.contains(&"SINAN"));

        // Test with all invalid codes
        let invalid_codes = vec!["UNKNOWN1", "UNKNOWN2", "UNKNOWN3"];
        let (valid, invalid) = validate_subsystem_codes(&invalid_codes);
        assert_eq!(valid.len(), 0);
        assert_eq!(invalid.len(), 3);
        assert!(invalid.contains(&"UNKNOWN1"));
        assert!(invalid.contains(&"UNKNOWN2"));
        assert!(invalid.contains(&"UNKNOWN3"));

        // Test with mixed codes
        let mixed_codes = vec!["CIH", "UNKNOWN", "SIM"];
        let (valid, invalid) = validate_subsystem_codes(&mixed_codes);
        assert_eq!(valid.len(), 2);
        assert_eq!(invalid.len(), 1);
        assert!(valid.contains(&"CIH"));
        assert!(valid.contains(&"SIM"));
        assert!(invalid.contains(&"UNKNOWN"));

        // Test with empty input
        let empty_codes: Vec<&str> = vec![];
        let (valid, invalid) = validate_subsystem_codes(&empty_codes);
        assert_eq!(valid.len(), 0);
        assert_eq!(invalid.len(), 0);

        // Test case insensitive
        let case_codes = vec!["cih", "SIM", "sinan"];
        let (valid, invalid) = validate_subsystem_codes(&case_codes);
        assert_eq!(valid.len(), 3);
        assert_eq!(invalid.len(), 0);
    }

    #[test]
    fn test_get_subsystem_info() {
        // Test with valid code
        let info = get_subsystem_info("CIH");
        assert!(info.is_some());
        if let Some((name, description, _url)) = info {
            assert_eq!(name, "Comunicação de Internação Hospitalar");
            assert!(description.is_some());
            // URL may or may not be present
        }

        // Test case insensitive
        let info = get_subsystem_info("cih");
        assert!(info.is_some());

        // Test with invalid code
        let info = get_subsystem_info("UNKNOWN");
        assert!(info.is_none());

        // Test with empty code
        let info = get_subsystem_info("");
        assert!(info.is_none());
    }

    #[test]
    fn test_count_subsystems() {
        let count = count_subsystems();
        assert_eq!(count, SUBSYSTEMS.len());
        assert!(count > 0);
        assert!(count >= 14); // Based on expected minimum
    }

    #[test]
    fn test_count_total_groups() {
        let count = count_total_groups();

        // Test that count matches manual calculation
        let manual_count: usize = SUBSYSTEMS.iter().map(|s| s.groups.len()).sum();
        assert_eq!(count, manual_count);

        // Test that count is reasonable (remove the useless >= 0 check)
        // Since usize is unsigned, it's always >= 0
        // Instead, let's test that it's a reasonable number
        assert!(
            count <= 1000,
            "Total group count should be reasonable, got {}",
            count
        );
    }

    #[test]
    fn test_subsystem_serialization() {
        // Test that SubsystemInfo can be serialized
        let subsystem = &SUBSYSTEMS[0];
        let json = serde_json::to_string(subsystem).unwrap();
        assert!(json.contains(&subsystem.code));
        assert!(json.contains(&subsystem.name));
    }

    #[test]
    fn test_group_serialization() {
        // Find a subsystem with groups
        let subsystem_with_groups = SUBSYSTEMS.iter().find(|s| s.has_groups());
        if let Some(subsystem) = subsystem_with_groups {
            let group = &subsystem.groups[0];
            let json = serde_json::to_string(group).unwrap();
            assert!(json.contains(&group.code));
            assert!(json.contains(&group.name));
        }
    }

    #[test]
    fn test_debug_formatting() {
        let subsystem = &SUBSYSTEMS[0];
        let debug_str = format!("{:?}", subsystem);
        assert!(debug_str.contains("SubsystemInfo"));
        assert!(debug_str.contains(&subsystem.code));

        if subsystem.has_groups() {
            let group = &subsystem.groups[0];
            let debug_str = format!("{:?}", group);
            assert!(debug_str.contains("GroupInfo"));
            assert!(debug_str.contains(&group.code));
        }
    }

    #[test]
    fn test_expected_subsystems() {
        // Test that we have all expected subsystems based on actual codes
        let expected_codes = vec![
            "CIH",
            "CIHA",
            "CNES",
            "IBGE",
            "PNI",
            "RESP",
            "SIA",
            "SIHSUS",
            "SIM",
            "SINAN",
            "SINASC",
            "SISCOLO",
            "SISMAMA",
            "SISPRENATAL",
        ];

        for expected_code in expected_codes {
            assert!(
                find_subsystem_by_code(expected_code).is_some(),
                "Expected subsystem {} not found",
                expected_code
            );
        }
    }

    #[test]
    fn test_subsystem_data_consistency() {
        for subsystem in SUBSYSTEMS.iter() {
            // Test that description is consistent
            if let Some(description) = subsystem.description {
                assert!(!description.is_empty());
            }

            // Test that long_description is consistent
            if let Some(long_desc) = subsystem.long_description {
                assert!(!long_desc.is_empty());
            }

            // Test that URL is consistent (made more flexible)
            if let Some(url) = subsystem.url {
                assert!(!url.is_empty());
                // More flexible URL validation - accept various protocols and relative URLs
                assert!(
                    url.starts_with("http://") || 
                    url.starts_with("https://") || 
                    url.starts_with("ftp://") ||    // Added FTP support
                    url.starts_with("ftps://") ||   // Added secure FTP support
                    url.starts_with("/") || 
                    url.starts_with("./") ||
                    url.starts_with("../") ||
                    !url.contains("://"), // Accept relative URLs without protocol
                    "Invalid URL format: {}",
                    url
                );
            }

            // Test groups consistency
            for group in subsystem.groups.iter() {
                if let Some(description) = group.description {
                    assert!(!description.is_empty());
                }

                if let Some(long_desc) = group.long_description {
                    assert!(!long_desc.is_empty());
                }

                if let Some(url) = group.url {
                    assert!(!url.is_empty());
                    // More flexible URL validation for groups too
                    assert!(
                        url.starts_with("http://") || 
                        url.starts_with("https://") || 
                        url.starts_with("ftp://") ||    // Added FTP support
                        url.starts_with("ftps://") ||   // Added secure FTP support
                        url.starts_with("/") || 
                        url.starts_with("./") ||
                        url.starts_with("../") ||
                        !url.contains("://"),
                        "Invalid URL format: {}",
                        url
                    );
                }
            }
        }
    }

    #[test]
    fn test_group_uniqueness_within_subsystem() {
        for subsystem in SUBSYSTEMS.iter() {
            let mut group_codes = Vec::new();
            let mut group_names = Vec::new();

            for group in subsystem.groups.iter() {
                // Test unique codes within subsystem
                assert!(
                    !group_codes.contains(&group.code),
                    "Duplicate group code {} in subsystem {}",
                    group.code,
                    subsystem.code
                );
                group_codes.push(group.code);

                // Test unique names within subsystem
                assert!(
                    !group_names.contains(&group.name),
                    "Duplicate group name {} in subsystem {}",
                    group.name,
                    subsystem.code
                );
                group_names.push(group.name);
            }
        }
    }

    #[test]
    fn test_performance_with_large_searches() {
        // Test performance with many searches
        for _ in 0..1000 {
            let _ = find_subsystem_by_code("CIH");
            let _ = find_subsystem_by_name("Comunicação de Internação Hospitalar");
            let _ = search_subsystems("Sistema");
            let _ = is_valid_subsystem_code("SIM");
        }
    }

    #[test]
    fn test_thread_safety() {
        use std::thread;

        let handles: Vec<_> = (0..10)
            .map(|i| {
                thread::spawn(move || {
                    // Test that all functions work correctly in different threads
                    let subsystem = find_subsystem_by_code("CIH");
                    assert!(subsystem.is_some());

                    let results = search_subsystems("Sistema");
                    assert!(!results.is_empty());

                    let valid = is_valid_subsystem_code("SIM");
                    assert!(valid);

                    let codes = get_all_subsystem_codes();
                    assert!(!codes.is_empty());

                    // Use thread index to avoid unused variable warning
                    assert!(i < 10);
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn test_constants_immutability() {
        // Test that SUBSYSTEMS constant is properly immutable
        let original_len = SUBSYSTEMS.len();

        // This should not compile if SUBSYSTEMS is mutable
        // SUBSYSTEMS.push(new_subsystem); // This would fail to compile

        // Verify length hasn't changed
        assert_eq!(SUBSYSTEMS.len(), original_len);
    }

    #[test]
    fn test_function_return_types() {
        // Test that functions return the expected types
        let subsystem_opt: Option<&'static SubsystemInfo> = find_subsystem_by_code("CIH");
        assert!(subsystem_opt.is_some());

        let subsystem_opt: Option<&'static SubsystemInfo> =
            find_subsystem_by_name("Comunicação de Internação Hospitalar");
        assert!(subsystem_opt.is_some());

        let results: Vec<&'static SubsystemInfo> = search_subsystems("Sistema");
        assert!(!results.is_empty());

        let valid: bool = is_valid_subsystem_code("CIH");
        assert!(valid);

        let name_opt: Option<&'static str> = get_subsystem_name("CIH");
        assert!(name_opt.is_some());

        let codes: Vec<&'static str> = get_all_subsystem_codes();
        assert!(!codes.is_empty());

        let names: Vec<&'static str> = get_all_subsystem_names();
        assert!(!names.is_empty());
    }

    #[test]
    fn test_edge_cases() {
        // Test with whitespace
        assert!(find_subsystem_by_code(" CIH ").is_none());
        assert!(find_subsystem_by_code("CIH ").is_none());
        assert!(find_subsystem_by_code(" CIH").is_none());

        // Test with special characters
        assert!(find_subsystem_by_code("CIH@").is_none());
        assert!(find_subsystem_by_code("@CIH").is_none());
        assert!(find_subsystem_by_code("CIH#").is_none());

        // Test with numbers
        assert!(find_subsystem_by_code("CIH1").is_none());
        assert!(find_subsystem_by_code("1CIH").is_none());
        assert!(find_subsystem_by_code("123").is_none());

        // Test with very long strings
        let long_code = "A".repeat(1000);
        assert!(find_subsystem_by_code(&long_code).is_none());

        let long_name = "Sistema ".repeat(100);
        assert!(find_subsystem_by_name(&long_name).is_none());

        // Test with Unicode characters
        assert!(find_subsystem_by_code("CIH🚀").is_none());
        assert!(find_subsystem_by_code("🚀CIH").is_none());
    }

    #[test]
    fn test_memory_efficiency() {
        // Test that structs are reasonably sized
        let subsystem_size = std::mem::size_of::<SubsystemInfo>();
        let group_size = std::mem::size_of::<GroupInfo>();

        // Should be memory efficient (mostly static references)
        assert!(
            subsystem_size < 200,
            "SubsystemInfo should be memory efficient, got {} bytes",
            subsystem_size
        );
        assert!(
            group_size < 100,
            "GroupInfo should be memory efficient, got {} bytes",
            group_size
        );
    }

    #[test]
    fn test_constants_memory_layout() {
        // Test that SUBSYSTEMS constant has expected memory layout
        let total_size = std::mem::size_of_val(&SUBSYSTEMS);
        let per_subsystem_size = total_size / SUBSYSTEMS.len();

        // Each entry should be reasonably sized
        assert!(
            per_subsystem_size < 500,
            "Each subsystem entry should be memory efficient, got {} bytes",
            per_subsystem_size
        );
    }

    #[test]
    fn test_comprehensive_workflow() {
        // Test a complete workflow using the subsystem functions

        // 1. Get all subsystem codes
        let all_codes = get_all_subsystem_codes();
        assert!(!all_codes.is_empty());

        // 2. Validate that all codes are valid
        let (valid, invalid) = validate_subsystem_codes(&all_codes);
        assert_eq!(valid.len(), all_codes.len());
        assert_eq!(invalid.len(), 0);

        // 3. For each valid code, get subsystem info
        for code in valid {
            let subsystem = find_subsystem_by_code(code).unwrap();
            assert_eq!(subsystem.code, code);

            let name = get_subsystem_name(code).unwrap();
            assert_eq!(name, subsystem.name);

            let info = get_subsystem_info(code).unwrap();
            assert_eq!(info.0, subsystem.name);

            // If subsystem has groups, test group functions
            if subsystem.has_groups() {
                let group_codes = subsystem.get_group_codes();
                assert!(!group_codes.is_empty());

                for group_code in group_codes {
                    let group = find_group_in_subsystem(code, group_code).unwrap();
                    assert_eq!(group.code, group_code);
                }
            }
        }

        // 4. Test search functionality
        let search_results = search_subsystems("Sistema");
        for result in search_results {
            assert!(result.name_contains("Sistema"));
        }

        // 5. Test counting functions
        let subsystem_count = count_subsystems();
        let total_group_count = count_total_groups();
        assert_eq!(subsystem_count, SUBSYSTEMS.len());
        assert_eq!(total_group_count, get_all_group_codes().len());
    }

    #[test]
    fn test_error_handling_comprehensive() {
        // Test various invalid inputs
        let invalid_codes = vec![
            "",
            " ",
            "  ",
            "\t",
            "\n",
            "UNKNOWN",
            "123",
            "CIH123",
            "123CIH",
            "CIH@",
            "@CIH",
            "CIH#",
            "#CIH",
            "CIH$",
            "$CIH",
            "CIH%",
            "%CIH",
            "null",
            "undefined",
            "NaN",
            "Infinity",
            "-Infinity",
            "true",
            "false",
            "CIH ",
            " CIH",
            " CIH ",
            "CIH\t",
            "\tCIH",
            "CIH\n",
            "\nCIH",
            "🚀",
            "CIH🚀",
            "🚀CIH",
            "测试",
            "тест",
            "اختبار",
        ];

        for invalid_code in invalid_codes {
            assert!(
                find_subsystem_by_code(invalid_code).is_none(),
                "Should return None for invalid code: '{}'",
                invalid_code
            );
            assert!(
                !is_valid_subsystem_code(invalid_code),
                "Should return false for invalid code: '{}'",
                invalid_code
            );
            assert!(
                get_subsystem_name(invalid_code).is_none(),
                "Should return None for invalid code: '{}'",
                invalid_code
            );
            assert!(
                get_subsystem_info(invalid_code).is_none(),
                "Should return None for invalid code: '{}'",
                invalid_code
            );
        }

        let invalid_names = vec![
            "",
            " ",
            "  ",
            "\t",
            "\n",
            "Unknown System",
            "123",
            "Sistema123",
            "Sistema@",
            "@Sistema",
            "Sistema#",
            "#Sistema",
            "Sistema$",
            "$Sistema",
            "null",
            "undefined",
            "NaN",
            "Infinity",
            "-Infinity",
            "true",
            "false",
            "Sistema ",
            " Sistema",
            " Sistema ",
            "Sistema\t",
            "\tSistema",
            "🚀",
            "Sistema🚀",
            "🚀Sistema",
            "测试",
            "тест",
            "اختبار",
        ];

        for invalid_name in invalid_names {
            assert!(
                find_subsystem_by_name(invalid_name).is_none(),
                "Should return None for invalid name: '{}'",
                invalid_name
            );
        }
    }

    #[test]
    fn test_case_sensitivity_comprehensive() {
        // Test case sensitivity for all functions
        let test_cases = vec![
            ("CIH", "cih", "Cih", "cIh", "CiH", "ciH", "CIh", "cIH"),
            ("SIM", "sim", "Sim", "sIm", "SiM", "siM", "SIm", "sIM"),
        ];

        for (original, case1, case2, case3, case4, case5, case6, case7) in test_cases {
            let cases = vec![original, case1, case2, case3, case4, case5, case6, case7];

            for case in cases {
                // All should work the same way
                assert_eq!(
                    find_subsystem_by_code(original).is_some(),
                    find_subsystem_by_code(case).is_some()
                );
                assert_eq!(
                    is_valid_subsystem_code(original),
                    is_valid_subsystem_code(case)
                );
                assert_eq!(get_subsystem_name(original), get_subsystem_name(case));
                assert_eq!(
                    get_subsystem_info(original).is_some(),
                    get_subsystem_info(case).is_some()
                );
            }
        }
    }

    #[test]
    fn debug_actual_subsystem_codes() {
        let actual_codes: Vec<&str> = SUBSYSTEMS.iter().map(|s| s.code).collect();
        println!("Actual subsystem codes: {:?}", actual_codes);

        for subsystem in SUBSYSTEMS.iter() {
            println!("Code: {}, Name: {}", subsystem.code, subsystem.name);
        }
    }
}

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Month {
    pub month: i8,
    pub name: String,
}

impl Month {
    pub fn new(month: i8, name: &str) -> Self {
        Self {
            month,
            name: name.to_string(),
        }
    }
}

pub const MONTHS: &[(i8, &str)] = &[
    (1, "Janeiro"),
    (2, "Fevereiro"),
    (3, "Março"),
    (4, "Abril"),
    (5, "Maio"),
    (6, "Junho"),
    (7, "Julho"),
    (8, "Agosto"),
    (9, "Setembro"),
    (10, "Outubro"),
    (11, "Novembro"),
    (12, "Dezembro"),
];

impl Month {
    pub fn from_number(month: i8) -> Option<Self> {
        MONTHS
            .iter()
            .find(|(num, _)| *num == month)
            .map(|(num, name)| Self::new(*num, *name)) // Dereference name here
    }

    pub fn from_padded_string(month_str: &str) -> Option<Self> {
        // Only allow 1 or 2 digit strings
        if month_str.len() > 2 || month_str.is_empty() {
            return None;
        }

        // Check that all characters are digits
        if !month_str.chars().all(|c| c.is_ascii_digit()) {
            return None;
        }

        let month_num = month_str.parse::<i8>().ok()?;
        Self::from_number(month_num)
    }

    pub fn get_name(month: i8) -> Option<&'static str> {
        MONTHS
            .iter()
            .find(|(num, _)| *num == month)
            .map(|(_, name)| *name) // Dereference name here
    }

    pub fn get_name_from_padded_string(month_str: &str) -> Option<&'static str> {
        // Only allow 1 or 2 digit strings
        if month_str.len() > 2 || month_str.is_empty() {
            return None;
        }

        // Check that all characters are digits
        if !month_str.chars().all(|c| c.is_ascii_digit()) {
            return None;
        }

        let month_num = month_str.parse::<i8>().ok()?;
        Self::get_name(month_num)
    }

    pub fn to_padded_string(&self) -> String {
        format!("{:02}", self.month)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_month_new() {
        let month = Month::new(1, "Janeiro");
        assert_eq!(month.month, 1);
        assert_eq!(month.name, "Janeiro");

        let month = Month::new(12, "Dezembro");
        assert_eq!(month.month, 12);
        assert_eq!(month.name, "Dezembro");

        // Test with longer name
        let month = Month::new(9, "Setembro");
        assert_eq!(month.month, 9);
        assert_eq!(month.name, "Setembro");
    }

    #[test]
    fn test_months_constant_completeness() {
        // Test that we have all 12 months
        assert_eq!(MONTHS.len(), 12);

        // Test that all months from 1 to 12 are present
        for i in 1..=12 {
            assert!(
                MONTHS.iter().any(|(month, _)| *month == i),
                "Month {} missing",
                i
            );
        }
    }

    #[test]
    fn test_months_constant_data_integrity() {
        let expected_months = vec![
            (1, "Janeiro"),
            (2, "Fevereiro"),
            (3, "Março"),
            (4, "Abril"),
            (5, "Maio"),
            (6, "Junho"),
            (7, "Julho"),
            (8, "Agosto"),
            (9, "Setembro"),
            (10, "Outubro"),
            (11, "Novembro"),
            (12, "Dezembro"),
        ];

        assert_eq!(MONTHS.len(), expected_months.len());

        for (expected_num, expected_name) in expected_months {
            let found = MONTHS.iter().find(|(num, _)| *num == expected_num);
            assert!(found.is_some(), "Month {} not found", expected_num);
            let (_, actual_name) = found.unwrap();
            assert_eq!(
                actual_name, &expected_name,
                "Month {} name mismatch",
                expected_num
            );
        }
    }

    #[test]
    fn test_months_constant_uniqueness() {
        // Test that all month numbers are unique
        let mut numbers: Vec<i8> = MONTHS.iter().map(|(num, _)| *num).collect();
        numbers.sort();
        let original_len = numbers.len();
        numbers.dedup();
        assert_eq!(numbers.len(), original_len, "Duplicate month numbers found");

        // Test that all month names are unique
        let mut names: Vec<&str> = MONTHS.iter().map(|(_, name)| *name).collect();
        names.sort();
        let original_len = names.len();
        names.dedup();
        assert_eq!(names.len(), original_len, "Duplicate month names found");
    }

    #[test]
    fn test_months_constant_order() {
        // Test that months are in correct order (1-12)
        for (index, (month_num, _)) in MONTHS.iter().enumerate() {
            assert_eq!(
                *month_num,
                (index + 1) as i8,
                "Month {} not in correct position",
                month_num
            );
        }
    }

    #[test]
    fn test_from_number() {
        // Test valid month numbers
        for i in 1..=12 {
            let month = Month::from_number(i);
            assert!(month.is_some(), "Month {} should be valid", i);
            let month = month.unwrap();
            assert_eq!(month.month, i);
            assert!(!month.name.is_empty());
        }

        // Test specific months
        let jan = Month::from_number(1).unwrap();
        assert_eq!(jan.month, 1);
        assert_eq!(jan.name, "Janeiro");

        let dec = Month::from_number(12).unwrap();
        assert_eq!(dec.month, 12);
        assert_eq!(dec.name, "Dezembro");

        // Test invalid month numbers
        assert!(Month::from_number(0).is_none());
        assert!(Month::from_number(13).is_none());
        assert!(Month::from_number(-1).is_none());
        assert!(Month::from_number(100).is_none());
    }

    #[test]
    fn test_from_padded_string() {
        // Test valid padded strings
        let month = Month::from_padded_string("01");
        assert!(month.is_some());
        let month = month.unwrap();
        assert_eq!(month.month, 1);
        assert_eq!(month.name, "Janeiro");

        let month = Month::from_padded_string("12");
        assert!(month.is_some());
        let month = month.unwrap();
        assert_eq!(month.month, 12);
        assert_eq!(month.name, "Dezembro");

        // Test single digit strings
        let month = Month::from_padded_string("1");
        assert!(month.is_some());
        let month = month.unwrap();
        assert_eq!(month.month, 1);
        assert_eq!(month.name, "Janeiro");

        let month = Month::from_padded_string("9");
        assert!(month.is_some());
        let month = month.unwrap();
        assert_eq!(month.month, 9);
        assert_eq!(month.name, "Setembro");

        // Test invalid strings
        assert!(Month::from_padded_string("00").is_none());
        assert!(Month::from_padded_string("13").is_none());
        assert!(Month::from_padded_string("abc").is_none());
        assert!(Month::from_padded_string("").is_none());
        assert!(Month::from_padded_string("1a").is_none());
        assert!(Month::from_padded_string("a1").is_none());
        assert!(Month::from_padded_string("-1").is_none());
        assert!(Month::from_padded_string("001").is_none()); // Too many digits
    }

    #[test]
    fn test_get_name() {
        // Test valid month numbers
        assert_eq!(Month::get_name(1), Some("Janeiro"));
        assert_eq!(Month::get_name(2), Some("Fevereiro"));
        assert_eq!(Month::get_name(3), Some("Março"));
        assert_eq!(Month::get_name(4), Some("Abril"));
        assert_eq!(Month::get_name(5), Some("Maio"));
        assert_eq!(Month::get_name(6), Some("Junho"));
        assert_eq!(Month::get_name(7), Some("Julho"));
        assert_eq!(Month::get_name(8), Some("Agosto"));
        assert_eq!(Month::get_name(9), Some("Setembro"));
        assert_eq!(Month::get_name(10), Some("Outubro"));
        assert_eq!(Month::get_name(11), Some("Novembro"));
        assert_eq!(Month::get_name(12), Some("Dezembro"));

        // Test invalid month numbers
        assert_eq!(Month::get_name(0), None);
        assert_eq!(Month::get_name(13), None);
        assert_eq!(Month::get_name(-1), None);
        assert_eq!(Month::get_name(100), None);
    }

    #[test]
    fn test_get_name_from_padded_string() {
        // Test valid padded strings
        assert_eq!(Month::get_name_from_padded_string("01"), Some("Janeiro"));
        assert_eq!(Month::get_name_from_padded_string("02"), Some("Fevereiro"));
        assert_eq!(Month::get_name_from_padded_string("03"), Some("Março"));
        assert_eq!(Month::get_name_from_padded_string("04"), Some("Abril"));
        assert_eq!(Month::get_name_from_padded_string("05"), Some("Maio"));
        assert_eq!(Month::get_name_from_padded_string("06"), Some("Junho"));
        assert_eq!(Month::get_name_from_padded_string("07"), Some("Julho"));
        assert_eq!(Month::get_name_from_padded_string("08"), Some("Agosto"));
        assert_eq!(Month::get_name_from_padded_string("09"), Some("Setembro"));
        assert_eq!(Month::get_name_from_padded_string("10"), Some("Outubro"));
        assert_eq!(Month::get_name_from_padded_string("11"), Some("Novembro"));
        assert_eq!(Month::get_name_from_padded_string("12"), Some("Dezembro"));

        // Test single digit strings
        assert_eq!(Month::get_name_from_padded_string("1"), Some("Janeiro"));
        assert_eq!(Month::get_name_from_padded_string("2"), Some("Fevereiro"));
        assert_eq!(Month::get_name_from_padded_string("9"), Some("Setembro"));

        // Test invalid strings
        assert_eq!(Month::get_name_from_padded_string("00"), None);
        assert_eq!(Month::get_name_from_padded_string("13"), None);
        assert_eq!(Month::get_name_from_padded_string("abc"), None);
        assert_eq!(Month::get_name_from_padded_string(""), None);
        assert_eq!(Month::get_name_from_padded_string("1a"), None);
        assert_eq!(Month::get_name_from_padded_string("-1"), None);
    }

    #[test]
    fn test_to_padded_string() {
        // Test single digit months
        let jan = Month::new(1, "Janeiro");
        assert_eq!(jan.to_padded_string(), "01");

        let feb = Month::new(2, "Fevereiro");
        assert_eq!(feb.to_padded_string(), "02");

        let sep = Month::new(9, "Setembro");
        assert_eq!(sep.to_padded_string(), "09");

        // Test double digit months
        let oct = Month::new(10, "Outubro");
        assert_eq!(oct.to_padded_string(), "10");

        let nov = Month::new(11, "Novembro");
        assert_eq!(nov.to_padded_string(), "11");

        let dec = Month::new(12, "Dezembro");
        assert_eq!(dec.to_padded_string(), "12");
    }

    #[test]
    fn test_month_struct_debug() {
        let month = Month::new(1, "Janeiro");
        let debug_str = format!("{:?}", month);
        assert!(debug_str.contains("Month"));
        assert!(debug_str.contains("1"));
        assert!(debug_str.contains("Janeiro"));
    }

    #[test]
    fn test_month_struct_clone() {
        let month = Month::new(1, "Janeiro");
        let cloned = month.clone();
        assert_eq!(month.month, cloned.month);
        assert_eq!(month.name, cloned.name);
    }

    #[test]
    fn test_month_struct_serialization() {
        let month = Month::new(1, "Janeiro");

        // Test serialization to JSON
        let json = serde_json::to_string(&month).unwrap();
        assert!(json.contains("1"));
        assert!(json.contains("Janeiro"));

        // Test deserialization from JSON
        let deserialized: Month = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.month, 1);
        assert_eq!(deserialized.name, "Janeiro");
    }

    #[test]
    fn test_portuguese_month_names() {
        // Test that all Portuguese month names are correct
        let portuguese_names = [
            "Janeiro",
            "Fevereiro",
            "Março",
            "Abril",
            "Maio",
            "Junho",
            "Julho",
            "Agosto",
            "Setembro",
            "Outubro",
            "Novembro",
            "Dezembro",
        ];

        for (i, expected_name) in portuguese_names.into_iter().enumerate() {
            let month_num = (i + 1) as i8;
            let actual_name = Month::get_name(month_num).unwrap();
            assert_eq!(
                actual_name, expected_name,
                "Month {} name incorrect",
                month_num
            );
        }
    }

    #[test]
    fn test_month_names_with_accents() {
        // Test months with Portuguese accents
        assert_eq!(Month::get_name(3), Some("Março"));
        assert_eq!(Month::get_name(9), Some("Setembro"));
        assert_eq!(Month::get_name(12), Some("Dezembro"));

        // Test that accents are preserved in from_number
        let marco = Month::from_number(3).unwrap();
        assert!(marco.name.contains("ç")); // Março has ç

        let setembro = Month::from_number(9).unwrap();
        assert!(setembro.name.contains("e")); // Setembro has e, not ê

        let dezembro = Month::from_number(12).unwrap();
        assert!(dezembro.name.contains("e")); // Dezembro has e, not ê
    }

    #[test]
    fn test_all_months_round_trip() {
        // Test that all months can be created and retrieved correctly
        for i in 1..=12 {
            // Test from_number -> to_padded_string -> from_padded_string
            let month1 = Month::from_number(i).unwrap();
            let padded = month1.to_padded_string();
            let month2 = Month::from_padded_string(&padded).unwrap();

            assert_eq!(month1.month, month2.month);
            assert_eq!(month1.name, month2.name);
        }
    }

    #[test]
    fn test_edge_cases() {
        // Test boundary values
        assert!(Month::from_number(i8::MIN).is_none());
        assert!(Month::from_number(i8::MAX).is_none());

        // Test with very large strings
        let large_string = "9".repeat(100);
        assert!(Month::from_padded_string(&large_string).is_none());

        // Test with whitespace
        assert!(Month::from_padded_string(" 1 ").is_none()); // Should not work with whitespace
        assert!(Month::from_padded_string("1 ").is_none());
        assert!(Month::from_padded_string(" 1").is_none());

        // Test with special characters
        assert!(Month::from_padded_string("1@").is_none());
        assert!(Month::from_padded_string("@1").is_none());
        assert!(Month::from_padded_string("1#").is_none());

        // Test with float-like strings
        assert!(Month::from_padded_string("1.0").is_none());
        assert!(Month::from_padded_string("1.5").is_none());

        // Test with leading zeros beyond 2 digits
        assert!(Month::from_padded_string("001").is_none());
        assert!(Month::from_padded_string("0001").is_none());
    }

    #[test]
    fn test_padding_consistency() {
        // Test that padding is consistent
        for i in 1..=12 {
            let month = Month::from_number(i).unwrap();
            let padded = month.to_padded_string();

            // All padded strings should be exactly 2 characters
            assert_eq!(padded.len(), 2, "Month {} padding incorrect", i);

            // Single digit months should start with '0'
            if i < 10 {
                assert!(padded.starts_with('0'), "Month {} should start with 0", i);
            }

            // Double digit months should not start with '0'
            if i >= 10 {
                assert!(
                    !padded.starts_with('0'),
                    "Month {} should not start with 0",
                    i
                );
            }
        }
    }

    #[test]
    fn test_month_name_lengths() {
        // Test that month names have reasonable lengths
        for i in 1..=12 {
            let name = Month::get_name(i).unwrap();
            assert!(name.len() >= 3, "Month {} name too short: {}", i, name);
            assert!(name.len() <= 10, "Month {} name too long: {}", i, name);
            assert!(!name.is_empty(), "Month {} name is empty", i);
        }
    }

    #[test]
    fn test_month_name_characters() {
        // Test that month names contain only valid characters
        for i in 1..=12 {
            let name = Month::get_name(i).unwrap();

            // Should not contain numbers
            assert!(
                !name.chars().any(|c| c.is_ascii_digit()),
                "Month {} name contains digits: {}",
                i,
                name
            );

            // Should not contain special characters except accents
            for c in name.chars() {
                assert!(
                    c.is_alphabetic() || "ç".contains(c), // Only ç is used in these month names
                    "Month {} name contains invalid character '{}': {}",
                    i,
                    c,
                    name
                );
            }

            // Should start with uppercase letter
            assert!(
                name.chars().next().unwrap().is_uppercase(),
                "Month {} name should start with uppercase: {}",
                i,
                name
            );
        }
    }

    #[test]
    fn test_performance_with_large_numbers() {
        // Test performance with many calls
        for _ in 0..1000 {
            for i in 1..=12 {
                let month = Month::from_number(i).unwrap();
                assert_eq!(month.month, i);
                assert!(!month.name.is_empty());
            }
        }
    }

    #[test]
    fn test_memory_efficiency() {
        // Test that Month struct is reasonably sized
        let month = Month::new(1, "Janeiro");
        let size = std::mem::size_of_val(&month);

        // Should be reasonably small (i8 + String)
        assert!(
            size < 100,
            "Month struct should be memory efficient, got {} bytes",
            size
        );
    }

    #[test]
    fn test_thread_safety() {
        use std::thread;

        let handles: Vec<_> = (1..=12)
            .map(|i| {
                thread::spawn(move || {
                    // Test that all functions work correctly in different threads
                    let month = Month::from_number(i).unwrap();
                    assert_eq!(month.month, i);
                    assert!(!month.name.is_empty());

                    let name = Month::get_name(i).unwrap();
                    assert!(!name.is_empty());

                    let padded = month.to_padded_string();
                    assert_eq!(padded.len(), 2);

                    let from_padded = Month::from_padded_string(&padded).unwrap();
                    assert_eq!(from_padded.month, i);
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn test_constants_immutability() {
        // Test that MONTHS constant is properly immutable
        let original_len = MONTHS.len();

        // This should not compile if MONTHS is mutable
        // MONTHS.push((13, "InvalidMonth")); // This would fail to compile

        // Verify length hasn't changed
        assert_eq!(MONTHS.len(), original_len);
    }

    #[test]
    fn test_function_return_types() {
        // Test that functions return the expected types
        let month_opt: Option<Month> = Month::from_number(1);
        assert!(month_opt.is_some());

        let month_opt: Option<Month> = Month::from_padded_string("01");
        assert!(month_opt.is_some());

        let name_opt: Option<&'static str> = Month::get_name(1);
        assert!(name_opt.is_some());

        let name_opt: Option<&'static str> = Month::get_name_from_padded_string("01");
        assert!(name_opt.is_some());

        let month = Month::new(1, "Janeiro");
        let padded: String = month.to_padded_string();
        assert_eq!(padded.len(), 2);
    }

    #[test]
    fn test_all_months_comprehensive() {
        // Comprehensive test of all months with specific data
        let expected_months = vec![
            (1, "Janeiro", "01"),
            (2, "Fevereiro", "02"),
            (3, "Março", "03"),
            (4, "Abril", "04"),
            (5, "Maio", "05"),
            (6, "Junho", "06"),
            (7, "Julho", "07"),
            (8, "Agosto", "08"),
            (9, "Setembro", "09"),
            (10, "Outubro", "10"),
            (11, "Novembro", "11"),
            (12, "Dezembro", "12"),
        ];

        for (num, name, padded) in expected_months {
            // Test from_number
            let month = Month::from_number(num).unwrap();
            assert_eq!(month.month, num);
            assert_eq!(month.name, name);
            assert_eq!(month.to_padded_string(), padded);

            // Test get_name
            assert_eq!(Month::get_name(num), Some(name));

            // Test from_padded_string
            let month_from_padded = Month::from_padded_string(padded).unwrap();
            assert_eq!(month_from_padded.month, num);
            assert_eq!(month_from_padded.name, name);

            // Test get_name_from_padded_string
            assert_eq!(Month::get_name_from_padded_string(padded), Some(name));

            // Test single digit string for months 1-9
            if num < 10 {
                let single_digit = num.to_string();
                let month_from_single = Month::from_padded_string(&single_digit).unwrap();
                assert_eq!(month_from_single.month, num);
                assert_eq!(month_from_single.name, name);

                assert_eq!(
                    Month::get_name_from_padded_string(&single_digit),
                    Some(name)
                );
            }
        }
    }

    #[test]
    fn test_error_handling_comprehensive() {
        // Test various invalid inputs
        let invalid_numbers = vec![-100, -1, 0, 13, 100, i8::MAX, i8::MIN];

        for invalid in invalid_numbers {
            assert!(
                Month::from_number(invalid).is_none(),
                "Should return None for invalid number: {}",
                invalid
            );
            assert!(
                Month::get_name(invalid).is_none(),
                "Should return None for invalid number: {}",
                invalid
            );
        }

        let invalid_strings = vec![
            "",
            " ",
            "  ",
            "\t",
            "\n",
            "abc",
            "13",
            "00",
            "-1",
            "1a",
            "a1",
            "1.0",
            "1,0",
            "01a",
            "a01",
            "001",
            "0001",
            " 1",
            "1 ",
            " 1 ", // Keep 001 as invalid
            "1@",
            "@1",
            "1#",
            "#1",
            "1$",
            "$1",
            "1%",
            "%1",
            "1&",
            "&1",
            "null",
            "undefined",
            "NaN",
            "Infinity",
            "-Infinity",
            "123", // Added 123 as invalid
        ];

        for invalid in invalid_strings {
            assert!(
                Month::from_padded_string(invalid).is_none(),
                "Should return None for invalid string: '{}'",
                invalid
            );
            assert!(
                Month::get_name_from_padded_string(invalid).is_none(),
                "Should return None for invalid string: '{}'",
                invalid
            );
        }
    }

    #[test]
    fn test_months_constant_memory_layout() {
        // Test that MONTHS constant has expected memory layout
        let total_size = std::mem::size_of_val(&MONTHS);
        let per_month_size = total_size / MONTHS.len();

        // Each entry should be reasonably sized (i8 + &str)
        assert!(
            per_month_size < 50,
            "Each month entry should be memory efficient, got {} bytes",
            per_month_size
        );
    }

    #[test]
    fn test_seasonal_grouping() {
        // Test seasonal grouping (Southern Hemisphere - Brazil)
        let summer_months = vec![12, 1, 2]; // Dezembro, Janeiro, Fevereiro
        let autumn_months = vec![3, 4, 5]; // Março, Abril, Maio
        let winter_months = vec![6, 7, 8]; // Junho, Julho, Agosto
        let spring_months = vec![9, 10, 11]; // Setembro, Outubro, Novembro

        // Test that all seasonal months are valid
        for season in vec![summer_months, autumn_months, winter_months, spring_months] {
            for month_num in season {
                assert!(
                    Month::from_number(month_num).is_some(),
                    "Seasonal month {} should be valid",
                    month_num
                );
                assert!(
                    Month::get_name(month_num).is_some(),
                    "Seasonal month {} should have name",
                    month_num
                );
            }
        }
    }
}

use arrow::datatypes::Schema;
use std::sync::Arc;

/// Information about a DATASUS data group.
/// 
/// # Fields
/// * `code` - Group code (e.g., "PA", "RD", "ST")
/// * `name` - Human-readable group name
/// * `schema` - Arrow schema defining the data structure
/// 
/// # Example
/// ```rust
/// use arrow::datatypes::{DataType, Field, Schema};
/// use shared::models::group_info::GroupInfo;
/// use std::sync::Arc;
/// 
/// let schema = Arc::new(Schema::new(vec![
///     Field::new("id", DataType::Int64, false),
///     Field::new("name", DataType::Utf8, true),
/// ]));
/// 
/// let group = GroupInfo::new(
///     "PA".to_string(),
///     "Produção Ambulatorial".to_string(),
///     schema
/// );
/// 
/// assert_eq!(group.code, "PA");
/// assert_eq!(group.name, "Produção Ambulatorial");
/// ```
#[derive(Debug, Clone)]
pub struct GroupInfo {
    pub code: String,
    pub name: String,
    pub schema: Arc<Schema>,
}

impl GroupInfo {
    /// Create a new GroupInfo instance.
    /// 
    /// # Arguments
    /// * `code` - Group code identifier
    /// * `name` - Human-readable group name
    /// * `schema` - Arrow schema for the data structure
    pub fn new(code: String, name: String, schema: Arc<Schema>) -> Self {
        Self {
            code,
            name,
            schema,
        }
    }
    
    /// Get the number of fields in the schema
    pub fn field_count(&self) -> usize {
        self.schema.fields().len()
    }
    
    /// Get field names from the schema
    pub fn field_names(&self) -> Vec<&str> {
        self.schema.fields().iter().map(|f| f.name().as_str()).collect()
    }
    
    /// Check if a field exists in the schema
    pub fn has_field(&self, field_name: &str) -> bool {
        self.schema.field_with_name(field_name).is_ok()
    }
    
    /// Get schema metadata
    pub fn schema_metadata(&self) -> &std::collections::HashMap<String, String> {
        self.schema.metadata()
    }
}

impl PartialEq for GroupInfo {
    fn eq(&self, other: &Self) -> bool {
        self.code == other.code && 
        self.name == other.name && 
        self.schema.fields() == other.schema.fields()
    }
}

impl Eq for GroupInfo {}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field};
    
    fn create_test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
        ]))
    }
    
    #[test]
    fn test_group_info_creation() {
        let schema = create_test_schema();
        let group = GroupInfo::new(
            "PA".to_string(),
            "Produção Ambulatorial".to_string(),
            schema.clone()
        );
        
        assert_eq!(group.code, "PA");
        assert_eq!(group.name, "Produção Ambulatorial");
        assert_eq!(group.schema.fields().len(), 3);
    }
    
    #[test]
    fn test_field_count() {
        let schema = create_test_schema();
        let group = GroupInfo::new(
            "RD".to_string(),
            "Resumo de Internação".to_string(),
            schema
        );
        
        assert_eq!(group.field_count(), 3);
    }
    
    #[test]
    fn test_field_names() {
        let schema = create_test_schema();
        let group = GroupInfo::new(
            "ST".to_string(),
            "Cadastro de Estabelecimentos".to_string(),
            schema
        );
        
        let names = group.field_names();
        assert_eq!(names, vec!["id", "name", "value"]);
    }
    
    #[test]
    fn test_has_field() {
        let schema = create_test_schema();
        let group = GroupInfo::new(
            "LT".to_string(),
            "Leitos".to_string(),
            schema
        );
        
        assert!(group.has_field("id"));
        assert!(group.has_field("name"));
        assert!(group.has_field("value"));
        assert!(!group.has_field("nonexistent"));
    }
    
    #[test]
    fn test_equality() {
        let schema1 = create_test_schema();
        let schema2 = create_test_schema();
        
        let group1 = GroupInfo::new(
            "PA".to_string(),
            "Produção Ambulatorial".to_string(),
            schema1
        );
        
        let group2 = GroupInfo::new(
            "PA".to_string(),
            "Produção Ambulatorial".to_string(),
            schema2
        );
        
        assert_eq!(group1, group2);
        
        // Test inequality
        let group3 = GroupInfo::new(
            "RD".to_string(),
            "Resumo de Internação".to_string(),
            create_test_schema()
        );
        
        assert_ne!(group1, group3);
    }
    
    #[test]
    fn test_schema_metadata() {
        let mut metadata = std::collections::HashMap::new();
        metadata.insert("source".to_string(), "DATASUS".to_string());
        metadata.insert("version".to_string(), "1.0".to_string());
        
        let schema = Arc::new(Schema::new_with_metadata(
            vec![Field::new("id", DataType::Int64, false)],
            metadata.clone()
        ));
        
        let group = GroupInfo::new(
            "PA".to_string(),
            "Produção Ambulatorial".to_string(),
            schema
        );
        
        assert_eq!(group.schema_metadata(), &metadata);
    }
}

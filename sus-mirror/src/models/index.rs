use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use tokio::fs;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataIndex {
    pub data: HashMap<String, DatasetInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetInfo {
    pub name: String,
    pub source: String,
    pub n_files: u32,
    pub total_size: u64,
    pub partition_n_ufs: u32,
    pub partition_period_start: Option<String>,
    pub partition_period_end: Option<String>,
    pub partition_n_periods: u32,
    pub partition_periodicity: Option<String>,
    pub latest_update: String, // ISO datetime format
}

impl DataIndex {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    pub fn add_dataset(&mut self, key: String, dataset: DatasetInfo) {
        self.data.insert(key, dataset);
    }

    pub fn get_dataset(&self, key: &str) -> Option<&DatasetInfo> {
        self.data.get(key)
    }

    pub fn get_datasets_by_source(&self, source: &str) -> Vec<(&String, &DatasetInfo)> {
        self.data
            .iter()
            .filter(|(_, dataset)| dataset.source == source)
            .collect()
    }

    pub fn get_all_sources(&self) -> Vec<String> {
        let mut sources: Vec<String> = self
            .data
            .values()
            .map(|dataset| dataset.source.clone())
            .collect();
        sources.sort();
        sources.dedup();
        sources
    }

    pub fn get_dataset_keys(&self) -> Vec<&String> {
        self.data.keys().collect()
    }

    pub fn count_datasets(&self) -> usize {
        self.data.len()
    }

    pub fn total_files(&self) -> u32 {
        self.data.values().map(|dataset| dataset.n_files).sum()
    }

    pub fn total_size(&self) -> u64 {
        self.data.values().map(|dataset| dataset.total_size).sum()
    }

    pub fn get_datasets_by_periodicity(&self, periodicity: &str) -> Vec<(&String, &DatasetInfo)> {
        self.data
            .iter()
            .filter(|(_, dataset)| {
                dataset
                    .partition_periodicity
                    .as_ref()
                    .map_or(false, |p| p == periodicity)
            })
            .collect()
    }

    pub fn get_largest_datasets(&self, limit: usize) -> Vec<(&String, &DatasetInfo)> {
        let mut datasets: Vec<(&String, &DatasetInfo)> = self.data.iter().collect();
        datasets.sort_by(|a, b| b.1.total_size.cmp(&a.1.total_size));
        datasets.into_iter().take(limit).collect()
    }

    pub fn get_most_recent_updates(&self, limit: usize) -> Vec<(&String, &DatasetInfo)> {
        let mut datasets: Vec<(&String, &DatasetInfo)> = self.data.iter().collect();
        datasets.sort_by(|a, b| b.1.latest_update.cmp(&a.1.latest_update));
        datasets.into_iter().take(limit).collect()
    }

    // Async methods
    pub async fn load_from_file(path: &Path) -> tokio::io::Result<Self> {
        let content = fs::read_to_string(path).await?;
        serde_json::from_str(&content)
            .map_err(|e| tokio::io::Error::new(tokio::io::ErrorKind::InvalidData, e))
    }

    pub async fn save_to_file(&self, path: &Path) -> tokio::io::Result<()> {
        let json = serde_json::to_string_pretty(self)
            .map_err(|e| tokio::io::Error::new(tokio::io::ErrorKind::InvalidData, e))?;
        fs::write(path, json).await
    }

    pub async fn load_from_url(
        url: &str,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let response = reqwest::get(url).await?;
        let content = response.text().await?;
        let index = serde_json::from_str(&content)?;
        Ok(index)
    }

    pub async fn merge_with(&mut self, other: DataIndex) {
        for (key, dataset) in other.data {
            self.data.insert(key, dataset);
        }
    }

    pub async fn update_dataset(&mut self, key: &str, dataset: DatasetInfo) -> Option<DatasetInfo> {
        self.data.insert(key.to_string(), dataset)
    }

    pub async fn remove_dataset(&mut self, key: &str) -> Option<DatasetInfo> {
        self.data.remove(key)
    }

    pub async fn filter_async<F>(&self, predicate: F) -> Vec<(String, DatasetInfo)>
    // Changed return type
    where
        F: Fn(&DatasetInfo) -> bool + Send + Sync + 'static,
    {
        let data_clone: Vec<(String, DatasetInfo)> = self
            .data
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        tokio::task::spawn_blocking(move || {
            data_clone
                .into_iter() // Changed from .iter() to .into_iter()
                .filter(|(_, dataset)| predicate(dataset))
                .collect::<Vec<(String, DatasetInfo)>>() // Return owned data
        })
        .await
        .unwrap_or_default()
    }

    pub async fn process_datasets_async<F, R>(&self, processor: F) -> Vec<R>
    where
        F: Fn(&DatasetInfo) -> R + Send + Sync + Clone + 'static, // Added + 'static
        R: Send + 'static,
    {
        let mut handles = Vec::new();

        for dataset in self.data.values() {
            let dataset_clone = dataset.clone();
            let processor_clone = processor.clone();

            let handle = tokio::spawn(async move { processor_clone(&dataset_clone) });

            handles.push(handle);
        }

        let mut results = Vec::new();
        for handle in handles {
            if let Ok(result) = handle.await {
                results.push(result);
            }
        }

        results
    }

    pub async fn batch_process_datasets<F, R>(
        index: &DataIndex,
        batch_size: usize,
        processor: F,
    ) -> Vec<R>
    where
        F: Fn(&DatasetInfo) -> R + Send + Sync + Clone + 'static, // Added + 'static
        R: Send + 'static,
    {
        let datasets: Vec<&DatasetInfo> = index.data.values().collect();
        let mut results = Vec::new();

        for chunk in datasets.chunks(batch_size) {
            let mut handles = Vec::new();

            for dataset in chunk {
                let dataset_clone = (*dataset).clone();
                let processor_clone = processor.clone();

                let handle = tokio::spawn(async move { processor_clone(&dataset_clone) });

                handles.push(handle);
            }

            for handle in handles {
                if let Ok(result) = handle.await {
                    results.push(result);
                }
            }
        }

        results
    }
}

impl DatasetInfo {
    pub fn new(
        name: String,
        source: String,
        n_files: u32,
        total_size: u64,
        partition_n_ufs: u32,
        partition_period_start: Option<String>,
        partition_period_end: Option<String>,
        partition_n_periods: u32,
        partition_periodicity: Option<String>,
        latest_update: String,
    ) -> Self {
        Self {
            name,
            source,
            n_files,
            total_size,
            partition_n_ufs,
            partition_period_start,
            partition_period_end,
            partition_n_periods,
            partition_periodicity,
            latest_update,
        }
    }

    pub fn has_period_info(&self) -> bool {
        self.partition_period_start.is_some() && self.partition_period_end.is_some()
    }

    pub fn is_yearly(&self) -> bool {
        self.partition_periodicity
            .as_ref()
            .map_or(false, |p| p == "yearly")
    }

    pub fn is_monthly(&self) -> bool {
        self.partition_periodicity
            .as_ref()
            .map_or(false, |p| p == "monthly")
    }

    pub fn get_human_readable_size(&self) -> String {
        let size = self.total_size as f64;
        let units = ["B", "KB", "MB", "GB", "TB"];
        let mut size_f = size;
        let mut unit_index = 0;

        while size_f >= 1024.0 && unit_index < units.len() - 1 {
            size_f /= 1024.0;
            unit_index += 1;
        }

        if unit_index == 0 {
            format!("{} {}", size as u64, units[unit_index])
        } else {
            format!("{:.1} {}", size_f, units[unit_index])
        }
    }

    pub fn get_period_range(&self) -> Option<(String, String)> {
        match (&self.partition_period_start, &self.partition_period_end) {
            (Some(start), Some(end)) => Some((start.clone(), end.clone())),
            _ => None,
        }
    }

    pub fn matches_source(&self, source: &str) -> bool {
        self.source.eq_ignore_ascii_case(source)
    }

    // Async validation method
    pub async fn validate_async(&self) -> Vec<String> {
        tokio::task::spawn_blocking({
            let dataset = self.clone();
            move || {
                let mut errors = Vec::new();

                if dataset.name.is_empty() {
                    errors.push("Dataset name cannot be empty".to_string());
                }

                if dataset.source.is_empty() {
                    errors.push("Dataset source cannot be empty".to_string());
                }

                if dataset.n_files == 0 {
                    errors.push("Dataset must have at least one file".to_string());
                }

                if dataset.total_size == 0 {
                    errors.push("Dataset total size cannot be zero".to_string());
                }

                errors
            }
        })
        .await
        .unwrap_or_default()
    }
}

impl Default for DataIndex {
    fn default() -> Self {
        Self::new()
    }
}

// Async utility functions
pub async fn load_index_from_file(path: &Path) -> tokio::io::Result<DataIndex> {
    DataIndex::load_from_file(path).await
}

pub async fn save_index_to_file(index: &DataIndex, path: &Path) -> tokio::io::Result<()> {
    index.save_to_file(path).await
}

pub async fn load_index_from_url(
    url: &str,
) -> Result<DataIndex, Box<dyn std::error::Error + Send + Sync>> {
    DataIndex::load_from_url(url).await
}

pub async fn merge_indices(indices: Vec<DataIndex>) -> DataIndex {
    let mut merged = DataIndex::new();

    for index in indices {
        merged.merge_with(index).await;
    }

    merged
}

// Synchronous utility functions (keeping for backward compatibility)
pub fn load_index_from_json(json_str: &str) -> Result<DataIndex, serde_json::Error> {
    serde_json::from_str(json_str)
}

pub fn save_index_to_json(index: &DataIndex) -> Result<String, serde_json::Error> {
    serde_json::to_string_pretty(index)
}

pub fn filter_datasets_by_source<'a>(
    index: &'a DataIndex,
    source: &str,
) -> Vec<(&'a String, &'a DatasetInfo)> {
    index.get_datasets_by_source(source)
}

pub fn get_statistics(index: &DataIndex) -> IndexStatistics {
    IndexStatistics {
        total_datasets: index.count_datasets(),
        total_files: index.total_files(),
        total_size: index.total_size(),
        sources: index.get_all_sources(),
    }
}

// Async statistics
pub async fn get_statistics_async(index: &DataIndex) -> IndexStatistics {
    tokio::task::spawn_blocking({
        let index_clone = index.clone();
        move || get_statistics(&index_clone)
    })
    .await
    .unwrap_or_else(|_| IndexStatistics {
        total_datasets: 0,
        total_files: 0,
        total_size: 0,
        sources: vec![],
    })
}

pub async fn batch_process_datasets<F, R>(
    index: &DataIndex,
    batch_size: usize,
    processor: F,
) -> Vec<R>
where
    F: Fn(&DatasetInfo) -> R + Send + Sync + Clone + 'static,
    R: Send + 'static,
{
    let datasets: Vec<&DatasetInfo> = index.data.values().collect();
    let mut results = Vec::new();

    for chunk in datasets.chunks(batch_size) {
        let mut handles = Vec::new();

        for dataset in chunk {
            let dataset_clone = (*dataset).clone();
            let processor_clone = processor.clone();

            let handle = tokio::spawn(async move { processor_clone(&dataset_clone) });

            handles.push(handle);
        }

        for handle in handles {
            if let Ok(result) = handle.await {
                results.push(result);
            }
        }
    }

    results
}

#[derive(Debug, Serialize)]
pub struct IndexStatistics {
    pub total_datasets: usize,
    pub total_files: u32,
    pub total_size: u64,
    pub sources: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use tempfile::NamedTempFile;
    use tokio::io::AsyncWriteExt;

    fn create_sample_dataset_info() -> DatasetInfo {
        DatasetInfo::new(
            "Test Dataset".to_string(),
            "test-source".to_string(),
            10,
            1024000,
            1,
            Some("2020-01".to_string()),
            Some("2023-12".to_string()),
            48,
            Some("monthly".to_string()),
            "2023-12-01T10:00:00".to_string(),
        )
    }

    fn create_sample_index() -> DataIndex {
        let mut index = DataIndex::new();

        index.add_dataset(
            "test-dataset-1".to_string(),
            DatasetInfo::new(
                "Test Dataset 1".to_string(),
                "source-a".to_string(),
                5,
                512000,
                1,
                Some("2020".to_string()),
                Some("2023".to_string()),
                4,
                Some("yearly".to_string()),
                "2023-01-01T00:00:00".to_string(),
            ),
        );

        index.add_dataset(
            "test-dataset-2".to_string(),
            DatasetInfo::new(
                "Test Dataset 2".to_string(),
                "source-b".to_string(),
                15,
                2048000,
                27,
                Some("2021-01".to_string()),
                Some("2023-12".to_string()),
                36,
                Some("monthly".to_string()),
                "2023-12-01T00:00:00".to_string(),
            ),
        );

        index.add_dataset(
            "test-dataset-3".to_string(),
            DatasetInfo::new(
                "Test Dataset 3".to_string(),
                "source-a".to_string(),
                8,
                1024000,
                1,
                None,
                None,
                0,
                None,
                "2022-06-01T00:00:00".to_string(),
            ),
        );

        index
    }

    #[test]
    fn test_data_index_new() {
        let index = DataIndex::new();
        assert_eq!(index.count_datasets(), 0);
        assert!(index.data.is_empty());
    }

    #[test]
    fn test_data_index_default() {
        let index = DataIndex::default();
        assert_eq!(index.count_datasets(), 0);
    }

    #[test]
    fn test_add_and_get_dataset() {
        let mut index = DataIndex::new();
        let dataset = create_sample_dataset_info();

        index.add_dataset("test-key".to_string(), dataset.clone());

        assert_eq!(index.count_datasets(), 1);
        let retrieved = index.get_dataset("test-key").unwrap();
        assert_eq!(retrieved.name, "Test Dataset");
        assert_eq!(retrieved.source, "test-source");
        assert_eq!(retrieved.n_files, 10);
    }

    #[test]
    fn test_get_nonexistent_dataset() {
        let index = DataIndex::new();
        assert!(index.get_dataset("nonexistent").is_none());
    }

    #[test]
    fn test_get_datasets_by_source() {
        let index = create_sample_index();

        let source_a_datasets = index.get_datasets_by_source("source-a");
        assert_eq!(source_a_datasets.len(), 2);

        let source_b_datasets = index.get_datasets_by_source("source-b");
        assert_eq!(source_b_datasets.len(), 1);

        let nonexistent_datasets = index.get_datasets_by_source("nonexistent");
        assert_eq!(nonexistent_datasets.len(), 0);
    }

    #[test]
    fn test_get_all_sources() {
        let index = create_sample_index();
        let sources = index.get_all_sources();

        assert_eq!(sources.len(), 2);
        assert!(sources.contains(&"source-a".to_string()));
        assert!(sources.contains(&"source-b".to_string()));
    }

    #[test]
    fn test_get_dataset_keys() {
        let index = create_sample_index();
        let keys = index.get_dataset_keys();

        assert_eq!(keys.len(), 3);
        assert!(keys.contains(&&"test-dataset-1".to_string()));
        assert!(keys.contains(&&"test-dataset-2".to_string()));
        assert!(keys.contains(&&"test-dataset-3".to_string()));
    }

    #[test]
    fn test_statistics() {
        let index = create_sample_index();

        assert_eq!(index.count_datasets(), 3);
        assert_eq!(index.total_files(), 28); // 5 + 15 + 8
        assert_eq!(index.total_size(), 3584000); // 512000 + 2048000 + 1024000
    }

    #[test]
    fn test_get_datasets_by_periodicity() {
        let index = create_sample_index();

        let monthly = index.get_datasets_by_periodicity("monthly");
        assert_eq!(monthly.len(), 1);
        assert_eq!(monthly[0].0, "test-dataset-2");

        let yearly = index.get_datasets_by_periodicity("yearly");
        assert_eq!(yearly.len(), 1);
        assert_eq!(yearly[0].0, "test-dataset-1");

        let nonexistent = index.get_datasets_by_periodicity("daily");
        assert_eq!(nonexistent.len(), 0);
    }

    #[test]
    fn test_get_largest_datasets() {
        let index = create_sample_index();
        let largest = index.get_largest_datasets(2);

        assert_eq!(largest.len(), 2);
        assert_eq!(largest[0].0, "test-dataset-2"); // 2048000 bytes
        assert_eq!(largest[1].0, "test-dataset-3"); // 1024000 bytes
    }

    #[test]
    fn test_get_most_recent_updates() {
        let index = create_sample_index();
        let recent = index.get_most_recent_updates(2);

        assert_eq!(recent.len(), 2);
        assert_eq!(recent[0].0, "test-dataset-2"); // 2023-12-01
        assert_eq!(recent[1].0, "test-dataset-1"); // 2023-01-01
    }

    #[test]
    fn test_dataset_info_methods() {
        let dataset = create_sample_dataset_info();

        assert!(dataset.has_period_info());
        assert!(dataset.is_monthly());
        assert!(!dataset.is_yearly());
        assert!(dataset.matches_source("test-source"));
        assert!(dataset.matches_source("TEST-SOURCE")); // case insensitive
        assert!(!dataset.matches_source("other-source"));

        let period_range = dataset.get_period_range().unwrap();
        assert_eq!(period_range.0, "2020-01");
        assert_eq!(period_range.1, "2023-12");
    }

    #[test]
    fn test_dataset_info_without_periods() {
        let dataset = DatasetInfo::new(
            "Test".to_string(),
            "test".to_string(),
            1,
            1000,
            1,
            None,
            None,
            0,
            None,
            "2023-01-01T00:00:00".to_string(),
        );

        assert!(!dataset.has_period_info());
        assert!(!dataset.is_monthly());
        assert!(!dataset.is_yearly());
        assert!(dataset.get_period_range().is_none());
    }

    #[test]
    fn test_human_readable_size() {
        let dataset = DatasetInfo::new(
            "Test".to_string(),
            "test".to_string(),
            1,
            1536, // 1.5 KB
            1,
            None,
            None,
            0,
            None,
            "2023-01-01T00:00:00".to_string(),
        );

        assert_eq!(dataset.get_human_readable_size(), "1.5 KB");

        let large_dataset = DatasetInfo::new(
            "Large".to_string(),
            "test".to_string(),
            1,
            1073741824, // 1 GB
            1,
            None,
            None,
            0,
            None,
            "2023-01-01T00:00:00".to_string(),
        );

        assert_eq!(large_dataset.get_human_readable_size(), "1.0 GB");
    }

    #[test]
    fn test_load_index_from_json() {
        let json_str = r#"
        {
            "data": {
                "test-dataset": {
                    "name": "Test Dataset",
                    "source": "test-source",
                    "n_files": 5,
                    "total_size": 1024,
                    "partition_n_ufs": 1,
                    "partition_period_start": "2020",
                    "partition_period_end": "2023",
                    "partition_n_periods": 4,
                    "partition_periodicity": "yearly",
                    "latest_update": "2023-01-01T00:00:00"
                }
            }
        }
        "#;

        let index = load_index_from_json(json_str).unwrap();
        assert_eq!(index.count_datasets(), 1);

        let dataset = index.get_dataset("test-dataset").unwrap();
        assert_eq!(dataset.name, "Test Dataset");
        assert_eq!(dataset.n_files, 5);
    }

    #[test]
    fn test_save_index_to_json() {
        let index = create_sample_index();
        let json_str = save_index_to_json(&index).unwrap();

        // Parse it back to verify
        let parsed_index = load_index_from_json(&json_str).unwrap();
        assert_eq!(parsed_index.count_datasets(), 3);
    }

    #[test]
    fn test_filter_datasets_by_source() {
        let index = create_sample_index();
        let filtered = filter_datasets_by_source(&index, "source-a");
        assert_eq!(filtered.len(), 2);
    }

    #[test]
    fn test_get_statistics() {
        let index = create_sample_index();
        let stats = get_statistics(&index);

        assert_eq!(stats.total_datasets, 3);
        assert_eq!(stats.total_files, 28);
        assert_eq!(stats.total_size, 3584000);
        assert_eq!(stats.sources.len(), 2);
    }

    // Async tests
    #[tokio::test]
    async fn test_async_file_operations() {
        let index = create_sample_index();

        // Create a temporary file
        let temp_file = NamedTempFile::new().unwrap();
        let temp_path = temp_file.path();

        // Save index to file
        index.save_to_file(temp_path).await.unwrap();

        // Load index from file
        let loaded_index = DataIndex::load_from_file(temp_path).await.unwrap();

        assert_eq!(loaded_index.count_datasets(), 3);
        assert_eq!(loaded_index.total_files(), 28);
    }

    #[tokio::test]
    async fn test_async_utility_functions() {
        let index = create_sample_index();

        // Create a temporary file
        let temp_file = NamedTempFile::new().unwrap();
        let temp_path = temp_file.path();

        // Test save_index_to_file
        save_index_to_file(&index, temp_path).await.unwrap();

        // Test load_index_from_file
        let loaded_index = load_index_from_file(temp_path).await.unwrap();
        assert_eq!(loaded_index.count_datasets(), 3);
    }

    #[tokio::test]
    async fn test_merge_indices() {
        let mut index1 = DataIndex::new();
        index1.add_dataset("dataset-1".to_string(), create_sample_dataset_info());

        let mut index2 = DataIndex::new();
        index2.add_dataset("dataset-2".to_string(), create_sample_dataset_info());

        let merged = merge_indices(vec![index1, index2]).await;
        assert_eq!(merged.count_datasets(), 2);
    }

    #[tokio::test]
    async fn test_update_and_remove_dataset() {
        let mut index = create_sample_index();

        // Update dataset
        let new_dataset = create_sample_dataset_info();
        let old_dataset = index
            .update_dataset("test-dataset-1", new_dataset.clone())
            .await;
        assert!(old_dataset.is_some());

        let updated = index.get_dataset("test-dataset-1").unwrap();
        assert_eq!(updated.name, "Test Dataset");

        // Remove dataset
        let removed = index.remove_dataset("test-dataset-1").await;
        assert!(removed.is_some());
        assert_eq!(index.count_datasets(), 2);
    }

    #[tokio::test]
    async fn test_filter_async() {
        let index = create_sample_index();

        // Filter datasets with more than 10 files
        let filtered = index.filter_async(|dataset| dataset.n_files > 10).await;
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].0, "test-dataset-2");
    }

    #[tokio::test]
    async fn test_process_datasets_async() {
        let index = create_sample_index();

        // Process datasets to extract file counts
        let file_counts = index
            .process_datasets_async(|dataset| dataset.n_files)
            .await;
        assert_eq!(file_counts.len(), 3);
        assert!(file_counts.contains(&5));
        assert!(file_counts.contains(&15));
        assert!(file_counts.contains(&8));
    }

    #[tokio::test]
    async fn test_batch_process_datasets() {
        let index = create_sample_index();

        // Process datasets in batches to extract sources
        let sources = batch_process_datasets(&index, 2, |dataset| dataset.source.clone()).await;
        assert_eq!(sources.len(), 3);
        assert!(sources.contains(&"source-a".to_string()));
        assert!(sources.contains(&"source-b".to_string()));
    }

    #[tokio::test]
    async fn test_get_statistics_async() {
        let index = create_sample_index();
        let stats = get_statistics_async(&index).await;

        assert_eq!(stats.total_datasets, 3);
        assert_eq!(stats.total_files, 28);
        assert_eq!(stats.total_size, 3584000);
        assert_eq!(stats.sources.len(), 2);
    }

    #[tokio::test]
    async fn test_dataset_validate_async() {
        let valid_dataset = create_sample_dataset_info();
        let errors = valid_dataset.validate_async().await;
        assert!(errors.is_empty());

        let invalid_dataset = DatasetInfo::new(
            "".to_string(), // Empty name
            "".to_string(), // Empty source
            0,              // Zero files
            0,              // Zero size
            1,
            None,
            None,
            0,
            None,
            "2023-01-01T00:00:00".to_string(),
        );

        let errors = invalid_dataset.validate_async().await;
        assert_eq!(errors.len(), 4);
        assert!(errors.contains(&"Dataset name cannot be empty".to_string()));
        assert!(errors.contains(&"Dataset source cannot be empty".to_string()));
        assert!(errors.contains(&"Dataset must have at least one file".to_string()));
        assert!(errors.contains(&"Dataset total size cannot be zero".to_string()));
    }

    #[tokio::test]
    async fn test_merge_with() {
        let mut index1 = DataIndex::new();
        index1.add_dataset("dataset-1".to_string(), create_sample_dataset_info());

        let mut index2 = DataIndex::new();
        index2.add_dataset("dataset-2".to_string(), create_sample_dataset_info());

        index1.merge_with(index2).await;
        assert_eq!(index1.count_datasets(), 2);
    }

    #[tokio::test]
    async fn test_load_from_invalid_file() {
        let temp_file = NamedTempFile::new().unwrap();
        let temp_path = temp_file.path();

        // Write invalid JSON to file
        let mut file = tokio::fs::File::create(temp_path).await.unwrap();
        file.write_all(b"invalid json").await.unwrap();
        file.flush().await.unwrap();

        let result = DataIndex::load_from_file(temp_path).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_load_from_nonexistent_file() {
        let result = DataIndex::load_from_file(Path::new("nonexistent.json")).await;
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_json_parsing() {
        let invalid_json = "{ invalid json }";
        let result = load_index_from_json(invalid_json);
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_index_statistics() {
        let index = DataIndex::new();
        let stats = get_statistics(&index);

        assert_eq!(stats.total_datasets, 0);
        assert_eq!(stats.total_files, 0);
        assert_eq!(stats.total_size, 0);
        assert!(stats.sources.is_empty());
    }

    #[test]
    fn test_dataset_info_edge_cases() {
        // Test with very large numbers
        let large_dataset = DatasetInfo::new(
            "Large Dataset".to_string(),
            "large-source".to_string(),
            u32::MAX,
            u64::MAX,
            1,
            Some("1900".to_string()),
            Some("2100".to_string()),
            0,
            Some("century".to_string()),
            "2023-01-01T00:00:00".to_string(),
        );

        assert_eq!(large_dataset.n_files, u32::MAX);
        assert_eq!(large_dataset.total_size, u64::MAX);
        assert!(large_dataset.has_period_info());
        assert!(!large_dataset.is_monthly());
        assert!(!large_dataset.is_yearly());
    }

    #[test]
    fn test_human_readable_size_edge_cases() {
        // Test with bytes
        let small_dataset = DatasetInfo::new(
            "Small".to_string(),
            "test".to_string(),
            1,
            512, // 512 bytes
            1,
            None,
            None,
            0,
            None,
            "2023-01-01T00:00:00".to_string(),
        );
        assert_eq!(small_dataset.get_human_readable_size(), "512 B");

        // Test with TB
        let huge_dataset = DatasetInfo::new(
            "Huge".to_string(),
            "test".to_string(),
            1,
            1099511627776, // 1 TB
            1,
            None,
            None,
            0,
            None,
            "2023-01-01T00:00:00".to_string(),
        );
        assert_eq!(huge_dataset.get_human_readable_size(), "1.0 TB");
    }

    #[test]
    fn test_get_datasets_by_periodicity_edge_cases() {
        let index = create_sample_index();

        // Test with None periodicity
        let none_periodicity = index.get_datasets_by_periodicity("");
        assert_eq!(none_periodicity.len(), 0);

        // Test case sensitivity
        let monthly_upper = index.get_datasets_by_periodicity("MONTHLY");
        assert_eq!(monthly_upper.len(), 0); // Should be case sensitive
    }

    #[test]
    fn test_get_largest_datasets_limit() {
        let index = create_sample_index();

        // Test with limit larger than dataset count
        let all_datasets = index.get_largest_datasets(10);
        assert_eq!(all_datasets.len(), 3);

        // Test with limit of 0
        let no_datasets = index.get_largest_datasets(0);
        assert_eq!(no_datasets.len(), 0);
    }

    #[test]
    fn test_get_most_recent_updates_limit() {
        let index = create_sample_index();

        // Test with limit larger than dataset count
        let all_updates = index.get_most_recent_updates(10);
        assert_eq!(all_updates.len(), 3);

        // Test with limit of 0
        let no_updates = index.get_most_recent_updates(0);
        assert_eq!(no_updates.len(), 0);
    }

    #[tokio::test]
    async fn test_filter_async_empty_result() {
        let index = create_sample_index();

        // Filter with condition that matches nothing
        let filtered = index.filter_async(|dataset| dataset.n_files > 100).await;
        assert_eq!(filtered.len(), 0);
    }

    #[tokio::test]
    async fn test_process_datasets_async_empty_index() {
        let index = DataIndex::new();

        let results = index
            .process_datasets_async(|dataset| dataset.n_files)
            .await;
        assert_eq!(results.len(), 0);
    }

    #[tokio::test]
    async fn test_batch_process_datasets_empty_index() {
        let index = DataIndex::new();

        let results = batch_process_datasets(&index, 2, |dataset| dataset.source.clone()).await;
        assert_eq!(results.len(), 0);
    }

    // Mock HTTP test would require additional setup
    // For now, we'll skip the load_from_url test as it requires external dependencies
    // and network access. In a real project, you'd use a mock HTTP client.

    #[test]
    fn test_index_statistics_serialization() {
        let index = create_sample_index();
        let stats = get_statistics(&index);

        // Test that IndexStatistics can be serialized
        let json = serde_json::to_string(&stats).unwrap();
        assert!(json.contains("total_datasets"));
        assert!(json.contains("total_files"));
        assert!(json.contains("total_size"));
        assert!(json.contains("sources"));
    }
}

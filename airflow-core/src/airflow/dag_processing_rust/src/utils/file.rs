use anyhow::{anyhow, Result};
use globset::{Glob, GlobSet, GlobSetBuilder};
use log::{debug, warn};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use walkdir::{DirEntry, WalkDir};

const AIRFLOW_IGNORE_FILE: &str = ".airflowignore";

/// Find python files in a directory, respecting .airflowignore files.
/// This is a rust implementation of the logic in airflow.utils.file.find_dag_file_paths
pub fn find_dag_file_paths(directory: &str) -> Result<Vec<String>> {
    let base_dir = Path::new(directory);
    if !base_dir.is_dir() {
        return Err(anyhow!("'{}' is not a directory", directory));
    }

    let mut file_paths = Vec::new();
    let mut patterns_by_dir: HashMap<PathBuf, GlobSet> = HashMap::new();

    let walker = WalkDir::new(directory)
        .follow_links(true)
        .into_iter()
        .filter_entry(|e| {
            let path = e.path();
            let parent = path.parent().unwrap_or(base_dir);

            let mut current_patterns = GlobSetBuilder::new();
            let ignore_file_path = parent.join(AIRFLOW_IGNORE_FILE);
            if ignore_file_path.is_file() {
                if let Ok(lines) = fs::read_to_string(&ignore_file_path) {
                    for line in lines.lines() {
                        let trimmed = line.trim();
                        if trimmed.is_empty() || trimmed.starts_with('#') {
                            continue;
                        }
                        // TODO: Handle more complex gitignore patterns like negation `!` or dir-specific `/`
                        match Glob::new(trimmed) {
                            Ok(glob) => {
                                current_patterns.add(glob);
                            }
                            Err(e) => warn!(
                                "Invalid glob pattern '{}' in {:?}: {}",
                                trimmed, ignore_file_path, e
                            ),
                        }
                    }
                }
            }
            let new_patterns = match current_patterns.build() {
                Ok(patterns) => patterns,
                Err(e) => {
                    warn!("Error building glob set for {:?}: {}", parent, e);
                    GlobSet::empty()
                }
            };

            // Check against all parent patterns
            let mut current_path = parent;
            loop {
                if let Some(patterns) = patterns_by_dir.get(current_path) {
                    if patterns.is_match(path.strip_prefix(current_path).unwrap_or(path)) {
                        debug!("Ignoring {:?} due to rule in {:?}", path, current_path);
                        return false;
                    }
                }
                if current_path == base_dir {
                    break;
                }
                if let Some(p) = current_path.parent() {
                    current_path = p;
                } else {
                    break;
                }
            }

            if new_patterns.is_match(path.strip_prefix(parent).unwrap_or(path)) {
                debug!("Ignoring path {:?}", path);
                return false;
            }

            if path.is_dir() {
                patterns_by_dir.insert(path.to_path_buf(), new_patterns);
            }

            true
        });

    for entry in walker {
        match entry {
            Ok(entry) => {
                if is_dag_file(&entry) {
                    if let Some(path_str) = entry.path().to_str() {
                        file_paths.push(path_str.to_string());
                    }
                }
            }
            Err(e) => warn!("Error walking directory: {}", e),
        }
    }

    Ok(file_paths)
}

fn is_dag_file(entry: &DirEntry) -> bool {
    entry.file_type().is_file()
        && entry
            .path()
            .extension()
            .map_or(false, |ext| ext == "py")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{self, File};
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn test_find_dag_file_paths_simple() {
        let dir = tempdir().unwrap();
        File::create(dir.path().join("dag1.py")).unwrap();
        File::create(dir.path().join("dag2.py")).unwrap();
        File::create(dir.path().join("not_a_dag.txt")).unwrap();

        let mut result = find_dag_file_paths(dir.path().to_str().unwrap()).unwrap();
        result.sort();

        assert_eq!(result.len(), 2);
        assert!(result[0].ends_with("dag1.py"));
        assert!(result[1].ends_with("dag2.py"));
    }

    #[test]
    fn test_find_dag_file_paths_with_airflowignore() {
        let dir = tempdir().unwrap();
        let path = dir.path();
        File::create(path.join("dag1.py")).unwrap();
        File::create(path.join("ignored_dag.py")).unwrap();
        let mut ignore_file = File::create(path.join(".airflowignore")).unwrap();
        writeln!(ignore_file, "ignored_*.py").unwrap();

        let mut result = find_dag_file_paths(path.to_str().unwrap()).unwrap();
        result.sort();

        assert_eq!(result.len(), 1);
        assert!(result[0].ends_with("dag1.py"));
    }

    #[test]
    fn test_find_dag_file_paths_with_subdir_airflowignore() {
        let dir = tempdir().unwrap();
        let path = dir.path();
        let subdir_path = path.join("subdir");
        fs::create_dir(&subdir_path).unwrap();

        File::create(path.join("dag1.py")).unwrap();
        File::create(subdir_path.join("dag2.py")).unwrap();
        File::create(subdir_path.join("ignored_dag.py")).unwrap();

        let mut ignore_file = File::create(subdir_path.join(".airflowignore")).unwrap();
        writeln!(ignore_file, "ignored_*.py").unwrap();

        let mut result = find_dag_file_paths(path.to_str().unwrap()).unwrap();
        result.sort();

        assert_eq!(result.len(), 2);
        assert!(result[0].ends_with("dag1.py"));
        assert!(result[1].ends_with("dag2.py"));
    }

    #[test]
    fn test_find_dag_file_paths_ignore_subdir() {
        let dir = tempdir().unwrap();
        let path = dir.path();
        let subdir_path = path.join("ignored_subdir");
        fs::create_dir(&subdir_path).unwrap();

        File::create(path.join("dag1.py")).unwrap();
        File::create(subdir_path.join("dag2.py")).unwrap();

        let mut ignore_file = File::create(path.join(".airflowignore")).unwrap();
        writeln!(ignore_file, "ignored_subdir").unwrap();

        let result = find_dag_file_paths(path.to_str().unwrap()).unwrap();

        assert_eq!(result.len(), 1);
        assert!(result[0].ends_with("dag1.py"));
    }

    #[test]
    fn test_find_dag_file_paths_with_invalid_airflowignore_pattern() {
        let dir = tempdir().unwrap();
        let path = dir.path();
        File::create(path.join("dag1.py")).unwrap();
        File::create(path.join("dag_to_be_ignored.py")).unwrap();
        let mut ignore_file = File::create(path.join(".airflowignore")).unwrap();
        // This is an invalid glob pattern, it should be ignored.
        writeln!(ignore_file, "dag_to_be_ignored.py[").unwrap();

        // We expect both files to be found, as the invalid pattern is skipped.
        let mut result = find_dag_file_paths(path.to_str().unwrap()).unwrap();
        result.sort();

        assert_eq!(result.len(), 2);
        assert!(result[0].ends_with("dag1.py"));
        assert!(result[1].ends_with("dag_to_be_ignored.py"));
    }
}

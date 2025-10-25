use crate::bundles::traits::DagBundle;

#[derive(Debug)]
pub struct LocalDagBundle {
    pub path: String,
    pub refresh_interval: u64
}

impl LocalDagBundle {
    pub fn new(path: String, refresh_interval: u64) -> Self {
        Self {
            path,
            refresh_interval
        }
    }
}

impl DagBundle for LocalDagBundle {
    fn get_current_version(&self) -> anyhow::Result<Option<String>> {
        Ok(None)
    }
    fn refresh(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

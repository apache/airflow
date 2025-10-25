pub trait DagBundle {
    fn get_current_version(&self) -> anyhow::Result<Option<String>>;
    fn refresh(&mut self) -> anyhow::Result<()>;
}

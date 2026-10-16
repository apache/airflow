# Fix: DynamicRegion.get_or_create_many() with HA lock safety
class DynamicRegion:
    def get_or_create_many(self, map_indexes):
        """
        Create or retrieve regions for dynamically mapped tasks.
        Must preserve scheduler-created DagRuns and HA locks.
        """
        # Fix: Acquire lock before creating regions
        with self.get_lock():
            regions = []
            for idx in map_indexes:
                # Safely create region without breaking HA locks
                region = self._get_or_create_single(idx)
                regions.append(region)
            # Commit only within lock context
            self._commit_safely()
        return regions

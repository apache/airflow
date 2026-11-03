Fix ``DELETE /assets/{asset_id}/queuedEvents`` deleting queued asset events of Dags the user can only read; it now deletes only the queued events of Dags the user may edit.

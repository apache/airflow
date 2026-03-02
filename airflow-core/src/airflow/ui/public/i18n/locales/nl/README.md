# Dutch (nl) Translation Guidelines

This directory contains the Dutch translation files for the Airflow UI.

## Terminology Selection

The Dutch translation follows these core principles:

1.  **Airflow Specific Terms**: Terms like `Dag`, `XCom`, and `Asset` are kept in English as they are widely recognized technical objects within the Airflow ecosystem. We use `Dag` (not `DAG`) to align with Airflow conventions.
2.  **User Address**: We use the informal **"je/jouw"** register. This makes the UI feel more modern and accessible, which is a common trend in technical software localized for the Dutch market.
3.  **Technical vs. Natural Language**: 
    - `Task` is used when referring to the technical object (e.g., `Task Instance`).
    - `taak` is used in more descriptive prose or standard UI actions.
4.  **Established UI Mappings**:
    - `Schedule` -> `Planning`
    - `Queue` -> `Wachtrij`
    - `State` -> `Status`
    - `Run` -> `Run` (kept in English as it's a standard term in data engineering)

## Consistency

All new translations should be verified against `common.json` to ensure they use the standard Dutch terms for buttons (e.g., `Opslaan`, `Annuleren`, `Verwijderen`) and states (e.g., `Lopend`, `Mislukt`, `Succesvol`).

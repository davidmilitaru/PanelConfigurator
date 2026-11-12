# PanelConfigurator

**A photovoltaic panel configuration and optimization tool for residential households.**

PanelConfigurator is a Python application that analyzes household electricity consumption and solar irradiation data to determine a suitable photovoltaic panel configuration. It combines database querying, time-series processing, energy analysis, and numerical optimization to identify configurations that balance energy independence and financial performance.

## Overview

The application uses household consumption patterns, solar irradiation measurements, installation constraints, and financial parameters to evaluate possible photovoltaic configurations.

Its main objective is to maximize the **Net Present Value (NPV) over a five-year period**, while accounting for installation costs, operating expenses, electricity purchases from the grid, and revenue from surplus energy injected into the grid.

## Data Processing Pipeline

The project follows an end-to-end data processing workflow:

**Existing regional energy dataset → SQLite database import → SQL data extraction → Python processing → Energy analysis → Configuration optimization**

- **Dataset and database:** An existing energy-related dataset originating from a region in France was imported into a local SQLite database (`irise.sqlite3`).
- **SQL extraction:** Parameterized SQL queries retrieve the consumption and weather records required for a given household, appliance, weather station, and measurement period.
- **Python processing:** Retrieved measurements are transformed, aggregated, and aligned into hourly time series.
- **Energy analysis:** Consumption and irradiation data are used to evaluate relevant energy indicators, including self-consumption, self-sufficiency, and energy exchanged with the grid.
- **Optimization:** Candidate photovoltaic configurations are evaluated to identify a financially advantageous solution that respects installation constraints.

## Key Features

- Household electricity consumption analysis.
- Solar irradiation data processing.
- Hourly time-series aggregation and alignment.
- Evaluation of photovoltaic configurations with different panel types.
- Numerical optimization using SciPy's Differential Evolution algorithm.
- Financial evaluation based on a five-year NPV model.
- Interactive application interface and energy data visualizations.

## Technical Stack

| Technology | Purpose |
|---|---|
| Python | Core application logic and data processing |
| SQLite | Relational database for energy data |
| SQL / `sqlite3` | Database querying and data retrieval |
| NumPy | Numerical computations |
| SciPy | Differential Evolution optimization |
| Matplotlib | Data visualization |
| Streamlit | Interactive application interface |

## Engineering Highlights

The project demonstrates several software engineering and data processing concepts:

- **Object-oriented programming:** Encapsulation of household, consumption, production, and database-related functionality into dedicated classes.
- **Database integration:** Importing an existing dataset into SQLite and retrieving relevant records through parameterized SQL queries.
- **Time-series processing:** Aggregating high-frequency measurements into hourly consumption values and aligning consumption and weather data by timestamp.
- **Numerical optimization:** Applying a population-based optimization algorithm to a constrained, financially driven configuration problem.
- **Data visualization:** Presenting energy consumption patterns and analysis results across different time scales.

## Project Structure

| File | Description |
|---|---|
| `main.py` | Main application, optimization logic, financial evaluation, and Streamlit interface |
| `analysis_module.py` | Energy analysis, supporting classes, and visualizations |

## Installation and Usage

Clone the repository:

```bash
git clone https://github.com/davidmilitaru/PanelConfigurator.git
cd PanelConfigurator
```

Install the required dependencies:

```bash
pip install numpy scipy matplotlib streamlit
```

Make sure the local SQLite database (`irise.sqlite3`) is available in the expected project location. The database is not included in the repository.

Run the application:

```bash
streamlit run main.py
```

---

*Developed as part of a mandatory summer practical training programme during the third year of the Bachelor's degree in Computer Science at the Faculty of Automatic Control and Computer Science.*

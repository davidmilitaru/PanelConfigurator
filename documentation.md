# PanelConfigurator – Solar Panel Configuration Optimization

A Python-based application that analyzes household electricity consumption and solar irradiation data to determine an economically optimal photovoltaic (PV) system configuration. The application combines numerical optimization, relational database querying, financial modelling, and interactive data visualization to recommend a suitable combination of solar panels based on user-defined requirements.

Developed as part of a mandatory summer practical training programme during the third year of the Bachelor's degree in Automatic Control and Computer Science, under the supervision of a faculty professor.

## 1. Project Overview

The purpose of PanelConfigurator is to determine an economically viable photovoltaic panel configuration for a household, taking into consideration its electricity consumption patterns, local solar irradiation, available installation area, and desired energy self-sufficiency.

Instead of simply maximizing the installed photovoltaic capacity, the application searches for a configuration that maximizes the investment's Net Present Value (NPV) over a five-year period while satisfying technical constraints.

The application considers three types of photovoltaic panels:

| Panel category | Nominal power | Cost per watt |
|---|---:|---:|
| Low-cost | 200 W | €0.16 |
| Standard | 300 W | €0.22 |
| High-efficiency | 415 W | €0.31 |

The resulting configuration is evaluated using historical household consumption and weather data, allowing the application to compare electricity demand against estimated photovoltaic production.

## 2. Technical Stack

| Technology | Purpose |
|---|---|
| Python | Core application development and implementation of the optimization and analysis logic |
| SQLite | Persistent relational storage of household consumption and weather data |
| SQL | Database querying, filtering, and data retrieval |
| NumPy | Numerical computations and statistical aggregation |
| SciPy | Differential Evolution optimization algorithm |
| Matplotlib | Time-series plotting and visualization |
| Streamlit | Interactive web application and user interface |
| Python OOP | Domain modelling, encapsulation, and separation of responsibilities |
| Python Type Hints | Explicit function signatures and improved code readability |

## 3. Application Architecture

The application is divided into two main Python modules, each responsible for a different part of the processing pipeline.

### `main.py` – Optimization and Application Entry Point

This module handles the main application workflow:

- Defines photovoltaic panel specifications and financial parameters.
- Implements the database access functionality required by the optimization process.
- Retrieves household consumption and solar irradiation data.
- Estimates photovoltaic production for different panel configurations.
- Calculates electricity costs, investment returns, and technical indicators.
- Defines the optimization objective and its constraints.
- Integrates the Streamlit interface with the optimization engine.
- Displays the optimal configuration and its performance indicators.

### `analysis_module.py` – Energy Analysis and Visualization

This module encapsulates the analysis of household energy consumption and photovoltaic production.

It introduces dedicated classes for representing the application's domain entities and provides an `EnergyAnalysis` class responsible for:

- Retrieving and organizing household consumption data.
- Processing solar irradiation and photovoltaic production data.
- Aggregating consumption and production into hourly time series.
- Calculating technical performance indicators.
- Generating comparative plots at different temporal resolutions.

This separation allows the optimization workflow and the subsequent performance analysis to be handled by distinct modules.

## 4. Database Management and Data Processing

One of the central components of the application is its interaction with a relational SQLite database.

### Database storage

The application uses a local SQLite database named `irise.sqlite3`, accessed through Python's built-in `sqlite3` module.

SQLite provides a lightweight relational database solution that does not require a separate database server. The database is stored as a file on the local filesystem and accessed directly by the Python application.

The application retrieves information from two main tables:

- **Consumption** – household electricity consumption records, including household identifiers, appliance identifiers, timestamps, and measured values.
- **WeatherData** – meteorological measurements, including weather station identifiers, weather variable identifiers, timestamps, and recorded values.

Rather than loading the entire database into memory, the application uses SQL queries to retrieve the records relevant to the selected household, appliances, weather station, and measurement period.

### SQL querying

The database access layer relies on parameterized SQL queries to filter records according to their identifiers and timestamps.

For example, household consumption is retrieved by filtering on `HouseIDREF`, `ApplianceIDREF`, and `EpochTime`.

This approach provides several benefits:

- Retrieves only the records required by the application.
- Keeps database operations separate from the energy analysis logic.
- Allows different household appliances and weather variables to be queried independently.
- Uses parameter binding rather than constructing SQL statements through string concatenation.

The retrieved records are subsequently transformed into Python objects, making them easier to manipulate throughout the application.

### Data aggregation and transformation

The raw consumption measurements are processed before being used in the optimization and visualization stages.

In particular, the consumption retrieval logic groups six consecutive measurements into an hourly value, summing their recorded consumption and associating the result with a timestamp.

The resulting hourly records are then aggregated across individual appliances to obtain the household's total electricity consumption.

Solar irradiation data is retrieved independently and used to estimate photovoltaic production.

This produces two comparable time series:

1. Hourly household electricity consumption.
2. Hourly photovoltaic electricity production.

The application uses Python dictionaries keyed by `datetime` objects to efficiently organize and match consumption and production records by timestamp.

These transformations provide the foundation for the subsequent technical and financial calculations.

## 5. Object-Oriented Programming

An important software engineering aspect of the project is the use of Object-Oriented Programming to represent the application's main entities and organize its responsibilities.

Several classes are defined in `analysis_module.py`:

| Class | Responsibility |
|---|---|
| `Consumer` | Represents an electricity-consuming appliance and its associated consumption measurements. |
| `House` | Represents a household through its identifier and location. |
| `Production` | Encapsulates photovoltaic production values and their corresponding timestamps. |
| `Rayonnement` | Represents solar irradiation measurements and their timestamps. |
| `DatabaseManager` | Encapsulates SQLite connection management and database retrieval operations. |
| `EnergyAnalysis` | Coordinates data retrieval, processing, technical indicator calculations, and visualization. |

### Encapsulation and separation of responsibilities

The `DatabaseManager` class encapsulates database-related operations, providing methods dedicated to retrieving consumption, irradiation, and production records.

This prevents SQL queries and database connection handling from being scattered throughout the analytical code.

The `EnergyAnalysis` class builds upon these data access capabilities. It maintains the relevant configuration parameters, loads the required datasets, aggregates the measurements, and exposes methods for calculating and visualizing the resulting indicators.

The domain classes (`Consumer`, `Production`, and `Rayonnement`) provide structured representations of the data instead of relying exclusively on loosely organized tuples or dictionaries.

This approach makes the code easier to understand, maintain, and extend.

The project also uses Python type annotations, including `List`, `Dict`, and `Tuple`, to make expected data structures and method interfaces more explicit.

## 6. Energy Analysis and Technical Indicators

The application evaluates the relationship between household consumption and photovoltaic production at several temporal resolutions.

### Consumption versus production

The application generates comparative graphs for:

- An entire year.
- An average month.
- An average week.
- An average day.

NumPy is used to calculate average consumption and production patterns, while Matplotlib provides the graphical representation of the resulting time series.

This makes it possible to identify periods when photovoltaic production exceeds household demand and periods when additional electricity must be purchased from the grid.

### Self-consumption

Self-consumption measures the proportion of photovoltaic electricity generated that is consumed directly by the household.

It is calculated by comparing photovoltaic production and household consumption over matching time intervals.

### Self-sufficiency

Self-sufficiency measures the proportion of household electricity demand that can be covered by the photovoltaic installation.

Unlike self-consumption, this indicator focuses on the household's energy requirements rather than total photovoltaic production.

It is particularly important in the optimization process because the user can specify a minimum desired self-sufficiency level.

### Net Energy Exchanged with the Grid (NEEG)

This indicator quantifies the energy exchanged between the household and the electricity grid.

It provides additional insight into the balance between photovoltaic production and household consumption.

The application calculates and visualizes the daily and annual evolution of these technical indicators.

## 7. Optimization and Financial Modelling

The optimization engine is implemented using SciPy's `differential_evolution` algorithm.

The objective is to identify the optimal number of panels from each of the three available categories.

The decision variables are:

- Number of low-cost panels.
- Number of standard panels.
- Number of high-efficiency panels.

### Objective function

The optimization seeks to maximize the Net Present Value (NPV) of the photovoltaic investment over a five-year period.

The financial model considers:

- Initial investment expenditure (CAPEX).
- Operating expenditure (OPEX).
- Electricity purchased from the grid.
- Revenue associated with electricity injected into the grid.
- The electricity grid price.
- The electricity injection remuneration.
- The investment discount rate.

The model estimates the financial benefits of installing photovoltaic panels by comparing the household's electricity costs with and without the photovoltaic system.

The objective function is implemented as a minimization problem by minimizing the negative NPV.

### Constraints

The optimization considers two main constraints:

**1. Maximum number of panels**

The total number of installed panels cannot exceed the maximum capacity specified by the user, based on the available installation area.

**2. Minimum self-sufficiency**

The resulting configuration must satisfy the minimum self-sufficiency percentage specified by the user.

Configurations that violate these requirements receive penalty values in the objective function, encouraging the optimization algorithm to search for feasible solutions.

### Differential Evolution

Differential Evolution is a population-based stochastic optimization algorithm that explores the search space using candidate solutions and evolutionary operations.

In this application, it is used to search for an advantageous combination of the three photovoltaic panel categories.

The implementation uses the `best1bin` strategy and allows up to 500 iterations.

Since the optimization variables represent discrete panel counts, the candidate values are converted to integers before evaluating the objective function.

The optimization process ultimately returns the recommended panel combination and its estimated five-year NPV.

## 8. Interactive Web Application

The user interface is implemented using Streamlit, allowing the optimization and analysis functionality to be accessed through a browser without requiring a separately developed frontend.

The application provides controls for specifying:

- The minimum desired energy self-sufficiency.
- The maximum number of panels that can be installed.

After the user starts the optimization, Streamlit displays a progress message while the algorithm searches for a suitable configuration.

Once the optimization is complete, the application presents:

- The optimal number of panels in each category.
- The estimated investment NPV after five years.
- Comparative consumption and production graphs.
- Self-consumption and self-sufficiency indicators.
- Net energy exchanged with the grid.

The use of Streamlit makes the underlying Python functionality accessible through an interactive interface, connecting the database, optimization engine, and analytical components into a single application.

## 9. Project Structure

```text
PanelConfigurator/
│
├── main.py
│   └── Application entry point, optimization,
│       financial modelling, database access,
│       and Streamlit interface.
│
├── analysis_module.py
│   └── Domain classes, database access,
│       energy analysis, technical indicators,
│       and visualization.
│
├── README.md
│   └── Project documentation.
│
└── caiet practica.txt
    └── Practical training activity log.
```

The SQLite database (`irise.sqlite3`) is expected to be available locally in the application's working directory. It is not included among the tracked repository files.

## 10. Installation and Execution

### Prerequisites

- Python 3.9 or later.
- pip package manager.
- The SQLite database file required by the application.

### Install dependencies

```bash
pip install numpy scipy matplotlib streamlit
```

### Run the application

Place `irise.sqlite3` in the project's working directory and execute:

```bash
streamlit run main.py
```

The application will start a local Streamlit server and provide a browser-based interface.

## 11. Skills and Concepts Demonstrated

This project combines several areas of software development and applied engineering:

- **Object-Oriented Programming:** domain modelling, encapsulation, class design, and separation of responsibilities.
- **Database Management:** relational database interaction, SQL querying, parameterized queries, and structured data retrieval using SQLite.
- **Data Engineering:** extraction, aggregation, transformation, and temporal alignment of consumption and meteorological data.
- **Numerical Computing:** vectorized and statistical computations using NumPy.
- **Optimization:** application of Differential Evolution to a constrained, mixed-integer configuration problem.
- **Financial Modelling:** NPV calculation, investment evaluation, and energy cost analysis.
- **Data Visualization:** time-series analysis and graphical representation using Matplotlib.
- **Application Development:** integration of computational logic into an interactive web application using Streamlit.
- **Software Design:** modular organization, type annotations, and separation between data access, analytical logic, and user interaction.

## 12. Project Context

PanelConfigurator was developed during the mandatory summer practical training programme undertaken in the third year of the Bachelor's degree in Automatic Control and Computer Science.

The work progressed from household energy consumption and solar irradiation analysis to the development of technical indicators, an optimization engine, and an interactive web application.

The project provided practical experience in combining mathematical optimization with software engineering, database management, and data visualization to address an applied renewable energy problem.

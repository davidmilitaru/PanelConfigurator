import numpy as np
import datetime
from scipy.optimize import differential_evolution
from typing import Dict, Tuple, List
import sqlite3
import streamlit as st
from analysis_module import EnergyAnalysis, Consumer, House, Production, Rayonnement

# Panel characteristics
panels = {
    "Low-cost": {"power": 200, "cost_per_watt": 0.16},
    "Standard": {"power": 300, "cost_per_watt": 0.22},
    "High Efficiency": {"power": 415, "cost_per_watt": 0.31},
}

# Define the known parameters
c_grid = 0.25  # Cost of energy from the grid
c_DAM = 0.1   # Cost of energy injected into the grid (Day Ahead Market)
r = 0.05      # Discount rate
Y = 5        # Time period (in years)
surface_per_panel = 1.6 # Solar panel dimension (squared meters)

# Streamlit interface
st.title("Optimizarea configurației unui sistem fotovoltaic")
st.markdown("""
Programul determină configurația optimă a unui sistem de panouri fotovoltaice pe baza procentului de autosuficiență dorit de utilizator, maximizând valoarea investiției după o perioadă de 5 ani.
Ținând cont de fluctuațiile în ce privește capacitatea de producție (iarna vs. vara și ziua vs. noaptea), pentru o configurație eficientă se recomandă un procent de autosuficiență între **20%** și **35%**.
Programul alege dintr-o gamă de 3 panouri fotovoltaice: 
            
- **Low-Cost**: 200W putere nominală, 0.16 euro cost per kW;
            
- **Standard**: 300W putere nominală, 0.22 euro cost per kW;
            
- **High-Efficiency**: 415W putere nominală, 0.31 euro cost per kW;
    
**Această aplicație vă permite să calculați numărul optim de panouri fotovoltaice pe baza dimensiunilor suprafeței disponibile. 
Dimensiunile unui panou sunt de 1.6m lungime și 1m lățime. Determinați, mai întâi, numărul maxim de panouri ce pot fi amplasate pe suprafața locuinței.**

""")

# Introduce the restraints
min_self_sufficiency = st.number_input("Introduceți procentul minim de autosuficiență dorit (0-1): ", min_value=0.0, max_value=1.0, value=0.5)
max_panels = st.number_input("Introduceți numărul maxim de panouri ce pot fi amplasate pe suprafața utilă a locuinței: ", min_value=1)

# DB Class
class DatabaseManager:
    def __init__(self, db_name: str):
        self.db_name = db_name
        self.connection = None
        self.cursor = None

    def __enter__(self):
        self.connection = sqlite3.connect(self.db_name)
        self.cursor = self.connection.cursor()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def get_cons_by_constype(self, locuinta_id: int, consumer_id: int, consumer: str) -> List[Consumer]:
        self.cursor.execute(
            'SELECT ApplianceIDREF, EpochTime, Value FROM Consumption WHERE HouseIDREF = ? AND ApplianceIDREF = ? AND EpochTime >= 886709400 AND EpochTime <= 918244800',
            (locuinta_id, consumer_id))
        rows = self.cursor.fetchall()

        consumers = []
        for i in range(0, len(rows), 6):
            group = rows[i:i + 6]
            if len(group) < 6:
                break

            ApplianceIDREF = group[0][0]
            EpochTime = group[5][1]
            Value = sum(row[2] for row in group)

            consumer_obj = Consumer(ApplianceIDREF, consumer, datetime.datetime.utcfromtimestamp(EpochTime), Value)
            consumers.append(consumer_obj)

        return consumers

    def get_rayonnement(self, station_id: int, parameter_id: int) -> List[Rayonnement]:
        self.cursor.execute(
            'SELECT Value, EpochTime FROM WeatherData WHERE WeatherStationIDREF = ? AND WeatherVariableIDREF = ? AND EpochTime >= 886712400 AND EpochTime <= 918244800',
            (station_id, parameter_id))
        rows = self.cursor.fetchall()
        return [Rayonnement(Value, datetime.datetime.fromtimestamp(EpochTime)) for Value, EpochTime in rows]

    def get_production(self, station_id: int, parameter_id: int, nominal_power: int, nr_panels: int, f: float) -> List[Production]:
        self.cursor.execute(
            'SELECT Value, EpochTime FROM WeatherData WHERE WeatherStationIDREF = ? AND WeatherVariableIDREF = ? AND EpochTime >= 886712400 AND EpochTime <= 918244800',
            (station_id, parameter_id))
        rows = self.cursor.fetchall()
        return [Production(nominal_power * nr_panels * f * Value / 1000, datetime.datetime.fromtimestamp(EpochTime)) for
                Value, EpochTime in rows]

    def close(self):
        if self.connection:
            self.connection.close()

# Extend the dataset to 5 years
def extend_data_to_years(
    hourly_production: Dict[datetime.datetime, float],
    hourly_consumption: Dict[datetime.datetime, float],
    years: int
) -> Tuple[Dict[datetime.datetime, float], Dict[datetime.datetime, float]]:
    extended_production = {}
    extended_consumption = {}

    for year in range(years):
        for time, production in hourly_production.items():
            new_time = time.replace(year=time.year + year)
            extended_production[new_time] = production
        
        for time, consumption in hourly_consumption.items():
            new_time = time.replace(year=time.year + year)
            extended_consumption[new_time] = consumption

    return extended_production, extended_consumption

# Compute monthly costs
def calculate_monthly_costs(
    hourly_production: Dict[datetime.datetime, float],
    hourly_consumption: Dict[datetime.datetime, float],
    c_grid: float,
    c_DAM: float
) -> Dict[Tuple[int, int], Tuple[float, float]]:
    monthly_costs = {}
    monthly_e_grid = {}
    monthly_e_injected = {}

    for time, consumption in hourly_consumption.items():
        production = hourly_production.get(time, 0)
        year_month = (time.year, time.month)

        if year_month not in monthly_e_grid:
            monthly_e_grid[year_month] = 0.0
        if year_month not in monthly_e_injected:
            monthly_e_injected[year_month] = 0.0

        if consumption > production:
            # Energy taken from the grid
            monthly_e_grid[year_month] += (consumption - production) / 1000
        else:
            # Energy injected to the grid
            monthly_e_injected[year_month] += (production - consumption) / 1000

    for year_month in monthly_e_grid:
        # Cost without panels
        cost_without_panels = sum(
            hourly_consumption[time] for time in hourly_consumption if (time.year, time.month) == year_month
        ) / 1000 * c_grid

        # Cost with panels
        e_grid = monthly_e_grid[year_month]
        e_injected = monthly_e_injected[year_month]
        cost_with_panels = e_grid * c_grid - e_injected * c_DAM

        monthly_costs[year_month] = (cost_without_panels, cost_with_panels)

    return monthly_costs


# Compute NPV
def calculate_npv(
    monthly_costs: Dict[Tuple[int, int], Tuple[float, float]],
    CapEX: float,
    r: float,
    Y: int
) -> float:
    OpEX = 0.03 * CapEX
    npv = -CapEX
    sorted_months = sorted(monthly_costs.keys())  # Sortează cheile care sunt acum de tip (an, luna)
    monthly_npv_values = []

    for t, year_month in enumerate(sorted_months, start=1):
        cost_without_panels, cost_with_panels = monthly_costs[year_month]
        G_t = cost_without_panels - cost_with_panels
        npv += (G_t - (OpEX / 12)) / ((1 + r) ** ((t - 1) / 12))
        monthly_npv_values.append((year_month, npv))

    return npv, monthly_npv_values


def npv_function(x):
    num_low_cost = int(x[0])
    num_standard = int(x[1])
    num_high_efficiency = int(x[2])
    x = [int(x[0]), int(x[1]), int(x[2])]

    CapEX = (
        num_low_cost * panels["Low-cost"]["power"] * panels["Low-cost"]["cost_per_watt"] +
        num_standard * panels["Standard"]["power"] * panels["Standard"]["cost_per_watt"] +
        num_high_efficiency * panels["High Efficiency"]["power"] * panels["High Efficiency"]["cost_per_watt"]
    )

    nominal_power_low = panels["Low-cost"]["power"]
    nominal_power_standard = panels["Standard"]["power"]
    nominal_power_high = panels["High Efficiency"]["power"]

    with DatabaseManager("irise.sqlite3") as db_manager:
        # Calculate the consumption and production based on the current panel configuration
        production_low = db_manager.get_production(
            station_id=26198001,
            parameter_id=4,
            nominal_power=nominal_power_low,
            nr_panels=num_low_cost,
            f=0.8
        )

        production_standard = db_manager.get_production(
            station_id=26198001,
            parameter_id=4,
            nominal_power=nominal_power_standard,
            nr_panels=num_standard,
            f=0.8
        )

        production_high = db_manager.get_production(
            station_id=26198001,
            parameter_id=4,
            nominal_power=nominal_power_high,
            nr_panels=num_high_efficiency,
            f=0.8
        )
        
        hourly_production = {prod.time: prod.val for prod in production_low}
        for prod in production_standard:
            if prod.time in hourly_production:
                hourly_production[prod.time] += prod.val
            else:
                hourly_production[prod.time] = prod.val
        for prod in production_high:
            if prod.time in hourly_production:
                hourly_production[prod.time] += prod.val
            else:
                hourly_production[prod.time] = prod.val

        consumers_data = [
            db_manager.get_cons_by_constype(2000916, 0, 'water pump'),
            db_manager.get_cons_by_constype(2000916, 1, 'water heater'),
            db_manager.get_cons_by_constype(2000916, 2, 'washing machine'),
            db_manager.get_cons_by_constype(2000916, 4, 'freezer'),
            db_manager.get_cons_by_constype(2000916, 5, 'fridge freezer'),
            db_manager.get_cons_by_constype(2000916, 6, 'total site light'),
            db_manager.get_cons_by_constype(2000916, 7, 'TV'),
            db_manager.get_cons_by_constype(2000916, 9, 'boiler')
        ]

        hourly_consumption = {}
        for consumer_list in consumers_data:
            for consumer in consumer_list:
                time_key = consumer.time
                if time_key not in hourly_consumption:
                    hourly_consumption[time_key] = 0.0
                hourly_consumption[time_key] += consumer.val

    # Extend to 5 years
    hourly_production_extended, hourly_consumption_extended = extend_data_to_years(hourly_production, hourly_consumption, Y)

    # Compute monthly costs
    monthly_costs = calculate_monthly_costs(hourly_production_extended, hourly_consumption_extended, c_grid, c_DAM)

    # Compute NPV
    npv, _ = calculate_npv(monthly_costs, CapEX, r, Y)

    # Compute SS
    sum_total_consumption = sum(hourly_consumption_extended.values())
    sum_min_prod_load = sum(min(hourly_consumption_extended[time], hourly_production_extended.get(time, 0)) for time in hourly_consumption_extended)

    self_sufficiency = sum_min_prod_load / sum_total_consumption
    print(npv, self_sufficiency, num_low_cost + num_standard + num_high_efficiency)

    # Verify panel number
    if num_low_cost + num_standard + num_high_efficiency > max_panels:
        return float('1000000')
 
    # Verify SS constraint and add weights based on how far from the target the solution is
    if self_sufficiency < min_self_sufficiency - 0.1:
        return float('500000')
    if self_sufficiency < min_self_sufficiency - 0.05:
        return float('450000')
    if self_sufficiency < min_self_sufficiency - 0.03:
        return float('400000')
    if self_sufficiency < min_self_sufficiency - 0.02:
        return float('350000')
    if self_sufficiency < min_self_sufficiency - 0.01:
        return float('300000')
    if self_sufficiency < min_self_sufficiency:
        return float(250000 + (min_self_sufficiency - self_sufficiency) * 1000000)

    return -npv

# Run optimization for each panel type
if st.button("Rulează programul"):
    # Container for text display while running, then erase after finish
    text_container = st.empty()
    text_container.write("Se determină configurația optimă... (poate dura aprox. 30 minute)")

    bounds = [(0, max_panels), (0, max_panels), (0, max_panels)]  # limits for panel number

    result = differential_evolution(npv_function, bounds, strategy='best1bin', maxiter=500)
    optimal_config = [int(result.x[0]), int(result.x[1]), int(result.x[2])]
    optimal_npv = -result.fun

    text_container.empty()
    st.write(f"Configurația optimă: {optimal_config[0]} panouri Low-Cost, {optimal_config[1]} panouri Standard, {optimal_config[2]} panouri High-Efficiency")
    st.write(f"NPV-ul investiției după 5 ani: {optimal_npv}")

    db_name = "irise.sqlite3"
    house_id = 2000916
    station_id = 26198001
    nr_panels = optimal_config[0] + optimal_config[1] + optimal_config[2]
    nominal_power = (optimal_config[0] * panels["Low-cost"]["power"] + optimal_config[1] * panels["Standard"]["power"] + optimal_config[2] * panels["High Efficiency"]["power"]) / nr_panels
    f = 0.8

    analysis = EnergyAnalysis(db_name, house_id, station_id, nominal_power, nr_panels, f)

    analysis.plot_consumption_vs_production()
    analysis.plot_consumption_vs_production_monthly()
    analysis.plot_consumption_vs_production_weekly()
    analysis.plot_consumption_vs_production_daily()
    analysis.plot_self_consumption()
    analysis.plot_self_sufficiency()
    analysis.plot_neeg()

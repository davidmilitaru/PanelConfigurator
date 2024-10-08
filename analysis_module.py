import datetime
from typing import List, Dict, Tuple
import sqlite3
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import streamlit as st

# Helper Classes
class Consumer:
    def __init__(self, type_id: int, type: str, time: datetime.datetime, val: float):
        self.type_id = type_id 
        self.type = type 
        self.val = val
        self.time = time

    def __repr__(self):
        return f"<Consumer type={self.type}, value={self.val}, start={self.time}>"

class House:
    def __init__(self, id_house: int, city: str):
        self.id_house = id_house
        self.city = city

    def __repr__(self):
        return f"<House id={self.id_house}, address={self.city}>"

class Production:
    def __init__(self, val: float, time: datetime.datetime):
        self.val = val
        self.time = time

    def __repr__(self):
        return f"<value={self.val}W, hour={self.time}>"

class Rayonnement:
    def __init__(self, val: float, time: datetime.datetime):
        self.val = val
        self.time = time

    def __repr__(self):
        return f"<value={self.val}, hour={self.time}>"
    
class DatabaseManager:
    def __init__(self, db_name: str):
        self.connection = sqlite3.connect(db_name)
        self.cursor = self.connection.cursor()

    def get_cons_by_constype(self, locuinta_id: int, consumer_id : int, consumer : str) -> List[Consumer]:
        self.cursor.execute('SELECT ApplianceIDREF, EpochTime, Value FROM Consumption WHERE HouseIDREF = ? AND ApplianceIDREF = ? AND EpochTime >= 886709400 AND EpochTime <= 918244800', (locuinta_id, consumer_id))
        rows = self.cursor.fetchall()
        consumers = []
        for i in range(0, len(rows), 6):
            group = rows[i:i+6]
            if len(group) < 6:
                break 

            ApplianceIDREF = group[0][0]
            EpochTime = group[5][1]
            Value = sum(row[2] for row in group)
            
            consumer_obj = Consumer(ApplianceIDREF, consumer, datetime.datetime.utcfromtimestamp(EpochTime), Value)
            consumers.append(consumer_obj)
        
        return consumers
    
    def get_rayonnement(self, station_id: int, parameter_id : int) -> List[Rayonnement]:
        self.cursor.execute('SELECT Value, EpochTime FROM WeatherData WHERE WeatherStationIDREF = ? AND WeatherVariableIDREF = ? AND EpochTime >= 886712400 AND EpochTime <= 918244800', (station_id, parameter_id))
        rows = self.cursor.fetchall()
        return [Rayonnement(Value, datetime.datetime.fromtimestamp(EpochTime)) for Value, EpochTime in rows]

    def get_production(self, station_id : int, parameter_id : int, nominal_power: int, nr_panels : int, f : float) -> List[Production]:
        self.cursor.execute('SELECT Value, EpochTime FROM WeatherData WHERE WeatherStationIDREF = ? AND WeatherVariableIDREF = ? AND EpochTime >= 886712400 AND EpochTime <= 918244800', (station_id, parameter_id))
        rows = self.cursor.fetchall()
        return [Production(nominal_power * nr_panels * f * Value/1000, datetime.datetime.fromtimestamp(EpochTime)) for Value, EpochTime in rows]

    def close(self):
        self.connection.close()

# Main Class for plotting

class EnergyAnalysis:
    def __init__(self, db_name: str, house_id: int, station_id: int, nominal_power: float, nr_panels: int, f: float):
        self.db_name = db_name
        self.house_id = house_id
        self.station_id = station_id
        self.nominal_power = nominal_power
        self.nr_panels = nr_panels
        self.f = f

        self.db_manager = DatabaseManager(db_name)
        self.consumers_data = self.load_consumers_data()
        self.rayonnement_data = self.load_rayonnement_data()
        self.production_data = self.load_production_data()

        self.hourly_consumption = self.aggregate_hourly_consumption()
        self.hourly_production = {prod.time: prod.val for prod in self.production_data}

    def load_consumers_data(self) -> List[List[Consumer]]:
        return [
            self.db_manager.get_cons_by_constype(self.house_id, 0, 'water pump'),
            self.db_manager.get_cons_by_constype(self.house_id, 1, 'water heater'),
            self.db_manager.get_cons_by_constype(self.house_id, 2, 'washing machine'),
            self.db_manager.get_cons_by_constype(self.house_id, 4, 'freezer'),
            self.db_manager.get_cons_by_constype(self.house_id, 5, 'fridge freezer'),
            self.db_manager.get_cons_by_constype(self.house_id, 6, 'total site light'),
            self.db_manager.get_cons_by_constype(self.house_id, 7, 'TV'),
            self.db_manager.get_cons_by_constype(self.house_id, 9, 'boiler')
        ]

    def load_rayonnement_data(self) -> List[Rayonnement]:
        return self.db_manager.get_rayonnement(self.station_id, 4)

    def load_production_data(self) -> List[Production]:
        return self.db_manager.get_production(self.station_id, 4, self.nominal_power, self.nr_panels, self.f)

    def close(self):
        self.db_manager.close()

    ## 1.1. Aggregate hourly consumption values
     
    def aggregate_hourly_consumption(self) -> Dict[datetime.datetime, float]:
        hourly_consumption = {}
        for consumer_list in self.consumers_data:
            for consumer in consumer_list:
                time_key = consumer.time
                if time_key not in hourly_consumption:
                    hourly_consumption[time_key] = 0.0
                hourly_consumption[time_key] += consumer.val
        return hourly_consumption

    ## 1.2. Plot the consumption versus production data for 1 year

    def plot_consumption_vs_production(self):
        fig, ax = plt.subplots(figsize=(15, 7))
        ax.plot(self.hourly_consumption.keys(), self.hourly_consumption.values(), label='Consumul orar')
        ax.plot(self.hourly_production.keys(), self.hourly_production.values(), label='Producția orară', linestyle='--')

        fig.gca().xaxis.set_major_locator(mdates.MonthLocator())
        fig.gca().xaxis.set_major_formatter(mdates.DateFormatter('%d %b %Y'))

        ax.set_xlabel('Data')
        ax.set_ylabel('Energia (W)')
        ax.set_title('Consumul și producția orară pe un an')
        ax.legend()
        ax.grid(True)

        plt.gcf().autofmt_xdate()
        st.pyplot(fig)

    ## 1.3. Plot the consumption versus production data for 1 month

    def plot_consumption_vs_production_monthly(self):

        # Calculate the average consumption for each day and hour
        hourly_consumption_month: Dict[Tuple[int, int], List[float]] = {}
        for key in self.hourly_consumption.keys():
            day_hour_key = (key.day, key.hour)
            if day_hour_key not in hourly_consumption_month:
                hourly_consumption_month[day_hour_key] = []
            hourly_consumption_month[day_hour_key].append(self.hourly_consumption[key])

        average_consumption = {}
        for key in hourly_consumption_month:
            average_consumption[key] = np.mean(hourly_consumption_month[key])

        # Prepare data for plotting
        times = []
        values = []
        year = 2000
        month = 1

        for day in range(1, 32): 
            for hour in range(24):
                time_key = (day, hour)
                if time_key in average_consumption:
                    times.append(datetime.datetime(year, month, day, hour))
                    values.append(average_consumption[time_key])


        # Calculate the average production for each day and hour
        hourly_production_month = {}
        for key in self.hourly_production.keys():
            day_hour_key = (key.day, key.hour)
            if day_hour_key not in hourly_production_month:
                hourly_production_month[day_hour_key] = []
            hourly_production_month[day_hour_key].append(self.hourly_production[key])

        average_production = {key: np.mean(hourly_production_month[key]) for key in hourly_production_month}

        # Prepare data for plotting
        times_prod = []
        values_prod = []

        for day in range(1, 32):
            for hour in range(24):
                time_key = (day, hour)
                if time_key in average_production:
                    times_prod.append(datetime.datetime(year, month, day, hour))
                    values_prod.append(average_production[time_key])

        # Plot the data
        fig, ax = plt.subplots(figsize=(15, 7))
        ax.plot(times, values, label='Consumul mediu orar pe o lună')
        ax.plot(times_prod, values_prod, label='Producția medie orară pe o lună', linestyle='--')

        fig.gca().xaxis.set_major_locator(mdates.DayLocator())
        fig.gca().xaxis.set_major_formatter(mdates.DateFormatter('%d %H:%M'))

        ax.set_xlabel('Ziua și ora')
        ax.set_ylabel('Energia (W)')
        ax.set_title('Consumul și producția orară medie pe o lună')
        ax.legend()
        ax.grid(True)

        plt.gcf().autofmt_xdate()
        st.pyplot(fig)

    ## 1.4. Plot the consumption versus production data for 1 week

    def plot_consumption_vs_production_weekly(self):
        hourly_consumption_week: Dict[Tuple[int, int], List[float]] = {}
        for key in self.hourly_consumption.keys():
            week_hour_key = (key.weekday(), key.hour)
            if week_hour_key not in hourly_consumption_week:
                hourly_consumption_week[week_hour_key] = []
            hourly_consumption_week[week_hour_key].append(self.hourly_consumption[key])

        # Calculate the average consumption for each day and hour of the week
        average_consumption_week = {}
        for key in hourly_consumption_week:
            average_consumption_week[key] = np.mean(hourly_consumption_week[key])

        # Sort the dictionary by day of the week and hour
        sorted_average_consumption_week = dict(sorted(average_consumption_week.items()))

        # Prepare data for plotting
        times = []
        values = []
        start_date = datetime.datetime(2000, 1, 3)  # This is a Monday

        for day, hour in sorted_average_consumption_week.keys():
            times.append(start_date + datetime.timedelta(days=day, hours=hour))
            values.append(sorted_average_consumption_week[(day, hour)])

        # Compute the production data for 1 week + cons
        hourly_production_week = {}
        for key in self.hourly_production.keys():
            week_hour_key = (key.weekday(), key.hour)
            if week_hour_key not in hourly_production_week:
                hourly_production_week[week_hour_key] = []
            hourly_production_week[week_hour_key].append(self.hourly_production[key])

        # Calculate the average production for each day and hour of the week
        average_production_week = {key: np.mean(hourly_production_week[key]) for key in hourly_production_week}

        # Sort the dictionary by day of the week and hour
        sorted_average_production_week = dict(sorted(average_production_week.items()))

        # Prepare data for plotting
        times_prod = []
        values_prod = []
        start_date = datetime.datetime(2000, 1, 3)

        for day, hour in sorted_average_production_week.keys():
            times_prod.append(start_date + datetime.timedelta(days=day, hours=hour))
            values_prod.append(sorted_average_production_week[(day, hour)])

        # Plot the data
        fig, ax = plt.subplots(figsize=(15, 7))
        ax.plot(times, values, label='Consumul mediu orar pe o săptămână')
        ax.plot(times_prod, values_prod, label='Producșia medie orară pe o săptămână', linestyle='--')

        fig.gca().xaxis.set_major_locator(mdates.HourLocator(interval=6))
        fig.gca().xaxis.set_major_formatter(mdates.DateFormatter('%a %H:%M'))

        ax.set_xlabel('Ziua și ora')
        ax.set_ylabel('Energia (W)')
        ax.set_title('Consumul și producția medie orară pe o săptămână')
        ax.legend()
        ax.grid(True)

        plt.gcf().autofmt_xdate()
        st.pyplot(fig)

    ## 1.5. Plot the consumption versus production data for 1 day

    def plot_consumption_vs_production_daily(self):
        hourly_consumption_day: Dict[int, List[float]] = {}
        for key in self.hourly_consumption.keys():
            hour_key = key.hour
            if hour_key not in hourly_consumption_day:
                hourly_consumption_day[hour_key] = []
            hourly_consumption_day[hour_key].append(self.hourly_consumption[key])

        # Calculate the average consumption for each hour of the day
        average_consumption_day = {}
        for key in hourly_consumption_day:
            average_consumption_day[key] = np.mean(hourly_consumption_day[key])

        # Sort the dictionary by hour
        sorted_average_consumption_day = dict(sorted(average_consumption_day.items()))

        # Prepare data for plotting
        times = []
        values = []
        generic_date = datetime.datetime(2000, 1, 1)

        for hour in sorted_average_consumption_day.keys():
            times.append(generic_date + datetime.timedelta(hours=hour))
            values.append(sorted_average_consumption_day[hour])

        # Compute the production data for 1 day
        hourly_production_day = {}
        for key in self.hourly_production.keys():
            hour_key = key.hour
            if hour_key not in hourly_production_day:
                hourly_production_day[hour_key] = []
            hourly_production_day[hour_key].append(self.hourly_production[key])

        # Calculate the average production for each hour of the day
        average_production_day = {key: np.mean(hourly_production_day[key]) for key in hourly_production_day}

        # Sort the dictionary by hour
        sorted_average_production_day = dict(sorted(average_production_day.items()))

        # Prepare data for plotting
        times_prod = []
        values_prod = []
        generic_date = datetime.datetime(2000, 1, 1)  # This is a generic date

        for hour in sorted_average_production_day.keys():
            times_prod.append(generic_date + datetime.timedelta(hours=hour))
            values_prod.append(sorted_average_production_day[hour])

        # Plot the data
        fig, ax = plt.subplots(figsize=(15, 7))
        ax.plot(times, values, label='Consumul mediu orar pe zi')
        ax.plot(times_prod, values_prod, label='Producția medie orară pe zi', linestyle='--')

        fig.gca().xaxis.set_major_locator(mdates.HourLocator(interval=1))
        fig.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))

        ax.set_xlabel('Ora')
        ax.set_ylabel('Energia (W)')
        ax.set_title('Consumul și producția medie orară pe o zi')
        ax.legend()
        ax.grid(True)

        plt.gcf().autofmt_xdate()
        st.pyplot(fig)


    ### PART 2 - TECHNICAL INDICATOR ANALYSIS

    ## 2.1. Self-Consumption

    def calculate_self_consumption_daily(self) -> Dict[datetime.date, float]:   
        daily_self_consumption = {}
        grouped_production = {}
        grouped_consumption = {}

        for time, production in self.hourly_production.items():
            day = time.date()
            if day not in grouped_production:
                grouped_production[day] = []
            grouped_production[day].append(production)
    
        for time, consumption in self.hourly_consumption.items():
            day = time.date()
            if day not in grouped_consumption:
                grouped_consumption[day] = []
            grouped_consumption[day].append(consumption)
    
        # Compute daily SC
        for day in grouped_production.keys():
            if day in grouped_consumption:
                prod_values = grouped_production[day]
                cons_values = grouped_consumption[day]
            
                sum_min_prod_load = sum(min(p, c) for p, c in zip(prod_values, cons_values))
                sum_total_production = sum(prod_values)

                daily_self_consumption[day] = sum_min_prod_load / sum_total_production if sum_total_production != 0 else 0
    
        return daily_self_consumption

    def calculate_total_self_consumption(self, daily_self_consumption: Dict[datetime.date, float]) -> float:
        # Compute SC for the whole year
        return sum(daily_self_consumption.values()) / len(daily_self_consumption)
    
    def plot_self_consumption(self):
        daily_self_consumption = self.calculate_self_consumption_daily()
        total_self_consumption = self.calculate_total_self_consumption(daily_self_consumption)
        st.write(f"Autoconsumul: {total_self_consumption:.2%}")

        # Plot the SC
        fig, ax = plt.subplots(figsize=(15, 7))
        ax.plot(daily_self_consumption.keys(), daily_self_consumption.values(), label='Autoconsumul zilnic', marker='o')

        ax.set_xlabel('Data')
        ax.set_ylabel('Autoconsumul (%)')
        ax.set_title('Autoconsumul zilnic')
        ax.legend()
        ax.grid(True)

        fig.gca().xaxis.set_major_formatter(mdates.DateFormatter('%d %b %Y'))
        plt.gcf().autofmt_xdate() 
        st.pyplot(fig)

    ## 2.2. Self-Sufficiency

    def calculate_self_sufficiency_daily(self) -> Dict[datetime.date, float]:    
        daily_self_sufficiency = {}
        grouped_production = {}
        grouped_consumption = {}

        for time, production in self.hourly_production.items():
            day = time.date()
            if day not in grouped_production:
                grouped_production[day] = []
            grouped_production[day].append(production)
    
        for time, consumption in self.hourly_consumption.items():
            day = time.date()
            if day not in grouped_consumption:
                grouped_consumption[day] = []
            grouped_consumption[day].append(consumption)
    
        # Compute daily SS
        for day in grouped_production.keys():
            if day in grouped_consumption:
                prod_values = grouped_production[day]
                cons_values = grouped_consumption[day]
            
                sum_min_prod_load = sum(min(p, c) for p, c in zip(prod_values, cons_values))
                sum_total_consumption = sum(cons_values)

                daily_self_sufficiency[day] = sum_min_prod_load / sum_total_consumption if sum_total_consumption != 0 else 0
    
        return daily_self_sufficiency

    def calculate_total_self_sufficiency(self, daily_self_sufficiency: Dict[datetime.date, float]) -> float:
        # Compute SS for the whole year
        return sum(daily_self_sufficiency.values()) / len(daily_self_sufficiency)
    
    def plot_self_sufficiency(self):
        daily_self_sufficiency = self.calculate_self_sufficiency_daily()
        total_self_sufficiency = self.calculate_total_self_sufficiency(daily_self_sufficiency)
        st.write(f"Autosuficiența: {total_self_sufficiency:.2%}")

        # Plot the SS
        fig, ax = plt.subplots(figsize=(15, 7))
        ax.plot(daily_self_sufficiency.keys(), daily_self_sufficiency.values(), label='Autosuficiența zilnică', marker='o')

        ax.set_xlabel('Data')
        ax.set_ylabel('Autosuficiența (%)')
        ax.set_title('Autosuficiența zilnică')
        ax.legend()
        ax.grid(True)

        fig.gca().xaxis.set_major_formatter(mdates.DateFormatter('%d %b %Y'))
        plt.gcf().autofmt_xdate()
        st.pyplot(fig)

    # 2.3. NEEG (Net Energy Exchanged with the Grid)

    def calculate_neeg_daily(self) -> Dict[datetime.date, float]:
        daily_neeg = {}
        grouped_production = {}
        grouped_consumption = {}

        for time, production in self.hourly_production.items():
            day = time.date()
            if day not in grouped_production:
                grouped_production[day] = []
            grouped_production[day].append(production)
    
        for time, consumption in self.hourly_consumption.items():
            day = time.date()
            if day not in grouped_consumption:
                grouped_consumption[day] = []
            grouped_consumption[day].append(consumption)
    
        # Calculate daily NEEG in kWh
        for day in grouped_production.keys():
            if day in grouped_consumption:
                prod_values = grouped_production[day]
                cons_values = grouped_consumption[day]
            
                sum_neeg = sum(abs(p - c) for p, c in zip(prod_values, cons_values))/1000

                daily_neeg[day] = sum_neeg
    
        return daily_neeg

    def calculate_total_neeg(self, daily_neeg: Dict[datetime.date, float]) -> float:
        # Calculate NEEG for the whole year
        return sum(daily_neeg.values()) 
    
    def plot_neeg(self):
        daily_neeg = self.calculate_neeg_daily()
        total_neeg = self.calculate_total_neeg(daily_neeg)

        st.write(f"NEEG pe durata întregului an: {total_neeg:.2f} kWh")

        # Plot the NEEG
        fig, ax = plt.subplots(figsize=(15, 7))
        ax.plot(daily_neeg.keys(), daily_neeg.values(), label='NEEG zilnic', marker='o')

        ax.set_xlabel('Data')
        ax.set_ylabel('NEEG (kWh)')
        ax.set_title('Energia netă interschimbată cu rețeaua (NEEG)')
        ax.legend()
        ax.grid(True)

        fig.gca().xaxis.set_major_formatter(mdates.DateFormatter('%d %b %Y'))
        plt.gcf().autofmt_xdate() 
        st.pyplot(fig)

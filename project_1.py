import requests
import pandas as pd
from datetime import datetime
url = "https://earthquake.usgs.gov/fdsnws/event/1/query"
all_records = []
start_year = datetime.now().year - 5
end_year = datetime.now().year
for year in range(start_year, end_year):
    for month in range(1, 13):
        start_date = f"{year}-{month:02d}-01"
        if month == 12:
            end_date = f"{year+1}-01-01"
        else:
            end_date = f"{year}-{month+1:02d}-01"
            
        params = {
             "format" : "geojson",
             "starttime": start_date,
              "endtime" : end_date,
             "minmagnitude" :3

         }
        response = requests.get(url,params=params)
        if response.status_code != 200:
            print(f" failed for {start_date} : {response.text[200]}")
            continue

        try:
            data = response.json()
        except Exception as e :
            continue
        for f in data.get("features",[]):
            p = f["properties"]
            g = f["geometry"]
            all_records.append({
                "id" : f.get("id"),
                "time" : pd.to_datetime(p.get("time"), unit = "ms"),
                "updated" : pd.to_datetime(p.get("updated"), unit = "ms"),
                "place" : p.get("place"),
                "mag" : p.get("mag"),
                "magtype" : p.get("magType"),
                "type" : p.get("type"),
                "latitude" : g["coordinates"][1] if g else None,
                "longitude" : g["coordinates"][0] if g else None,
                "depth_km" : g["coordinates"][2] if g else None,
                "status" : p.get("status"),
                "tsunami" : p.get("tsunami"),
                "alert" : p.get("alert"),
                "felt" : p.get("felt"),
                "cdi" : p.get("cdi"),
                "mmi" : p.get("mmi"),
                "sig" : p.get("sig"),
                "net" : p.get("net"),
                "code" : p.get("code"),
                "ids": p.get("ids"),
                "sources": p.get("sources"),
                "types": p.get("types"),
                "nst": p.get("nst"),
                "dmin": p.get("dmin"),
                "rms": p.get("rms"),
                "gap": p.get("gap"),
                "type": p.get("type")
            
            })
df = pd.DataFrame(all_records)     
import pandas as pd
# Load your CSV
df = pd.read_csv("c:/Users/manoj/earthquake_data.csv")
print(df.shape)
df['mmi'] = df['mmi'].fillna(df['mmi'].median())
df['cdi'] = df['cdi'].fillna(df['cdi'].median())
df['dmin'] = df['dmin'].fillna(df['dmin'].median())
df['rms'] = df['rms'].fillna(df['rms'].median())
df['alert'] = df['alert'].fillna(df['alert'].mode()[0])
df['nst'] = df['nst'].fillna(df['nst'].mode()[0])
df['felt'] = df['felt'].fillna(df['felt'].mode()[0])
df['gap'] = df['gap'].fillna(df['gap'].median())
df['rms'] = df['rms'].fillna(df['rms'].mean())
import pandas as pd
from sqlalchemy import create_engine

# Load CSV
csv_file = ("C:/Users/manoj/earthquake_cleaneddata.csv")
df = pd.read_csv(csv_file)

# MySQL connection details
username = "root"
password = "9786294039"
host = "localhost"   # <-- define host
port = 3306
database = "earthquake"

connection_string = f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}"
engine = create_engine(connection_string)
df['country'] = df['place'].apply(lambda x: x.split(',')[-1].strip() if ',' in str(x) else x)
# Save data to MySQL
df.to_sql("earthquake_data", con=engine, if_exists="replace", index=False)
print("CSV data successfully saved to MySQL table 'earthquake_data'")

df.to_sql("earthquakes", con=engine, if_exists="replace", index=False)


# Dictionary of 26 queries
queries = {
    "Top 10 strongest earthquakes": """
        SELECT * FROM earthquake_data ORDER BY mag DESC LIMIT 10;
    """,
    "Top 10 deepest earthquakes": """
        SELECT * FROM earthquake_data ORDER BY depth_km DESC LIMIT 10;
    """,
    "Shallow earthquakes <50km and mag >7.5": """
        SELECT * FROM earthquake_data WHERE depth_km < 50 AND mag > 7.5;
    """,
    "Average magnitude per magType": """
        SELECT magType, AVG(mag) AS avg_mag FROM earthquake_data GROUP BY magType;
    """,
    "Year with most earthquakes": """
        SELECT YEAR(time) AS year, COUNT(*) AS quake_count 
        FROM earthquake_data GROUP BY year ORDER BY quake_count DESC LIMIT 1;
    """,
    "Month with highest number of earthquakes": """
        SELECT MONTH(time) AS month, COUNT(*) AS quake_count 
        FROM earthquake_data GROUP BY month ORDER BY quake_count DESC LIMIT 1;
    """,
    "Day of week with most earthquakes": """
        SELECT DAYNAME(time) AS day, COUNT(*) AS quake_count 
        FROM earthquake_data GROUP BY day ORDER BY quake_count DESC LIMIT 1;
    """,
    "Count of earthquakes per hour of day": """
        SELECT HOUR(time) AS hour, COUNT(*) AS quake_count 
        FROM earthquake_data GROUP BY hour ORDER BY quake_count DESC;
    """,
    "Most active reporting network": """
        SELECT net, COUNT(*) AS quake_count 
        FROM earthquake_data GROUP BY net ORDER BY quake_count DESC LIMIT 1;
    """,
    "Top 5 places with highest casualties": """
        SELECT place, SUM(sig) AS total_significance
          FROM earthquake_data
           GROUP BY place
           ORDER BY total_significance DESC
LIMIT 5;
    """,
    "Average economic loss by alert level": """
        SELECT alert, AVG(sig) AS avg_significance
        FROM earthquake_data
        GROUP BY alert;
    """,
    "Count of reviewed vs automatic earthquakes": """
        SELECT status, COUNT(*) AS count FROM earthquake_data GROUP BY status;
    """,
    "Count by earthquake type": """
        SELECT type, COUNT(*) AS count FROM earthquake_data GROUP BY type;
    """,
    "Number of earthquakes by data type": """
        SELECT types, COUNT(*) AS count FROM earthquake_data GROUP BY types;
    """,
    "Events with high station coverage (nst > 50)": """
        SELECT * FROM earthquake_data WHERE nst > 50;
    """,
    "Number of tsunamis triggered per year": """
        SELECT YEAR(time) AS year, COUNT(*) AS tsunami_count 
        FROM earthquake_data WHERE tsunami = 1 GROUP BY year ORDER BY year;
    """,
    "Count earthquakes by alert levels": """
        SELECT alert, COUNT(*) AS count FROM earthquake_data GROUP BY alert;
    """,
    "Top 5 countries with highest avg magnitude (past 10 years)": """
        SELECT country, AVG(mag) AS avg_mag 
        FROM earthquake_data 
        WHERE YEAR(time) >= YEAR(CURDATE()) - 10 
        GROUP BY country ORDER BY avg_mag DESC LIMIT 5;
    """,
    "Countries with shallow & deep quakes same month": """
        SELECT country, YEAR(time) AS year, MONTH(time) AS month
        FROM earthquake_data
        GROUP BY country, year, month
        HAVING SUM(CASE WHEN depth_km < 70 THEN 1 ELSE 0 END) > 0
           AND SUM(CASE WHEN depth_km > 300 THEN 1 ELSE 0 END) > 0;
    """,
    "Year-over-year growth rate in total earthquakes": """
        SELECT year, quake_count,
               (quake_count - LAG(quake_count) OVER (ORDER BY year)) / LAG(quake_count) OVER (ORDER BY year) * 100 AS growth_rate
        FROM (
            SELECT YEAR(time) AS year, COUNT(*) AS quake_count
            FROM earthquake_data GROUP BY year
        ) t;
    """,
    "3 most seismically active regions": """
        SELECT place, COUNT(*) AS quake_count, AVG(mag) AS avg_mag
        FROM earthquake_data GROUP BY place
        ORDER BY quake_count DESC, avg_mag DESC LIMIT 3;
    """,
    "Average depth per country within ±5° latitude": """
        SELECT country, AVG(depth_km) AS avg_depth
        FROM earthquake_data
        WHERE latitude BETWEEN -5 AND 5
        GROUP BY country;
    """,
    "Countries with highest shallow/deep ratio": """
        SELECT country,
               SUM(CASE WHEN depth_km < 70 THEN 1 ELSE 0 END) / SUM(CASE WHEN depth_km >= 300 THEN 1 ELSE 0 END) AS shallow_deep_ratio
        FROM earthquake_data GROUP BY country ORDER BY shallow_deep_ratio DESC;
    """,
    "Average magnitude difference (tsunami vs non-tsunami)": """
        SELECT 
            (AVG(CASE WHEN tsunami = 1 THEN mag END) - AVG(CASE WHEN tsunami = 0 THEN mag END)) AS mag_diff
        FROM earthquake_data;
    """,
    "Events with lowest data reliability (gap & rms)": """
        SELECT * FROM earthquake_data ORDER BY gap DESC, rms DESC LIMIT 10;
    """,
    "Regions with highest frequency of deep-focus earthquakes": """
        SELECT place, COUNT(*) AS deep_quake_count
        FROM earthquake_data
        WHERE depth_km > 300
        GROUP BY place ORDER BY deep_quake_count DESC LIMIT 10;
    """
}

df['country'] = df['place'].apply(lambda x: x.split(',')[-1].strip() if ',' in str(x) else x)

# Execute queries
with engine.connect() as conn:
    for title, sql in queries.items():
        result = pd.read_sql(sql, conn)
        print(f"/n--- {title} ---")
        print(result)

import streamlit as st
import pandas as pd

# Load CSV
df = pd.read_csv("C:/Users/manoj/earthquake_cleaneddata.csv")
df["time"] = pd.to_datetime(df["time"], errors="coerce")
df["country"] = df["place"].str.split(",").str[-1].str.strip()

st.title("🌍 Earthquake Analytics Dashboard")

# Year filter
years = df["time"].dt.year.dropna().unique()
selected_year = st.selectbox("Select Year:", sorted(years))

# Filter data by year
df_year = df[df["time"].dt.year == selected_year]

# Define query map (using df_year instead of df)
query_map = {
    "Top 10 strongest earthquakes":
        df_year.nlargest(10, "mag"),

    "Top 10 deepest earthquakes":
        df_year.nlargest(10, "depth_km"),

    "Shallow earthquakes > 7.5":
        df_year[(df_year["depth_km"] < 50) & (df_year["mag"] > 7.5)],

    "Average magnitude per magnitude type":
        df_year.groupby("magtype")["mag"].mean().reset_index(name="avg_mag"),

    "Month with highest number of earthquakes":
        df_year.groupby(df_year["time"].dt.month.rename("month")).size().reset_index(name="quake_count").nlargest(1, "quake_count"),

    "Day of week with most earthquakes":
        df_year.groupby(df_year["time"].dt.day_name().rename("day")).size().reset_index(name="quake_count").nlargest(1, "quake_count"),

    "Count of earthquakes per hour of day":
        df_year.groupby(df_year["time"].dt.hour.rename("hour")).size().reset_index(name="quake_count").sort_values("quake_count", ascending=False),

    "Most active reporting network":
        df_year.groupby("net").size().reset_index(name="quake_count").nlargest(1, "quake_count"),

    "Top 5 places with highest significance":
        df_year.groupby("place")["sig"].sum().reset_index(name="total_significance").nlargest(5, "total_significance"),
}

# Query box at the top
selected_query = st.selectbox("Choose a query:", list(query_map.keys()))

# Show results directly
st.subheader(f"Results for {selected_year}: {selected_query}")
st.dataframe(query_map[selected_query])






 

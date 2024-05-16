import requests
import csv

def fetch_country_data(country_name):
    url = f'https://disease.sh/v3/covid-19/countries/{country_name}'
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch COVID-19 data for {country_name}. Status code:", response.status_code)
        return None
    
data = 'data.csv'
countries = ["India", "USA",  "China", "Russia", "Brazil", "France", "Germany", "UK", "Italy", "Spain",
             "Argentina", "Australia", "Japan", "South Korea", "Mexico", "Netherlands", "Switzerland",
             "Sweden", "Norway", "Ghana"]

# Open CSV file for writing
with open(data, 'w', newline='') as file:
    writer = csv.writer(file)

    # Write header row
    writer.writerow(['Country', 'Cases', 'Deaths', 'Recovered', 'Active_Cases', 'Critical_Cases'])

    # Iterate over countries and fetch data
    for country in countries:
        country_data = fetch_country_data(country)
        if country_data:
            writer.writerow([country_data['country'], country_data['cases'], country_data['deaths'],
                             country_data['recovered'], country_data['active'], country_data['critical']])

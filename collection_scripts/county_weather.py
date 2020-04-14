from noaa_sdk import noaa
import requests
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from pytz import timezone
import pytz
from selenium import webdriver
from uszipcode import SearchEngine
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import time
import database


def test():
    pass


def get_nearest_weather_station():
    n = noaa.NOAA()

    file = pd.read_csv('C:\\Users\\jdale\\OneDrive\\coronavirus\\'
                       'Coronavirus-ConradBI\\datasets\\zip_code_attached_to_county.csv')

    zip_list = list()
    station_list = list()

    file_2 = open("C:\\Users\\jdale\\OneDrive\\coronavirus\\"
                  "Coronavirus-ConradBI\\datasets\\station_scratch.csv", "w")
    file_2.write('Zip_Code,Station\n')

    for index, row in file.iterrows():
        if index > 0:
            if index < 3140:
                try:
                    observations = n.get_observations(row['Zip_Code'], country='US')
                    for i in observations:
                        station = i.get('station')
                        print(station[-4:], row['Zip_Code'])
                        insert_string = "{0},{1}\n".format(row['Zip_Code'], station[-4:])
                        file_2.write(insert_string)
                        station_list.append(station[-4:])
                        zip_list.append(row['Zip_Code'])
                        break
                except:
                    insert_string = "{0},UNKNOWN\n".format(row['Zip_Code'])
                    file_2.write(insert_string)
            else:
                file_2.close()
                data = {'Zip_Code': zip_list,
                        'Station': station_list}
                data = pd.DataFrame(data=data)
                data.set_index('Zip_Code', inplace=True)

                data.to_csv('C:\\Users\\jdale\\OneDrive\\coronavirus\\'
                            'Coronavirus-ConradBI\\datasets\\zip_code_nearest_station.csv')
                break



def get_zip_from_county():
    file = pd.read_csv('C:\\Users\\jdale\\OneDrive\\coronavirus\\Coronavirus-ConradBI\\datasets\\county_zip_fips.csv')

    zip_list = list()
    county_list = list()
    state_list = list()
    fips_list = list()

    for index, row in file.iterrows():
        if index == 0:
            zip_list.append(row['ZIP'])
            county_list.append(row['COUNTYNAME'])
            state_list.append(row['STATE'])
            fips_list.append(row['STCOUNTYFP'])
        if index > 0:
            if row['COUNTYNAME'] != file['COUNTYNAME'][index-1]:
                zip_list.append(row['ZIP'])
                county_list.append(row['COUNTYNAME'])
                state_list.append(row['STATE'])
                fips_list.append(row['STCOUNTYFP'])
    data = {'FIPs': fips_list,
            'County': county_list,
            'State': state_list,
            'Zip_Code': zip_list}
    data = pd.DataFrame(data=data)
    data.set_index('FIPs', inplace=True)

    data.to_csv('C:\\Users\\jdale\\OneDrive\\coronavirus\\'
                'Coronavirus-ConradBI\\datasets\\zip_code_attached_to_county.csv')


def get_location_data():
    file = pd.read_csv('C:\\Users\\jdale\\OneDrive\\coronavirus\\Coronavirus-ConradBI\\datasets\\station_scratch_fixed.csv')
    search = SearchEngine()
    zip_list = list()
    city_list = list()
    state_list = list()

    def num_there(s):
        return any(i.isdigit() for i in s)

    check_zip_list = list()
    for index, row in file.iterrows():
        if num_there(row['Station']) or row['Station'] == 'UNKNOWN':
            check_zip_list.append(row['Zip_Code'])
            print(row['Station'])

    data = {'Zip_Code': check_zip_list}
    data = pd.DataFrame(data=data)
    print(data)

    for index, row in data.iterrows():
        if len(str(row['Zip_Code'])) <= 4:
            zip_ = '0' + str(row['Zip_Code'])
        else:
            zip_ = row['Zip_Code']

        zipcode = search.by_zipcode(zip_)
        zip_list.append(zipcode.zipcode)
        city = zipcode.city.replace(' ', '-')
        city_list.append(city)
        state_list.append(zipcode.state)

    data = {'Zip_Code': zip_list,
            'City': city_list,
            'State': state_list}
    data = pd.DataFrame(data=data)

    scrape_station_information(data)


def scrape_station_information(data):

    zip_list = list()
    station_list = list()

    file_3 = open("C:\\Users\\jdale\\OneDrive\\coronavirus\\"
                  "Coronavirus-ConradBI\\datasets\\station_scratch_automation.csv", "w")
    file_3.write('Zip_Code,Station\n')
    file_3.close()

    for index, row in data.iterrows():
        try:
            file_3 = open("C:\\Users\\jdale\\OneDrive\\coronavirus\\"
                          "Coronavirus-ConradBI\\datasets\\station_scratch_automation.csv", "a")
            url = "https://www.wunderground.com/weather/us/{0}/{1}/{2}".format(row['State'], row['City'],
                                                                               row['Zip_Code'])
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            driver = webdriver.Chrome(options=chrome_options)
            driver.get(url)
            # right click "History", inspect, and then right click on the highlighted element Copy to get XPath
            WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,
                                                                        '//*[@id="inner-content"]/div[2]/'
                                                                        'lib-city-header/'
                                                                        'div[2]/lib-subnav/div/div[3]/ul/li[5]/a/span'))
                                            ).click()
            time.sleep(4)

            station = driver.current_url[-4:]
            driver.quit()

            print(station, row['Zip_Code'])
            insert_string = "{0},{1}\n".format(row['Zip_Code'], station)
            file_3.write(insert_string)
            file_3.close()
            station_list.append(station)
            zip_list.append(row['Zip_Code'])
        except:
            file_3 = open("C:\\Users\\jdale\\OneDrive\\coronavirus\\"
                          "Coronavirus-ConradBI\\datasets\\station_scratch_automation.csv", "a")
            insert_string = "{0},UNKNOWN\n".format(row['Zip_Code'])
            file_3.write(insert_string)
            file_3.close()


def consolidate_good_and_automated_files():
    good = pd.read_csv(
        'C:\\Users\\jdale\\OneDrive\\coronavirus\\Coronavirus-ConradBI\\datasets\\station_scratch_fixed.csv')
    automated = pd.read_csv(
        'C:\\Users\\jdale\\OneDrive\\coronavirus\\Coronavirus-ConradBI\\datasets\\station_scratch_automation.csv')

    flag = 0

    zip_list = list()
    station_list = list()

    for index, row in good.iterrows():
        for index2, row2 in automated.iterrows():
            if row['Zip_Code'] == row2['Zip_Code']:
                flag = 1
                zip_list.append(row['Zip_Code'])
                station_list.append(row2['Station'])
        if flag == 0:
            zip_list.append(row['Zip_Code'])
            station_list.append(row['Station'])
        else:
            flag = 0
        print(index)

    data = {'Zip_Code': zip_list,
            'Station': station_list}
    data = pd.DataFrame(data=data)
    data.set_index('Zip_Code', inplace=True)

    data.to_csv('C:\\Users\\jdale\\OneDrive\\coronavirus\\'
                'Coronavirus-ConradBI\\datasets\\zip_codes_with_stations_V1.csv')


def join_station_with_county():
    county = pd.read_csv(
        'C:\\Users\\jdale\\OneDrive\\coronavirus\\Coronavirus-ConradBI\\datasets\\zip_code_attached_to_county.csv')
    no_county = pd.read_csv(
        'C:\\Users\\jdale\\OneDrive\\coronavirus\\'
        'Coronavirus-ConradBI\\datasets\\zip_codes_with_stations_V1.csv')

    joined = county.join(no_county.set_index('Zip_Code'), on='Zip_Code')

    joined.drop_duplicates(subset='FIPs', keep='first', inplace=True)

    joined.to_csv('C:\\Users\\jdale\\OneDrive\\coronavirus\\'
                  'Coronavirus-ConradBI\\datasets\\county_zip_station_V1.csv')


def is_station_null():
    station_table = pd.read_csv(
        'C:\\Users\\jdale\\OneDrive\\coronavirus\\'
        'Coronavirus-ConradBI\\datasets\\county_zip_station_V1.csv')

    for index, row in station_table.iterrows():
        if row['Station'] != row['Station']:
            print(index, row['Zip_Code'])


def get_est_localtime(timestamp, utc, est_tz):

    utc_dt = utc.localize(datetime.utcfromtimestamp(timestamp))
    est_dt = utc_dt.astimezone(est_tz)
    return est_dt


# inspect network to get api from wunderground
def scrape_weather(station, start_date, end_date):
    utc = pytz.utc
    fmt = '%Y-%m-%d'
    est_tz = timezone('US/Eastern')
    # Inspect page, Network, refresh page, check out XHR's to find this API call
    url = 'https://api.weather.com/v1/location/{0}:9:US/observations/' \
          'historical.json?apiKey=6532d6454b8aa370768e63d6ba5a832e&' \
          'units=e&startDate={1}&endDate={2}'.format(station, start_date, end_date)
    r = requests.get(url)
    if r.status_code == 400:
        if station[0] == 'C':
            url = 'https://api.weather.com/v1/location/{0}:9:CA' \
                  '/observations/historical.json?apiKey=6532d6454b8' \
                  'aa370768e63d6ba5a832e&units=e&startDate={1}&endDate={2}'.format(station, start_date, end_date)
            r = requests.get(url)
            if r.status_code == 400:
                return pd.DataFrame()
        elif station[0] == 'M':
            url = 'https://api.weather.com/v1/location/{0}:9' \
                  ':MX/observations/historical.json?apiKey=6532d6454b8' \
                  'aa370768e63d6ba5a832e&units=e&startDate={1}&endDate={2}'.format(station, start_date, end_date)
            r = requests.get(url)
            if r.status_code == 400:
                return pd.DataFrame()
        else:
            return pd.DataFrame()
    df = pd.DataFrame(r.json()['observations'])
    # observations come in in blocks of time, this code consolidates and groups that code by
    # min, max, and mean for the dates
    df['valid_time_gmt'] = df['valid_time_gmt'].map(lambda x: get_est_localtime(x, utc, est_tz).strftime(fmt))
    df = df.groupby(by='valid_time_gmt').agg([min, max, np.mean]).reset_index()
    # print(df['precip_total']['mean'])
    # print(df['temp'])
    time.sleep(1)
    return df


def write_old_temperature(df, row, start_date, end_date):
    fips_list = list()
    county_list = list()
    state_list = list()
    zip_list = list()
    date_list = list()
    min_temp_list = list()
    max_temp_list = list()
    avg_temp_list = list()
    min_dp_list = list()
    max_dp_list = list()
    avg_dp_list = list()
    min_humid_list = list()
    max_humid_list = list()
    avg_humid_list = list()
    precip_list = list()

    if df.empty:
        print(row['Zip_Code'], row['Station'])
        fips_list.append(row['FIPs'])
        county_list.append(row['County'])
        state_list.append(row['State'])
        zip_list.append(row['Zip_Code'])
        date_list.append('NA')
        min_temp_list.append('NA')
        max_temp_list.append('NA')
        avg_temp_list.append('NA')
        min_dp_list.append('NA')
        max_dp_list.append('NA')
        avg_dp_list.append('NA')
        min_humid_list.append('NA')
        max_humid_list.append('NA')
        avg_humid_list.append('NA')
        precip_list.append('NA')

        data = {'FIPs': fips_list,
                'County': county_list,
                'State': state_list,
                'Zip_Code': zip_list,
                'Date': date_list,
                'Min_Temperature': min_temp_list,
                'Max_Temperature': max_temp_list,
                'Average_Temperature': avg_temp_list,
                'Min_Dew_Point': min_dp_list,
                'Max_Dew_Point': max_dp_list,
                'Average_Dew_Point': avg_dp_list,
                'Min_Relative_Humidity': min_humid_list,
                'Max_Relative_Humidity': max_humid_list,
                'Average_Relative_Humidity': avg_humid_list,
                'Total_Precipitation': precip_list}

        data = pd.DataFrame(data=data)
        data.to_csv('C:\\Users\\jdale\\OneDrive\\coronavirus\\'
                    'Coronavirus-ConradBI\\datasets\\old_weather.csv', mode='a', header=False, index=False)

    else:
        for index1, row1 in df.iterrows():
            try:
                if start_date <= \
                        date(int(list(row1['valid_time_gmt'])[0][:4]),
                             int(list(row1['valid_time_gmt'])[0][5:7]),
                             int(list(row1['valid_time_gmt'])[0][-2:])) \
                        <= end_date:
                    date_list.append(list(row1['valid_time_gmt'])[0])
                else:
                    continue
            except:
                pass
            fips_list.append(row['FIPs'])
            county_list.append(row['County'])
            state_list.append(row['State'])
            zip_list.append(row['Zip_Code'])
            try:
                min_temp_list.append(row1['temp']['min'])
            except:
                min_temp_list.append('NA')
            try:
                max_temp_list.append(row1['temp']['max'])
            except:
                max_temp_list.append('NA')
            try:
                avg_temp_list.append(row1['temp']['mean'])
            except:
                avg_temp_list.append('NA')
            try:
                min_dp_list.append(row1['dewPt']['min'])
            except:
                min_dp_list.append('NA')
            try:
                max_dp_list.append(row1['dewPt']['max'])
            except:
                max_dp_list.append('NA')
            try:
                avg_dp_list.append(row1['dewPt']['mean'])
            except:
                avg_dp_list.append('NA')
            try:
                min_humid_list.append(row1['rh']['min'])
            except:
                min_humid_list.append('NA')
            try:
                max_humid_list.append(row1['rh']['max'])
            except:
                max_humid_list.append('NA')
            try:
                avg_humid_list.append(row1['rh']['mean'])
            except:
                avg_humid_list.append('NA')
            try:
                precip_list.append(row1['precip_total']['mean'])
            except:
                precip_list.append('NA')
        data = {'FIPs': fips_list,
                'County': county_list,
                'State': state_list,
                'Zip_Code': zip_list,
                'Date': date_list,
                'Min_Temperature': min_temp_list,
                'Max_Temperature': max_temp_list,
                'Average_Temperature': avg_temp_list,
                'Min_Dew_Point': min_dp_list,
                'Max_Dew_Point': max_dp_list,
                'Average_Dew_Point': avg_dp_list,
                'Min_Relative_Humidity': min_humid_list,
                'Max_Relative_Humidity': max_humid_list,
                'Average_Relative_Humidity': avg_humid_list,
                'Total_Precipitation': precip_list}

        data = pd.DataFrame(data=data)
        data.to_csv('C:\\Users\\jdale\\OneDrive\\coronavirus\\'
                    'Coronavirus-ConradBI\\datasets\\old_weather.csv', mode='a', header=False, index=False)


def write_daily_temperature(df, row, start_date, end_date):
    fips_list = list()
    county_list = list()
    state_list = list()
    zip_list = list()
    date_list = list()
    min_temp_list = list()
    max_temp_list = list()
    avg_temp_list = list()
    min_dp_list = list()
    max_dp_list = list()
    avg_dp_list = list()
    min_humid_list = list()
    max_humid_list = list()
    avg_humid_list = list()
    precip_list = list()

    if df.empty:
        print(row['Zip_Code'], row['Station'])
        fips_list.append(row['FIPs'])
        county_list.append(row['County'])
        state_list.append(row['State'])
        zip_list.append(row['Zip_Code'])
        date_list.append('NA')
        min_temp_list.append('NA')
        max_temp_list.append('NA')
        avg_temp_list.append('NA')
        min_dp_list.append('NA')
        max_dp_list.append('NA')
        avg_dp_list.append('NA')
        min_humid_list.append('NA')
        max_humid_list.append('NA')
        avg_humid_list.append('NA')
        precip_list.append('NA')

        data = {'FIPs': fips_list,
                'County': county_list,
                'State': state_list,
                'Zip_Code': zip_list,
                'Date': date_list,
                'Min_Temperature': min_temp_list,
                'Max_Temperature': max_temp_list,
                'Average_Temperature': avg_temp_list,
                'Min_Dew_Point': min_dp_list,
                'Max_Dew_Point': max_dp_list,
                'Average_Dew_Point': avg_dp_list,
                'Min_Relative_Humidity': min_humid_list,
                'Max_Relative_Humidity': max_humid_list,
                'Average_Relative_Humidity': avg_humid_list,
                'Total_Precipitation': precip_list}

        data = pd.DataFrame(data=data)
        data.to_csv('C:\\Users\\jdale\\OneDrive\\coronavirus\\'
                    'Coronavirus-ConradBI\\datasets\\old_weather.csv', mode='a', header=False, index=False)

    else:
        for index1, row1 in df.iterrows():
            try:
                if start_date <= \
                        date(int(list(row1['valid_time_gmt'])[0][:4]),
                             int(list(row1['valid_time_gmt'])[0][5:7]),
                             int(list(row1['valid_time_gmt'])[0][-2:])) \
                        <= end_date:
                    date_list.append(list(row1['valid_time_gmt'])[0])
                else:
                    continue
            except:
                pass
            fips_list.append(row['FIPs'])
            county_list.append(row['County'])
            state_list.append(row['State'])
            zip_list.append(row['Zip_Code'])
            try:
                min_temp_list.append(row1['temp']['min'])
            except:
                min_temp_list.append('NA')
            try:
                max_temp_list.append(row1['temp']['max'])
            except:
                max_temp_list.append('NA')
            try:
                avg_temp_list.append(row1['temp']['mean'])
            except:
                avg_temp_list.append('NA')
            try:
                min_dp_list.append(row1['dewPt']['min'])
            except:
                min_dp_list.append('NA')
            try:
                max_dp_list.append(row1['dewPt']['max'])
            except:
                max_dp_list.append('NA')
            try:
                avg_dp_list.append(row1['dewPt']['mean'])
            except:
                avg_dp_list.append('NA')
            try:
                min_humid_list.append(row1['rh']['min'])
            except:
                min_humid_list.append('NA')
            try:
                max_humid_list.append(row1['rh']['max'])
            except:
                max_humid_list.append('NA')
            try:
                avg_humid_list.append(row1['rh']['mean'])
            except:
                avg_humid_list.append('NA')
            try:
                precip_list.append(row1['precip_total']['mean'])
            except:
                precip_list.append('NA')
        data = {'FIPs': fips_list,
                'County': county_list,
                'State': state_list,
                'Zip_Code': zip_list,
                'Date': date_list,
                'Min_Temperature': min_temp_list,
                'Max_Temperature': max_temp_list,
                'Average_Temperature': avg_temp_list,
                'Min_Dew_Point': min_dp_list,
                'Max_Dew_Point': max_dp_list,
                'Average_Dew_Point': avg_dp_list,
                'Min_Relative_Humidity': min_humid_list,
                'Max_Relative_Humidity': max_humid_list,
                'Average_Relative_Humidity': avg_humid_list,
                'Total_Precipitation': precip_list}

        data = pd.DataFrame(data=data)
        data.to_csv('C:\\Users\\jdale\\OneDrive\\coronavirus\\'
                    'Coronavirus-ConradBI\\datasets\\daily_weather.csv', mode='a', header=False, index=False)



def process_old_weather():
    df = pd.read_csv(
        'C:\\Users\\jdale\\OneDrive\\coronavirus\\Coronavirus-ConradBI\\datasets\\county_zip_station_V1.csv')

    data = {'FIPs': list(),
            'County': list(),
            'State': list(),
            'Zip_Code': list(),
            'Date': list(),
            'Min_Temperature': list(),
            'Max_Temperature': list(),
            'Average_Temperature': list(),
            'Min_Dew_Point': list(),
            'Max_Dew_Point': list(),
            'Average_Dew_Point': list(),
            'Min_Relative_Humidity': list(),
            'Max_Relative_Humidity': list(),
            'Average_Relative_Humidity': list(),
            'Total_Precipitation': list()}

    data = pd.DataFrame(data=data)
    data.to_csv('C:\\Users\\jdale\\OneDrive\\coronavirus\\'
                'Coronavirus-ConradBI\\datasets\\old_weather.csv', index=False)

    for index, row in df.iterrows():
        if index >= 0:
            print(index)

            df = scrape_weather(row['Station'], '20200313', '20200410')
            start = date(2020, 3, 13)
            end = date(2020, 4, 10)
            write_old_temperature(df, row, start, end)


def get_yesterday():
    return date.today() - timedelta(days=1)


def process_weather_daily():
    df = pd.read_csv(
        'C:\\Users\\jdale\\OneDrive\\coronavirus\\Coronavirus-ConradBI\\datasets\\county_zip_station_V1.csv')

    data = {'FIPs': list(),
            'County': list(),
            'State': list(),
            'Zip_Code': list(),
            'Date': list(),
            'Min_Temperature': list(),
            'Max_Temperature': list(),
            'Average_Temperature': list(),
            'Min_Dew_Point': list(),
            'Max_Dew_Point': list(),
            'Average_Dew_Point': list(),
            'Min_Relative_Humidity': list(),
            'Max_Relative_Humidity': list(),
            'Average_Relative_Humidity': list(),
            'Total_Precipitation': list()}

    data = pd.DataFrame(data=data)
    data.to_csv('C:\\Users\\jdale\\OneDrive\\coronavirus\\'
                'Coronavirus-ConradBI\\datasets\\daily_weather.csv', index=False)

    start_date = database.select_database("SELECT date from coronavirus.a_county_weather ORDER BY date DESC LIMIT 1;")
    start_date = (start_date['date'])[0] + timedelta(days=1)
    end_date = get_yesterday()
    start_date_formatted = str(start_date).replace('-', '')
    end_date_formatted = str(end_date).replace('-', '')

    for index, row in df.iterrows():
        if index >= 0:
            print(index)

            df = scrape_weather(row['Station'], start_date_formatted, end_date_formatted)
            write_daily_temperature(df, row, start_date, end_date)


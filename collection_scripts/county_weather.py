from noaa_sdk import noaa
import requests
import pandas as pd
import numpy as np
from datetime import datetime
from pytz import timezone
import pytz
from selenium import webdriver
from uszipcode import SearchEngine
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import time

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


def get_est_localtime(timestamp, utc, est_tz):

    utc_dt = utc.localize(datetime.utcfromtimestamp(timestamp))
    est_dt = utc_dt.astimezone(est_tz)
    return est_dt


# inspect network to get api from wunderground
def scrape_weather():
    utc = pytz.utc
    fmt = '%Y-%m-%d'
    est_tz = timezone('US/Eastern')
    r = requests.get('https://api.weather.com/v1/location/KOKC:9:US/observations/'
                     'historical.json?apiKey=6532d6454b8aa370768e63d6ba5a832e&'
                     'units=e&startDate=20200324&endDate=20200409')
    df = pd.DataFrame(r.json()['observations'])
    df['valid_time_gmt'] = df['valid_time_gmt'].map(lambda x: get_est_localtime(x, utc, est_tz).strftime(fmt))
    df = df.groupby(by='valid_time_gmt').agg([min, max, np.mean])
    #print(df['precip_total']['mean'])
    #print(df['temp'])
    return df


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
            WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH,
                                                                        '//*[@id="inner-content"]/div[2]/'
                                                                        'lib-city-header/'
                                                                        'div[2]/lib-subnav/div/div[3]/ul/li[5]/a/span'))
                                            ).click()
            time.sleep(3)

            station = driver.current_url[-4:]

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


def main():
    get_location_data()


if __name__ == '__main__':
    main()
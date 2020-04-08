from noaa_sdk import noaa
import pandas as pd
import requests


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


def main():
    get_nearest_weather_station()



if __name__ == '__main__':
    main()
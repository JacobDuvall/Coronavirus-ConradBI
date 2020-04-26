import county_weather
import database


def main():
    county_weather.process_weather_daily()
    database.bulk_insert_weather_csv()
    exit()


if __name__ == '__main__':
    main()

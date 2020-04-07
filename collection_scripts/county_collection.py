from database import *


def get_nyt_county_data(url):
    df = pd.read_csv(url)
    df.set_index('date', inplace=True)
    df.to_csv('C:\\Users\\jdale\\OneDrive\\coronavirus\\Coronavirus-ConradBI\\datasets\\county_data.csv')


def get_county():
    get_nyt_county_data('https://github.com/nytimes/covid-19-data/raw/master/us-counties.csv')
    bulk_insert_county_csv()


def get_nyt_state_data(url):
    df = pd.read_csv(url)
    df.set_index('date', inplace=True)
    df.to_csv('C:\\Users\\jdale\\OneDrive\\coronavirus\\Coronavirus-ConradBI\\datasets\\state_data.csv')


def get_state():
    get_nyt_state_data('https://github.com/nytimes/covid-19-data/raw/master/us-states.csv')
    bulk_insert_state_csv()


def main():
    get_county()
    get_state()


if __name__ == '__main__':
    main()
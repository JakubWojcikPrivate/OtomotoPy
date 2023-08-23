import os
import time
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import requests
from bs4 import BeautifulSoup

from utils.logger import console_logger
from utils.logger import file_logger


class AdvertisementFetcher:
    """
        Fetches advertisements
        Args:
             features_file_path: path to file with features
    """
    MAX_THREADS = 8

    def __init__(self, features_file_path='resources/input/feats.txt'):
        self.features_file_path = os.path.join(os.getcwd(), features_file_path)
        self.all_features = self._read_features()
        self._cars = []

    def _read_features(self):
        with open(self.features_file_path, 'r', encoding='utf-8') as featsFile:
            features = featsFile.readlines()
        return [x.strip() for x in features]

    def _make_line(self, main_features):
        temp = {feat: main_features.get(feat, None)
                for feat in self.all_features}
        return temp

    def _download_url(self, path):
        test_stage = 0
        try:
            test_stage = 1
            file_logger.info(f'Fetching {path}')

            test_stage = 2
            res = requests.get(path)

            test_stage = 3
            res.raise_for_status()

            test_stage = 4
            soup = BeautifulSoup(res.text, features='lxml')

            test_stage = 5
            main_params = soup.find_all(class_='offer-params__item')

            test_stage = 6
            features = {
                param.find('span', class_='offer-params__label').text.strip():
                    param.find('div', class_='offer-params__value').text.strip()
                for param in main_params
            }

            test_stage = 7
            extendend_params = soup.find_all('li', class_='parameter-feature-item')
            print(extendend_params)

            test_stage = 8
            for param in extendend_params:
                features[param.text.strip()] = 1

            # Searching for advert ID
            test_stage = 9
            id = ''.join(soup.find('span', class_='offer-price__number').text.strip().split()[:-1])

            # Searching for advert number
            test_stage = + 10
            id_number = ''.join(soup.find('span', class_='offer-price__number').text.strip().split()[:-1])

            test_stage = + 11
            price = ''.join(soup.find('span', class_='offer-price__number').text.strip().split()[:-1])
            features['Cena'] = price

            test_stage = + 12
            currency = soup.find('span', class_='offer-price__currency').text.strip()
            features['Waluta'] = currency

            test_stage = + 13
            price_details = soup.find('span', class_='offer-price__details').text.strip()
            features['Szczegóły ceny'] = price_details

            features = self._make_line(features)

        except Exception as e:
            file_logger.error(f'Error {e} at stage {test_stage} while fetching {path}')
            console_logger.error(f'Error {e} at stage {test_stage} while fetching {path}')
            return None

        time.sleep(0.25)

        return features

    def fetch_ads(self, links):
        """ Fetches ads
            Args:
                 links: links
        """
        with ThreadPoolExecutor(
                max_workers=min(
                    self.MAX_THREADS,
                    len(links)
                )
        ) as executor:
            features = [executor.submit(self._download_url, link) for link in links]
            for feature in features:
                result = feature.result()
                if result is not None:
                    self._cars.append(result)

    def save_ads(self, model):
        """
            Saves ads
            Args:
                 model: model
        """
        file_logger.info(f'Saving {model} ads')
        file_logger.info(f'Found {len(self._cars)} ads')
        console_logger.info(f'Found {len(self._cars)} ads')
        pd.DataFrame(self._cars).to_excel(f'output/data/{model}.xlsx')
        file_logger.info(f'Saved {model} ads')

    def setup_fetcher(self):
        self._cars = []

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

    def _search_ext_param(self, soup, param_name, split=True):

        # Set up everything for 'span' parameter
        tag_search = soup.find('span', class_=param_name)

        if tag_search is not None:
            if split:
                param = ''.join(tag_search.text.strip().split()[:-1])
            else:
                param = ''.join(tag_search.text.strip())
            return param

        # Set up everything for 'h3' parameter
        tag_search = soup.find('h3', class_=param_name)

        if tag_search is not None:
            param = ''.join(tag_search.text.strip())
            return param

        return ''

    def __search_ext_param(self, soup, param_prefix, param_name, split):
        # Iterate over tuple
        for p, n, s in zip(param_prefix, param_name, split):
            tag_search = soup.find(p, class_=n)
            if tag_search is not None:
                if s:
                    param = ''.join(tag_search.text.strip().split()[:-1])
                else:
                    param = ''.join(tag_search.text.strip())

                return param
        return ''

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

            test_stage = 8
            for param in extendend_params:
                features[param.text.strip()] = 1

            # Searching for advert ID
            test_stage = 9
            features['Id'] = self._search_ext_param(soup, 'offer-price__details', False)

            # Searching for advert number
            test_stage = + 10
            features['Id_num'] = self._search_ext_param(soup, 'offer-price__details', False)

            test_stage = + 11
            features['Cena'] = self.__search_ext_param(
                soup,
                ('span', 'h3'),
                ('offer-price__number', 'offer-price__number'),
                (True, False))

            test_stage = + 12
            features['Waluta'] = self._search_ext_param(soup, 'offer-price__currency')

            test_stage = + 13
            features['Szczegóły ceny'] = self._search_ext_param(soup, 'offer-price__details', False)

            test_stage = + 14
            features['Http'] = path

            test_stage = 15
            features['Data dodania'] = self._search_ext_param(soup, 'offer-price__details', False)

            test_stage = + 16
            features = self._make_line(features)

        except Exception as e:
            file_logger.error(f'Error {e} at stage {test_stage} while fetching {path}')
            console_logger.error(f'Error {e} at stage {test_stage} ext param len {str(len(extendend_params))} while '
                                 f'fetching {path}')
            return None

        time.sleep(0.2)

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

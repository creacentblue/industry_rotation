# coding=utf-8
"""functions for cache"""

import os
import numpy
from tantra.logger import logger
from tantra.utils.csv_utils import read_csv_to_list, write_list_to_csv


global_cache_dir = os.path.expanduser('~/workspace/data/cache/')


def has_cache(cache_dir, cache_name, use_csv=False):
    """Check if cache file exists
    Args:
        cache_dir (str)
        cache_name (str)
        use_csv (bool): detect only csv file
    """
    if use_csv:
        return os.path.exists(cache_dir + cache_name + '.csv')

    return os.path.exists(cache_dir + cache_name + '.npy')


def load_cache(cache_dir, cache_name, use_csv=False):
    """Load cache data
    Args:
        cache_dir (str)
        cache_name (str)
        use_csv (bool): load data from csv
    """
    if use_csv:
        data = read_csv_to_list(cache_dir + cache_name + '.csv')
        logger.debug("load cache: {:s}{:s}.csv {} rows".format(cache_dir, cache_name, len(data)))
        return data

    data = numpy.load(cache_dir + cache_name + '.npy')
    logger.debug("load cache: {:s}{:s}.npy {}".format(cache_dir, cache_name, data.shape))
    return data


def save_cache(cache_dir, cache_name, data, fmt='%.5f', overwrite=True, use_csv=False):
    """Save numpy data to cache
    Args:
        cache_dir (str)
        cache_name (str)
        data (numpy.ndarray or list)
        fmt (str)
        overwrite (bool)
        use_csv (bool): write data to csv
    """
    if not overwrite and has_cache(cache_dir, cache_name):
        return

    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir)

    if use_csv:
        logger.debug("save cache: {:s}{:s} {} rows".format(cache_dir, cache_name, len(data)))
        write_list_to_csv(cache_dir + cache_name + '.csv', data)
    else:
        logger.debug("save cache: {:s}{:s} {}".format(cache_dir, cache_name, data.shape))
        numpy.save(cache_dir + cache_name + '.npy', data)
        numpy.savetxt(cache_dir + cache_name + '.csv', data, fmt=fmt)

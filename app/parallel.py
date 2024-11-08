from multiprocessing import cpu_count
from geopandas.geodataframe import GeoDataFrame
# from shapely.geometry import Point
import logging
import time
import geopy.distance


def get_cpu_limit():
    """
    From https://donghao.org/2022/01/20/how-to-get-the-number-of-cpu-cores-inside-a-container/
    :return: number of CPUs that is available in the docker container
    """
    with open("/sys/fs/cgroup/cpu/cpu.cfs_quota_us") as fp:
        cfs_quota_us = int(fp.read())
    with open("/sys/fs/cgroup/cpu/cpu.cfs_period_us") as fp:
        cfs_period_us = int(fp.read())
    container_cpus = cfs_quota_us // cfs_period_us
    # For physical machine, the `cfs_quota_us` could be '-1'
    cpus = cpu_count() if container_cpus < 1 else container_cpus
    return cpus


def calculate_distance(data: GeoDataFrame):
    """
    Calculates the distance between each location and the previous location
    :param data: a GeoDataFrame
    :return: input data with distances in km
    """
    data.set_crs('epsg:4326')
    data["x"] = data.get_coordinates(include_z=False)["x"]
    data["y"] = data.get_coordinates(include_z=False)["y"]

    # Calculate the distance between the locations for each row in the data
    distances = [None] * (len(data)-1)
    for row in range(len(data) - 1):
        distances[row] = geopy.distance.distance((data["y"].iloc()[row], data["x"].iloc()[row]),
                                                 (data["y"].iloc()[row+1], data["x"].iloc()[row+1])).km

    data["distance_from_previous_geopy"] = [None] + distances

    # Now wait for 10 seconds
    logging.info("Sleeping")
    time.sleep(10)

    # return the data with distances
    return data

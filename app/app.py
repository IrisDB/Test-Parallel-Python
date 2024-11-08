from sdk.moveapps_spec import hook_impl
from sdk.moveapps_io import MoveAppsIo
from movingpandas import TrajectoryCollection
import logging
import time

from app.parallel import get_cpu_limit, calculate_distance


class App(object):

    def __init__(self, moveapps_io):
        self.moveapps_io = moveapps_io

    @hook_impl
    def execute(self, data: TrajectoryCollection, config: dict) -> TrajectoryCollection:
        """
        Execute the App code
        :param data: input data
        :param config: app configuration/settings in JSON format
        :return: data with distances between consecutive locations
        """

        logging.info(f'Welcome to the {config}')

        logging.info('Check if parallel computing in Python works in MoveApps')

        # find the number of cpu's that is available
        n_cpu = 1  # get_cpu_limit()  #TODO: execute get_cpu_limit
        logging.info(f'Number of cores currently available for parallel processing: {n_cpu}')

        # transfer the data to a GeoDataFrame
        data_gdf = data.to_point_gdf()

        # get all different track IDs
        track_id_col_name = data.get_traj_id_col()
        track_ids = data_gdf[track_id_col_name].unique()
        logging.info(f'Track IDs discovered: {track_ids}')

        # calculate the distances between consecutive locations per individual in a for-loop
        start_for = time.time()
        logging.info('Calculating distances')
        for i in track_ids:
            logging.info(f'Calculating distances for {i}')
            result = calculate_distance(data_gdf)  # TODO: merge data back together
            #logging.info(result)
        time_for = time.time() - start_for
        logging.info(f'Time used for calculating distances in a for-loop: {time_for}')

        # calculate the distances between consecutive locations per individual using parallel computing
        start_par = time.time()
        logging.info('Calculating distances')
        for i in track_ids:  # TODO: replace by parallel computing + merge data back together
            logging.info(f'Calculating distances for {i}')
            result = calculate_distance(data_gdf)
            #logging.info(result)
        time_par = time.time() - start_par
        logging.info(f'Time used for calculating distances in a for-loop: {time_par}')

        auxiliary_file_a = MoveAppsIo.get_auxiliary_file_path("auxiliary-file-a")
        with open(auxiliary_file_a, 'r') as f:
            logging.info(f.read())

        # return some useful data for next apps in the workflow
        return data

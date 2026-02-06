from sdk.moveapps_spec import hook_impl
from movingpandas import TrajectoryCollection
import logging
import time

from app.parallel import calculate_distance, parallelize


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

        logging.info('Main process started.')

        logging.info('Check if parallel computing in Python works in MoveApps')

        # transfer the data to a GeoDataFrame
        data_gdf = data.to_point_gdf()

        # get all different track IDs
        track_id_col_name = data.get_traj_id_col()
        track_ids = data_gdf[track_id_col_name].unique()
        logging.info(f'Track IDs discovered: {track_ids}')

        # calculate the distances between consecutive locations per individual using parallel computing
        start_par = time.time()
        logging.info('Calculating distances using parallel computing')
        result_par = parallelize(data, calculate_distance)
        time_par = time.time() - start_par
        logging.info(f'Time used for calculating distances using parallel computing: {time_par}')

        # translate the result back to a TrajectoryCollection
        if result_par is not None:
            result = TrajectoryCollection(
                result_par,
                traj_id_col=data.get_traj_id_col(),
                t=data.to_point_gdf().index.name,
                crs=data.get_crs()
            )
        else:
            result = None

        # return some useful data for next apps in the workflow
        return result

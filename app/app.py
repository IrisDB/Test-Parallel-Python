from sdk.moveapps_spec import hook_impl
from movingpandas import TrajectoryCollection
import pandas as pd
import logging
import time
import matplotlib.pyplot as plt

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

        logging.info('Check if parallel computing in Python works in MoveApps')

        logging.info('Calculating distances using a for-loop')
        start_for = time.time()

        # transfer the data to a GeoDataFrame
        data_gdf = data.to_point_gdf()

        # get all different track IDs
        track_id_col_name = data.get_traj_id_col()
        track_ids = data_gdf[track_id_col_name].unique()
        logging.info(f'Track IDs discovered: {track_ids}')

        # calculate the distances between consecutive locations per individual in a for-loop
        results = []
        for i in track_ids:
            logging.info(f'Calculating distances for {i}')
            results.append(calculate_distance(data_gdf[data_gdf[track_id_col_name] == i].copy()))
        result_for = pd.concat(results)
        time_for = time.time() - start_for
        logging.info(f'Time used for calculating distances in a for-loop: {time_for}')

        # calculate the distances between consecutive locations per individual using parallel computing
        start_par = time.time()
        logging.info('Calculating distances using parallel computing')
        result_par = parallelize(data, calculate_distance)
        time_par = time.time() - start_par
        logging.info(f'Time used for calculating distances using parallel computing: {time_par}')

        # Make plots of the results
        for tr_id in track_ids:
            data_sub = result_for[result_for[track_id_col_name] == tr_id]
            plt.scatter(data_sub.index, data_sub["distance_from_previous_geopy"])
        plot_file = self.moveapps_io.create_artifacts_file("plot_for.png")
        plt.savefig(plot_file)
        plt.clf()
        logging.info(f'saved plot to {plot_file}')

        for tr_id in track_ids:
            data_sub = result_par[result_par[track_id_col_name] == tr_id]
            plt.scatter(data_sub.index, data_sub["distance_from_previous_geopy"])
        plot_file = self.moveapps_io.create_artifacts_file("plot_par.png")
        plt.savefig(plot_file)
        plt.clf()
        logging.info(f'saved plot to {plot_file}')

        # test if the results are the same
        logging.info(f'Both methods gave the same result: {result_for.equals(result_par)}.')

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

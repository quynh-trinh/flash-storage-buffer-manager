import sys
import os
sys.path.insert(1, os.path.join(sys.path[0], '../..'))

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
from pathlib import Path

from src.util.constants import BENCHMARK_DATA_FOLDER

benchmark_labels = {
    'num_updates': 'Number of Updates',
    'percent_updated': 'Percent of Video Updated',
    'video_length_update': 'Length of Video Updated (s)',
    'num_commits': 'Number of Updates',
    'num_aborts': 'Number of Updates'
}

def do_plot(benchmark_name: str,
            df: pd.DataFrame,
            context: str,
            x_axis_column: str,
            y_axis_column: str,
            x_label: str,
            y_label: str):
    fig_size = (16, 9)
    dpi = 300

    sns.set_theme(style='ticks')
    sns.set_context(context)
    # fig, line_plot = plt.subplots(figsize = fig_size, dpi = dpi)
    fig, line_plot = plt.subplots(figsize = fig_size)
    line_plot.grid(axis='y')
    line_plot = sns.lineplot(x=x_axis_column,
                            y=y_axis_column,
                            hue='algorithm',
                            style='algorithm',
                            markers=True,
                            ci=None,
                            data=df,
                            palette=sns.color_palette('tab10', n_colors=4))
    line_plot.set_xlabel(x_label)
    line_plot.set_ylabel(y_label)
    line_plot.legend(bbox_to_anchor=(1,1), loc='upper left', title = 'Algorithm')
    sns.despine()
    plt.savefig(f'{BENCHMARK_DATA_FOLDER}/{benchmark_name}_{y_axis_column}_{context}.png', bbox_inches='tight', dpi=dpi)

def plot(benchmark_name: str):
    if benchmark_name == 'trace':
        csv_file_name = f'{BENCHMARK_DATA_FOLDER}/trace.csv'
    elif benchmark_name == 'synthetic':
        csv_file_name = f'{BENCHMARK_DATA_FOLDER}/synthetic.csv'
    else:
        raise ValueError("benchmark_name must be 'trace' or 'synthetic'")
    df = pd.read_csv(csv_file_name)
    df['hit_rate'] = df['num_hits']/df['num_accesses']*100
    df['miss_rate'] = df['num_misses']/df['num_accesses']*100
    # df['ratio_dirty_evictions'] = df['num_dirty_evictions']/df['num_evictions']
    # df.loc[df['num_dirty_evictions'] == 0, 'ratio_dirty_evictions'] = 0

    print(df)

    for context in ['talk', 'poster']:
        for graph_info in [
            ['hit_rate', 'Hit Rate (%)'],
            ['num_dirty_evictions', 'Dirty Evictions']
        ]:
            do_plot(benchmark_name, df, context, 'relative_buffer_pool_size', graph_info[0], 'Buffer pool size relative to dataset size (%)', graph_info[1])


if __name__ == '__main__':
    if len(sys.argv) < 2:
        plot('trace')
        plot('synthetic')
    else:
        plot(sys.argv[1])
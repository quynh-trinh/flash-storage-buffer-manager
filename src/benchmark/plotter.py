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
    style = 'Algorithm' if 'Prefetching' not in df.columns else 'Prefetching'
    line_plot = sns.lineplot(x=x_axis_column,
                            y=y_axis_column,
                            hue='Algorithm',
                            style=style,
                            markers=True,
                            ci=None,
                            data=df,
                            palette=sns.color_palette('tab10', n_colors=4))
    line_plot.set_xlabel(x_label)
    line_plot.set_ylabel(y_label)
    legend_title = 'Algorithm' if 'Prefetching' not in df.columns else None
    line_plot.legend(bbox_to_anchor=(1,1), loc='upper left', title = legend_title)
    sns.despine()
    plt.savefig(f'{BENCHMARK_DATA_FOLDER}/{benchmark_name}_{y_axis_column}_{context}.png', bbox_inches='tight', dpi=dpi)
    plt.close(fig)

def plot(benchmark_name: str):
    x_column = 'relative_buffer_pool_size'
    x_label = 'Buffer pool size relative to dataset size (%)'
    non_prefetching_csv_file_name = None
    if benchmark_name == 'trace_90p_reads':
        csv_file_name = f'{BENCHMARK_DATA_FOLDER}/trace_90p_reads_timing.csv'
    elif benchmark_name == 'trace_20p_reads':
        csv_file_name = f'{BENCHMARK_DATA_FOLDER}/trace_20p_reads_timing.csv'
    elif benchmark_name == 'read_ratio':
        csv_file_name = f'{BENCHMARK_DATA_FOLDER}/read_ratio_timing.csv'
        x_column = 'percent_reads'
        x_label = 'Percent of accesses that are reads (%)'
    elif benchmark_name == 'synthetic':
        csv_file_name = f'{BENCHMARK_DATA_FOLDER}/synthetic.csv'
    elif benchmark_name == 'prefetching_trace':
        csv_file_name = f'{BENCHMARK_DATA_FOLDER}/prefetching_trace.csv'
        non_prefetching_csv_file_name = f'{BENCHMARK_DATA_FOLDER}/trace_90p_reads_timing.csv'
    elif benchmark_name == 'prefetching_synthetic':
        csv_file_name = f'{BENCHMARK_DATA_FOLDER}/prefetching_synthetic.csv'
        non_prefetching_csv_file_name = f'{BENCHMARK_DATA_FOLDER}/synthetic_timing.csv'
    else:
        raise ValueError("benchmark_name must be 'trace' or 'synthetic'")
    df = pd.read_csv(csv_file_name)
    if non_prefetching_csv_file_name != None:
        df['Prefetching'] = 'Prefetching'
        # df['algorithm'] = df['algorithm'] + ' (Prefetching)'

        non_prefetching_df = pd.read_csv(non_prefetching_csv_file_name)
        non_prefetching_df['Prefetching'] = 'No Prefetching'
        df = df.append(non_prefetching_df, ignore_index=True)
    df['hit_rate'] = df['num_hits']/df['num_accesses']*100
    df['miss_rate'] = df['num_misses']/df['num_accesses']*100
    # df['ratio_dirty_evictions'] = df['num_dirty_evictions']/df['num_evictions']
    # df.loc[df['num_dirty_evictions'] == 0, 'ratio_dirty_evictions'] = 0

    df.rename(columns = {'algorithm':'Algorithm'}, inplace = True)
    print(df)

    for context in ['talk', 'poster']:
        for graph_info in [
            ['hit_rate', 'Hit Rate (%)'],
            ['num_dirty_evictions', 'Dirty Evictions'],
            ['time', 'Execution Time (s)']
        ]:
            if graph_info[0] in df.columns:
                do_plot(benchmark_name, df, context, x_column, graph_info[0], x_label, graph_info[1])


if __name__ == '__main__':
    if len(sys.argv) < 2:
        plot('trace_90p_reads')
        plot('trace_20p_reads')
        plot('read_ratio')
        plot('synthetic')
        plot('prefetching_trace')
        # plot('prefetching_synthetic')
    else:
        plot(sys.argv[1])
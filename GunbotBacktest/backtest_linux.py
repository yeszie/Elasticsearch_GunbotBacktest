import subprocess
import time
import os
import signal
import shutil
import json
import re
import requests
from datetime import datetime, timedelta
from multiprocessing import Process, Queue, current_process

# Define constants
BASE_WORKING_DIRECTORY = '/home/yeszie/Desktop/gunbot/gb-instances'
RAM_HDD = '/mnt/ramdisk'
RESULTS_DIRECTORY = '/home/yeszie/Desktop/gunbot/results-cache'
WORKERS_DIRECTORY = os.path.join(RAM_HDD, 'workers')
#WORKERS_DIRECTORY = os.path.join(BASE_WORKING_DIRECTORY, 'workers')
WORKING_DIRECTORIES = [os.path.join(BASE_WORKING_DIRECTORY, str(i)) for i in range(1, 41                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 )]
COMMAND = ['./gunthy-linux']

# Log directories and files
LOG_DIRECTORY = '/home/yeszie/Desktop/gunbot/logs'
ERROR_LOG = os.path.join(LOG_DIRECTORY, 'errors.log')
WORKER_LOG = os.path.join(LOG_DIRECTORY, 'workers.log')
TASK_LOG = os.path.join(LOG_DIRECTORY, 'tasks.log')
RESULT_LOG = os.path.join(LOG_DIRECTORY, 'results.log')

# Ensure log directory exists
os.makedirs(LOG_DIRECTORY, exist_ok=True)

def clear_results_cache():
    if os.path.exists(RESULTS_DIRECTORY):
        for filename in os.listdir(RESULTS_DIRECTORY):
            if filename.endswith('.json'):
                file_path = os.path.join(RESULTS_DIRECTORY, filename)
                try:
                    os.remove(file_path)
                    log_message(f"Deleted JSON file: {file_path}", TASK_LOG, also_print=True, not_log=True)
                except Exception as e:
                    log_message(f"Error deleting {file_path}: {e}", ERROR_LOG, also_print=True, not_log=True)


def log_message(message, log_file=WORKER_LOG, also_print=False, not_log=True):
    if also_print:
        print(f"[{datetime.now()}] {message}")
    if not not_log:
        with open(log_file, 'a') as f:
            f.write(f"[{datetime.now()}] {message}\n")

def get_binance_tickers():
    try:
        url = 'https://api.binance.com/api/v3/ticker/24hr'
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        log_message(f"Error fetching Binance tickers: {e}", ERROR_LOG, also_print=True, not_log=True)
        return []

def get_coingecko_price_changes():
    try:
        url = 'https://api.coingecko.com/api/v3/coins/markets'
        params = {
            'vs_currency': 'usd',
            'order': 'market_cap_desc',
            'per_page': 250,
            'page': 1,
            'price_change_percentage': '7d'
        }
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        log_message(f"Error fetching CoinGecko price changes: {e}", ERROR_LOG, also_print=True, not_log=False)
        return []

def normalize(values):
    min_val = min(values)
    max_val = max(values)
    return [(val - min_val) / (max_val - min_val) if max_val != min_val else 0 for val in values]

def get_trending_pairs(limit_per_category=45):
    binance_tickers = get_binance_tickers()
    coingecko_price_changes = get_coingecko_price_changes()

    # Map CoinGecko data to symbols
    cg_price_changes = {coin['symbol'].upper(): coin['price_change_percentage_7d_in_currency'] for coin in coingecko_price_changes}

    # Create a list of CoinGecko symbols with common mappings to Binance symbols
    cg_symbols = {coin['symbol'].upper(): coin['symbol'].upper() for coin in coingecko_price_changes}

    quote_volumes = [float(ticker['quoteVolume']) for ticker in binance_tickers]
    price_changes_24h = [max(0, float(ticker['priceChangePercent'])) for ticker in binance_tickers]
    trade_counts = [float(ticker['count']) for ticker in binance_tickers]
    volumes = [float(ticker['volume']) for ticker in binance_tickers]

    # Map Binance pairs to CoinGecko symbols and get the 7d price change, default to 0 if not found
    price_changes_7d = []
    for ticker in binance_tickers:
        base = ticker['symbol'][:-4]  # Remove the quote currency part
        if base in cg_symbols:
            price_change_7d = cg_price_changes.get(cg_symbols[base], 0)
        else:
            price_change_7d = 0
        price_changes_7d.append(price_change_7d or 0)  # Ensure no None values

    normalized_quote_volumes = normalize(quote_volumes)
    normalized_price_changes_24h = normalize(price_changes_24h)
    normalized_trade_counts = normalize(trade_counts)
    normalized_volumes = normalize(volumes)
    normalized_price_changes_7d = normalize(price_changes_7d)

    # Calculate a score based on normalized values with a stronger focus on positive price change
    for i, ticker in enumerate(binance_tickers):
        ticker['score'] = (
            normalized_quote_volumes[i] * 0.3 +
            normalized_price_changes_24h[i] * 0.2 +
            normalized_trade_counts[i] * 0.2 +
            normalized_price_changes_7d[i] * 0.3
        )

    # Sort by the score
    binance_tickers.sort(key=lambda x: x['score'], reverse=True)
    
    filtered_pairs = {'BTC': [], 'ETH': [], 'USDT': []}
    for ticker in binance_tickers:
        pair = ticker['symbol']
        if pair.endswith('BTC') and len(filtered_pairs['BTC']) < limit_per_category:
            filtered_pairs['BTC'].append(pair)
        elif pair.endswith('ETH') and len(filtered_pairs['ETH']) < limit_per_category:
            filtered_pairs['ETH'].append(pair)
        elif pair.endswith('USDT') and len(filtered_pairs['USDT']) < limit_per_category:
            filtered_pairs['USDT'].append(pair)

        # Break if all categories have reached the limit
        if all(len(filtered_pairs[key]) >= limit_per_category for key in filtered_pairs):
            break

    combined_filtered_pairs = (
        filtered_pairs['BTC'] + filtered_pairs['ETH'] + filtered_pairs['USDT']
    )
    
    return combined_filtered_pairs

def transform_pair(pair):
    if pair.endswith('USDC'):
        return f"USDC-{pair[:-4]}"
    if pair.endswith('PLN'):
        return f"PLN-{pair[:-3]}"
    if pair.endswith('BTC'):
        return f"BTC-{pair[:-3]}"
    elif pair.endswith('ETH'):
        return f"ETH-{pair[:-3]}"
    elif pair.endswith('USDT'):
        return f"USDT-{pair[:-4]}"
    return pair

def delete_folders(working_directory):
    json_folder = os.path.join(working_directory, 'json')
    backtesting_folder = os.path.join(working_directory, 'backtesting')
    backtesting_reports_folder = os.path.join(working_directory, 'backtestingReports')
    
    if os.path.exists(json_folder):
        shutil.rmtree(json_folder)
        log_message(f"Deleted folder: {json_folder}", TASK_LOG)
    if os.path.exists(backtesting_folder):
        shutil.rmtree(backtesting_folder)
        log_message(f"Deleted folder: {backtesting_folder}", TASK_LOG)
    if os.path.exists(backtesting_reports_folder):
        shutil.rmtree(backtesting_reports_folder)
        log_message(f"Deleted folder: {backtesting_reports_folder}", TASK_LOG)

def read_config(working_directory):
    config_path = os.path.join(working_directory, 'config.js')
    with open(config_path, 'r') as file:
        config_data = file.read()
    config_data = re.sub(r'^module\.exports\s*=\s*', '', config_data)
    config_data = re.sub(r';\s*$', '', config_data)
    return json.loads(config_data)

#FIXED_PAIRS = ['USDT-ADA']

def write_config(config, working_directory):
    config_path = os.path.join(working_directory, 'config.js')
    config_data = json.dumps(config, indent=4)
    with open(config_path, 'w') as file:
        file.write(config_data)

def ensure_pair_config(config, pair):
    exchange = 'binance'
    config['pairs'][exchange] = {}
    config['pairs'][exchange][pair] = {
        "strategy": "stepgrid",
        "enabled": True,
        "override": {
                    "BUY_METHOD": "stepgrid",
                    "SELL_METHOD": "stepgrid",
                    "MAX_BUY_COUNT": 200,
                    "MIN_VOLUME_TO_SELL": 10,
                    "MAX_INVESTMENT": 999999999999999,
                    "PERIOD": 15,
                    "AUTO_STEP_SIZE": True,
                    "STEP_SIZE": "500",
                    "ENFORCE_STEP": True,
                    "unit_cost": False,
                    "STOP_AFTER_SELL": False,
                    "FOREVER_BAGS": False,
                    "BUY_ENABLED": True,
                    "SELL_ENABLED": True,
                    "PROTECT_PARTIAL_SELL": True,
                    "SMAPERIOD": 50,
                    "ATR_PERIOD": 50,
                    "KEEP_QUOTE": 0,
                    "IGNORE_TRADES_BEFORE": 0,
                    "BF_SINCE": 0,
                    "BF_UNTIL": 0,
                    "DEEP_TESTING": False
        }
    }

def update_config(config, pair, start, end):
    if not config['bot']['BACKFESTER']:
        config['bot']['BACKFESTER'] = True
    
    base, quote = pair.split('-')[0], pair.split('-')[1]
    balance = float(config['bot']['simulatorBalances']['binance'][base])
    
    trading_limit = balance / 200
    min_volume_to_sell = trading_limit * 0.1

    ensure_pair_config(config, pair)
    
    config['pairs']['binance'][pair]['override']['BF_SINCE'] = start
    config['pairs']['binance'][pair]['override']['BF_UNTIL'] = end
    config['pairs']['binance'][pair]['override']['TRADING_LIMIT'] = trading_limit
    config['pairs']['binance'][pair]['override']['MIN_VOLUME_TO_SELL'] = min_volume_to_sell
    config['pairs']['binance'][pair]['override']['INITIAL_FUNDS'] = balance

def copy_to_worker_directory(working_directory, worker_directory):
    log_message(f"Copying {working_directory} to {worker_directory}", TASK_LOG)
    if os.path.exists(worker_directory):
        shutil.rmtree(worker_directory)
    shutil.copytree(working_directory, worker_directory)

def copy_backtesting_report_to_results_cache(worker_directory):
    worker_backtesting_reports = os.path.join(worker_directory, 'backtestingReports')
    if os.path.exists(worker_backtesting_reports):
        for file_name in os.listdir(worker_backtesting_reports):
            if file_name.endswith('.json'):
                src = os.path.join(worker_backtesting_reports, file_name)
                dest = os.path.join(RESULTS_DIRECTORY, file_name)
                try:
                    shutil.copy(src, dest)
                    log_message(f"Copied {src} to {dest}", RESULT_LOG, also_print=True, not_log=False)
                except Exception as e:
                    log_message(f"Error copying {src} to {dest}: {e}", ERROR_LOG, also_print=True, not_log=False)

def run_backtest(task, working_directory, worker_directory):
    pair, start, end = task
    
    # Log queue task
    log_message(f"Queue task: {task}", TASK_LOG)

    # Prepare worker directory
    copy_to_worker_directory(working_directory, worker_directory)
    delete_folders(worker_directory)
    
    # Update config
    config = read_config(worker_directory)
    update_config(config, pair, start, end)
    write_config(config, worker_directory)
    
    # Start the process
    process = subprocess.Popen(['nice', '-n', '4', '--'] + COMMAND, cwd=worker_directory, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    log_message(f"Process {process.pid} started in {worker_directory}.", WORKER_LOG, also_print=True, not_log=True)
    
    last_output_time = time.time()
    error_occurred = False
    
    try:
        while True:
            output = process.stdout.readline()
            if output == '' and process.poll() is not None:
                break
            if output:
                last_output_time = time.time()
                log_message(f"{output.strip()}", also_print=True, not_log=True)
                if 'Backtesting report created successfully' in output or 'Backtester completed the job' in output:
                    time.sleep(3)
                    break
                if 'Error during createBacktestingReport' in output:
                    error_occurred = True
                    log_message(f"Error detected: {output.strip()}", ERROR_LOG, also_print=True, not_log=False)
                    break
            if time.time() - last_output_time > 8:
                log_message(f"No log output for 8 seconds, restarting process {process.pid}", ERROR_LOG, also_print=True, not_log=False)
                os.kill(process.pid, signal.SIGTERM)
                process.wait()
                process = subprocess.Popen(COMMAND, cwd=worker_directory, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                last_output_time = time.time()
    finally:
        if process.poll() is None:
            os.kill(process.pid, signal.SIGTERM)
            log_message(f"Process {process.pid} in {worker_directory} has been terminated.", WORKER_LOG, also_print=True, not_log=False)
        if not error_occurred:
            copy_backtesting_report_to_results_cache(worker_directory)
            log_message(f"Worker run ended successfully for task: {task}", TASK_LOG, also_print=True, not_log=False)
        else:
            log_message(f"Worker run errored for task: {task}", ERROR_LOG, also_print=True, not_log=False)
        delete_folders(worker_directory)

def load_tasks():
    trading_pairs = [transform_pair(pair) for pair in get_trending_pairs()]
    #trading_pairs = [transform_pair(pair) for pair in FIXED_PAIRS]
    today = datetime.today()
    start = int((today - timedelta(days=30)).replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
    end = int(today.replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
    return [(pair, start, end) for pair in trading_pairs]

def worker(task_queue, working_directory):
    worker_directory = os.path.join(WORKERS_DIRECTORY, f'worker_{working_directory.split("/")[-1]}')
    while True:
        task = task_queue.get()
        if task is None:
            log_message(f"No more tasks, exiting.", WORKER_LOG, also_print=True, not_log=False)
            break
        log_message(f"Processing {task[0]} from {task[1]} to {task[2]}", WORKER_LOG, also_print=True, not_log=False)
        run_backtest(task, working_directory, worker_directory)

if __name__ == "__main__":
    # Clear the results-cache before starting workers
    clear_results_cache()

    # Remove and recreate workers directory
    if os.path.exists(WORKERS_DIRECTORY):
        shutil.rmtree(WORKERS_DIRECTORY)
    os.makedirs(WORKERS_DIRECTORY)
    
    task_list = load_tasks()
    task_queues = [Queue() for _ in WORKING_DIRECTORIES]
    
    # Distribute tasks among the task queues
    for i, task in enumerate(task_list):
        task_queues[i % len(WORKING_DIRECTORIES)].put(task)
    
    # Add sentinel values to signal end of tasks
    for q in task_queues:
        q.put(None)
    
    # Start worker processes with a delay between each
    processes = []
    for i, directory in enumerate(WORKING_DIRECTORIES):
        try:
            log_message(f"Starting process {i+1} for {directory}", WORKER_LOG, also_print=True, not_log=False)
            p = Process(target=worker, args=(task_queues[i], directory), name=f"Worker-{i+1}")
            p.start()
            processes.append(p)
            time.sleep(5)  # delay before starting the next process
        except Exception as e:
            log_message(f"Failed to start process {i+1} for {directory}: {e}", ERROR_LOG, also_print=True, not_log=False)
    
    # Wait for all processes to finish
    for p in processes:
        p.join()
    
    log_message("All tasks completed", WORKER_LOG, also_print=True, not_log=False)

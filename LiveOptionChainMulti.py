import numpy as np
import pandas as pd
import json
import os
import multiprocessing
import time
import datetime
import threading
import csv
import time
from csv import writer
from tqdm import tqdm
from datetime import timedelta
from fast_arrow import (
    Client,
    StockMarketdata,
    OptionChain,
    Option,
    OptionMarketdata
)

class SetupAndRecord:

    def __init__(self, client, symbols, option_directory='/home/leoi137/Desktop/Resources/Data/LiveData4/'):


        """
        Parameters:
        client: your authentication
        symbols: a list of symbols to look at, each one takes up one core
        option_directory: the location to save it on

        """
        self.client = client
        self.symbols = symbols
        self.option_directory = option_directory
        self.option_chain_id = dict()
        self.expiration_dates = dict()
        self.expiration_date_directory = dict()
        self.columns = ['tradability','state','issue_date','chain_symbol', 'adjusted_mark_price', 'strike_price', 
                        'ask_price','ask_size', 'bid_price', 'bid_size', 'break_even_price', 'high_price', 
                        'last_trade_price', 'last_trade_size','low_price','mark_price','open_interest','previous_close_date',
                        'previous_close_price','volume','chance_of_profit_long','chance_of_profit_short',
                        'delta','gamma','implied_volatility','rho','theta','vega','high_fill_rate_buy_price',
                        'high_fill_rate_sell_price','low_fill_rate_buy_price','low_fill_rate_sell_price'] # selected columns to get rid of useless data

    # MAIN Recorder
    def runPeriodically(self, period, lag):
        """
        Parameters:
        period: every minute period you want to record (haven't tried above 60 minutes)
        lag: this is how many seconds to record before the minute you want, this is done
            so the recording centers around each period since it takes a couple of seconds
            to record a symbol so if you start at the 10 minute mark you will get 10 min and 5 sec.
            with a lag it would start at the 10min minus however many seconds. I noticed 3 seconds
            works great but it may depend on your cores.
        """
        
        start = time.perf_counter()

        threads = []
        for s in self.symbols:
            t = threading.Thread(target=self.runEveryPeriod, args=(period, lag, s,))
            t.start()
            threads.append(t)
        for thr in threads:
            thr.join()

        finish = time.perf_counter()
        print(f'Finished in: {finish-start:0.2f} second(s).')

    # MAIN Setup
    def run_setup(self, make_files_and_columns):
        """
        Parameters:
        make_files_and_columns: Boolean value (True or False) to create new files and fill in the columns
        ________________________________________________________
        This threads each symbol and runs the setup method
        """
        start = time.perf_counter()

        split_symbols = [self.symbols[:len(self.symbols)//2], self.symbols[len(self.symbols)//2:]] # Splits into two for less CPU usage
        for i in range(2): # Range is two since it is split into two
            threads = []
            for s in split_symbols[i]:
                print(s)
                t = threading.Thread(target=self.setup, args=(s,make_files_and_columns))
                t.start()
                threads.append(t)
            for thr in threads:
                thr.join()

        finish = time.perf_counter()
        print(f'Finished in: {finish-start:0.2f} second(s).')

    ################## SETUP METHODS ##################

    def setup(self, symbol, make_files_and_columns=True):

        self.expiration_date_directory[symbol] = dict()
        self.create_symbol_path(symbol)
        self.option_chain_id[symbol], self.expiration_dates[symbol] = self.get_option_chain(symbol)
        self.create_expiration_path(symbol)
        self.create_strike_id_dir(symbol)
        if make_files_and_columns:
            self.create_files_and_columns(symbol)
            
    # def setup(self, make_files_and_columns=True):
    #     for s in self.symbols:
    #         self.expiration_date_directory[s] = dict()
    #         self.create_symbol_path(s)
    #         self.option_chain_id[s], self.expiration_dates[s] = self.get_option_chain(s)
    #         self.create_expiration_path(s)
    #         self.create_strike_id_dir()
    #         if make_files_and_columns:
    #             self.create_files_and_columns(s)

     def get_option_chain(symbol):

        """
        This returns the option chain id and an array of the expiration dates
        """
        stock_id = StockMarketdata.quote_by_symbol(client, symbol)['instrument'].split('/')[-2]
        option_chain = OptionChain.fetch(client, stock_id, symbol)
        option_chain_id = option_chain["id"]
        expiration_dates = option_chain['expiration_dates']
        
        return option_chain_id, np.array(expiration_dates)#.reshape(len(expiration_dates),-1)

    def create_symbol_path(self, symbol):
        
        try:
            os.mkdir(self.option_directory+f'{symbol}/')
        except FileExistsError:
            #pass
            print(f"Directory for {symbol} already exists.")

    def create_expiration_path(self, symbol):
        """
        This creates a folder for each expiration date and then sets each
        expiration for the given symbiol within the self.expiration_date_directory
        """
        
        for expiration in self.expiration_dates[symbol]:
            try:
                os.mkdir(self.option_directory+f'{symbol}/{expiration}/')
                # print(f'Created directory: {expiration}')
            except FileExistsError:
                #pass
                print(f"Directory for {expiration} already exists.")

            self.expiration_date_directory[symbol][expiration] = self.option_directory+f'{symbol}/{expiration}/'

    def create_strike_id_dir(self, symbol):
        """
        This creates two csv (call and put) files that contain the
        strike price and its given strike price id for reference of
        which id is which strike price.
        """

        for date in self.expiration_dates[symbol]:
            if not os.path.isfile(self.expiration_date_directory[symbol][date]+'Call_ID_Strike.csv'):
                with open(self.expiration_date_directory[symbol][date]+'Call_ID_Strike.csv', 'w') as empty_csv:
                    csv_writer = writer(empty_csv)
                    csv_writer.writerow(['Strike_Price', 'Strike_ID'])
            if not os.path.isfile(self.expiration_date_directory[symbol][date]+'Put_ID_Strike.csv'):
                with open(self.expiration_date_directory[symbol][date]+'Put_ID_Strike.csv', 'w') as empty_csv:
                    csv_writer = writer(empty_csv)
                    csv_writer.writerow(['Strike_Price', 'Strike_ID'])

    def get_option_data(self, exp_date, symbol):

        ops = Option.in_chain(self.client, self.option_chain_id[symbol], expiration_dates=exp_date)
        ops2 = Option.mergein_marketdata_list(self.client, ops)

        return pd.DataFrame(ops2)

    def write_columns_to_csv(self, option_directory, op_chain):

        for ind in range(len(op_chain)):
            strike_id = op_chain.loc[ind,:]['id']
            path = option_directory+f'{strike_id}.csv'
            # print(strike_id)
            # print(path)

            try:
                pd.read_csv(option_directory+f'{strike_id}.csv')
            except FileNotFoundError:
                # print(f'Creating file {strike_id} and adding columns...')
                with open(path, 'a+', newline='') as writer_obj:
                    csv_writer = writer(writer_obj)
                    csv_writer.writerow(['Time']+list(self.columns))
            # else:
                # print(f'File {strike_id} already excists.')

    def create_strike_ids(self, option_directory, op_chain):

        for ind in range(len(op_chain)):
            strike_id = op_chain.loc[ind,:]['id']
            if op_chain.values[ind][11] == 'put':
                put_path = option_directory+'Put_ID_Strike.csv'
                if strike_id not in pd.read_csv(put_path)['Strike_ID'].values:
    #                 print('not there')
                    with open(put_path, 'a+', newline='') as writer_obj:
                        csv_writer = writer(writer_obj)
                        csv_writer.writerow([float(op_chain.values[ind][9])]+[strike_id])
            elif op_chain.values[ind][11] == 'call':
                call_path = option_directory+'Call_ID_Strike.csv'
                if strike_id not in pd.read_csv(call_path)['Strike_ID'].values:
                    with open(call_path, 'a+', newline='') as writer_obj:
                        csv_writer = writer(writer_obj)
                        csv_writer.writerow([float(op_chain.values[ind][9])]+[strike_id])

    def finish_Setup(self, expiration_date, symbol):

        op_data = self.get_option_data([expiration_date], symbol)
        self.write_columns_to_csv(self.expiration_date_directory[symbol][expiration_date], op_data)
        self.create_strike_ids(self.expiration_date_directory[symbol][expiration_date], op_data)

    def create_files_and_columns(self, symbol):

        """
        This multiprocesses the creation of new csv files when new data comes out
        and it creates the columns for the option chain to then append data to.
        This also adds the strike id and their given strike price to the reference strike
        id file.
        """

        mem = []

        counter = 0
        start = 0
        end = len(self.expiration_dates[symbol])+1
        step = len(self.expiration_dates[symbol])+1

        for _ in tqdm(range((len(self.expiration_dates[symbol])//step+1))):
            processes = []
            for exp_date in self.expiration_dates[symbol][start:end]:
                p = multiprocessing.Process(target=self.finish_Setup, args=[exp_date, symbol])
                processes.append(p)
                p.start()    
            for process in processes:
                process.join()
            del processes
            start+=step
            end+=step
            time.sleep(10)

    ################## RECORDING METHODS ##################

    def write_to_csv(self, option_directory, op_chain):

        for ind in range(len(op_chain)):
            strike_id = op_chain.loc[ind,:]['id']
            today = datetime.datetime.now()
            year, month, day, hour, minute, second = today.year,today.month,today.day,today.hour,today.minute,today.second
            todays_date = str(datetime.datetime(year,month,day,hour,minute,second))

            path = option_directory+f'{strike_id}.csv'
            with open(path, 'a+', newline='') as writer_obj:
                csv_writer = writer(writer_obj)
                csv_writer.writerow([todays_date]+list(op_chain[self.columns].iloc[ind,:]))

    def StartRecording(self):

        beingRecording = False
        while True:
            right_now = datetime.datetime.now()
            if right_now.hour >= 6 and right_now.minute>=0:
                beingRecording=True
                # print('start')
                break
        return beingRecording

    def runRecording(self, expiration_date, symbol):
        """
        This appends the data to each correponding csv file
        """

        op_data = self.get_option_data([expiration_date], symbol)
        self.write_to_csv(self.expiration_date_directory[symbol][expiration_date], op_data)

    def multiprocessRecording(self, symbol):

        """
        This multiprocesses each symbols expiration dates in order to run them
        concurrently, it does this so you get the recording of all expirations at
        around the same amount of time. It may use up all your cores though, LOL.
        """

        mem = []

        counter = 0
        start = 0
        end = len(self.expiration_dates[symbol])+1
        step = len(self.expiration_dates[symbol])+1

        for _ in range((len(self.expiration_dates[symbol])//step+1)):
            processes = []
            for exp_date in self.expiration_dates[symbol][start:end]:
                p = multiprocessing.Process(target=self.runRecording, args=[exp_date, symbol])
                processes.append(p)
                p.start()    
            for process in processes:
                process.join()
            del processes
            start+=step
            end+=step

    def get_periods(self, period):
        """
        This only works with the lag, basically it subtracts one minute and returns
        a set of every period to record on. If no lag then it will record 1 minute too soon.
        """

        return set([p-1 for p in range(1,61) if p%period==0])

    def get_sleep_time(self, period, wait=15):
        """
        Parameters:
        period: the minute period to record
        wait: amount of seconds to before each recording period to wake up on
        """

        return (period*60)-wait

    def runEveryPeriod(self, period, lag, symbol):

        """
        This records every minute period.
        """

        start = True
        sixty = datetime.timedelta(seconds=60)
        lag = datetime.timedelta(seconds=lag)
        periods = self.get_periods(period) # gets each time to record
        sleep_time = self.get_sleep_time(period) # makes the program sleep between each period recoding so it isnt looping and not doing anything

        # print(periods)
        # print(sleep_time)

        print(f'Starting: {symbol}')

        if self.StartRecording(): # Makes sure it is passed 6:30am PCT
            while True:
                right_now = datetime.datetime.now()
                if right_now.hour>=13: # if it is passed 1pm break, it doesn't stop right at 1 but 1 + your period cause it is sleeping after it records.
                    break

                if right_now.minute in periods:
                    if right_now.second>=(sixty-lag).seconds:
                        print(right_now, f'Recording {symbol}...')
                        self.multiprocessRecording(symbol)
                        time.sleep(sleep_time) # Sleeps passed the first 13 hour break mark

# -*- coding: utf-8 -*-
import os
import ccxt
import time
import logging
import requests
import json
from logging.handlers import TimedRotatingFileHandler

class Reaper:
    def __init__(self, config):
        self.config = config
        self.symbol = config["symbol"]
        self.base, self.quote = self.symbol.split('/')
        self.unbalance_ratio = config["unbalance_ratio"]
        self.monitor_interval = config["monitor_interval"]
        self.order_wait_time = config["order_wait_time"]
        self.log_interval = config["log_interval"]
        self.exchange = ccxt.okx({
            'apiKey': config["apiKey"],
            'secret': config["secret"],
            'password': config["password"],
            # 'proxies': {'http': 'http://127.0.0.1:10100', 'https': 'http://127.0.0.1:10100'},
        })
        
        self.logger = logging.getLogger('Reaper')
        self.logger.setLevel(logging.INFO)
        
        if not os.path.exists('logs'):
            os.makedirs('logs')
            
        handler = TimedRotatingFileHandler(
            'logs/reaper.log',
            when='midnight',
            interval=1,
            backupCount=30,
            encoding='utf-8'
        )
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        
    def get_symbol(self):
        return self.symbol, self.base, self.quote
        
    def get_mark_price(self):
        return self.exchange.fetchMarkPrice(self.symbol)['markPrice']
    
    def get_last_price(self):
        return self.exchange.fetchTicker(self.symbol)['last']
    
    def get_balance(self):
        balance = self.exchange.fetchBalance()['total']
        base_amount = balance[self.base]
        quote_price = balance[self.quote]
        
        mark_price = self.get_mark_price()
        base_price = mark_price * base_amount
        total_price = base_price + quote_price
        
        base_ratio = base_price / total_price
        quote_ratio = quote_price / total_price
        
        s1 = f"{self.base}:{base_amount}  =  {self.quote}:{base_price} @ {mark_price}"
        s2 = f"{self.quote}:{quote_price}"
        s3 = f"percent: {self.base}:{base_ratio:.4f}  {self.quote}:{quote_ratio:.4f}"
        
        return s1 + "\n" + s2 + "\n" + s3
    
    def monitor_positions(self, print_log=False):
        balance = self.exchange.fetchBalance()['total']
        base_amount = balance[self.base]
        quote_price = balance[self.quote]
        
        mark_price = self.get_mark_price()
        base_price = mark_price * base_amount
        total_price = base_price + quote_price
        
        base_ratio = base_price / total_price
        quote_ratio = quote_price / total_price
        
        if print_log:
            s1 = f"{self.base}:{base_amount}  =  {self.quote}:{base_price} @ {mark_price}"
            s2 = f"{self.quote}:{quote_price}"
            s3 = f"percent: {self.base}:{base_ratio:.4f}  {self.quote}:{quote_ratio:.4f}"
            log_message = f"\n{s1}\n{s2}\n{s3}"
            print(log_message)
            self.logger.info(log_message)

        if base_ratio > self.unbalance_ratio:
            target_base_price = total_price * 0.5
            sell_price = base_price - target_base_price
            return 'sell', sell_price
        elif quote_ratio > self.unbalance_ratio:
            target_quote_price = total_price * 0.5
            buy_price = target_quote_price - quote_price
            return 'buy', buy_price
        
        return None
        
    def place_order(self, direction, price):
        if direction == 'sell':
            self.exchange.createMarketSellOrderWithCost(self.symbol, price)
        elif direction == 'buy':
            self.exchange.createMarketBuyOrderWithCost(self.symbol, price)
        time.sleep(self.order_wait_time)
        open_orders = self.exchange.fetchOpenOrders()
        if len(open_orders) == 0:
            log_message = f"Order executed successfully - Direction: {direction}, Amount: {price} {self.quote}"
            print(log_message)
            self.logger.info(log_message)
            return True
        else:
            for order in open_orders:
                self.exchange.cancelOrder(order['id'], self.symbol)
            return False

    
    def run_trade(self):
        log_cnt = 0
        while True:
            signal = self.monitor_positions(print_log = log_cnt==0)
            if signal is not None:
                direction, price = signal
                if self.place_order(direction, price):
                    pass
            time.sleep(self.monitor_interval)
            log_cnt = (log_cnt + 1) % self.log_interval


if __name__ == '__main__':
    with open('config.json', 'r') as f:
        config = json.load(f)
        
    
    reaper = Reaper(config)
    print(reaper.get_symbol())
    print(reaper.get_mark_price())
    print(reaper.get_last_price())
    print(reaper.get_balance())
    
    reaper.run_trade()
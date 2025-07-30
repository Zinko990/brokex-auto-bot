import requests
import time
import random
import websocket
import json
import pytz
from datetime import datetime
from web3 import Web3
from eth_account import Account
from colorama import Fore, Style
from functools import wraps
from supabase import create_client
import os

def log_print(*args, **kwargs):
    wib = pytz.timezone('Asia/Jakarta')
    now = datetime.now(wib)
    timestamp = now.strftime("%H:%M:%S %d-%m-%Y")
    message = ' '.join(map(str, args))
    print(f"{timestamp} | {message}", **kwargs)

class BrokexBot:
    def __init__(self, private_key=None):
        self.private_key = private_key
        self.account = Account.from_key(private_key) if private_key else None
        self.wallet_address = self.account.address if self.account else None
        self.RPC_URL = "https://testnet.dplabs-internal.com"
        self.CHAIN_ID = 688688
        self.MAX_RETRIES = 5
        self.RETRY_BASE_DELAY = 2
        self.USDT_ADDRESS = Web3.to_checksum_address("0x78ac5e2d8a78a8b8e6d10c7b7274b03c10c91cef")
        self.BROKEX_ADDRESS = Web3.to_checksum_address("0xde897635870b3dd2e097c09f1cd08841dbc3976a")
        self.LIQUIDITY_CONTRACT_ADDRESS = Web3.to_checksum_address("0x9a88d07850723267db386c681646217af7e220d7")
        self.ROUTER_CONTRACT = Web3.to_checksum_address("0x50576285bd33261dee1ad99bf766cd8249520a58")
        self.ORACLE_PROOF_URL = "https://proofcrypto-production.up.railway.app/proof?pairs={}"
        self.USDT_DECIMALS = 6
        self.MIN_USDT_BALANCE = 10
        self.MAX_UINT256 = 2**256 - 1
        self.GAS_LIMIT = 300000
        self.GAS_PRICE = Web3.to_wei(5, 'gwei')
        self.WEBSOCKET_URL = "wss://wss-production-9302.up.railway.app"
        self.SUPABASE_URL = "https://yaikidiqvtxiqtrawvgf.supabase.co"
        self.SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InlhaWtpZGlxdnR4aXF0cmF3dmdmIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDM3MDI3MzcsImV4cCI6MjA1OTI3ODczN30.z2gZvFpA5HMIODCpjXJFNX0amE3V5MqAgJSrIr7jS1Y"
        self.STALE_ORDER_DEVIATION_PCT = 15
        self.nonce_cache = {}
        self.asset_data = {}
        self.USDT_ABI = json.loads('[{"constant":true,"inputs":[{"name":"owner","type":"address"}],"name":"balanceOf","outputs":[{"name":"","type":"uint256"}],"type":"function"},{"constant":true,"inputs":[{"name":"owner","type":"address"},{"name":"spender","type":"address"}],"name":"allowance","outputs":[{"name":"","type":"uint256"}],"type":"function"},{"constant":false,"inputs":[{"name":"spender","type":"address"},{"name":"value","type":"uint256"}],"name":"approve","outputs":[{"name":"","type":"bool"}],"type":"function"}]')
        self.BROKEX_ABI = json.loads('[{"inputs":[{"internalType":"address","name":"user","type":"address"}],"name":"getUserOpenIds","outputs":[{"internalType":"uint256[]","name":"","type":"uint256[]"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"id","type":"uint256"}],"name":"getOpenById","outputs":[{"components":[{"internalType":"address","name":"trader","type":"address"},{"internalType":"uint256","name":"id","type":"uint256"},{"internalType":"uint256","name":"assetIndex","type":"uint256"},{"internalType":"bool","name":"isLong","type":"bool"},{"internalType":"uint256","name":"leverage","type":"uint256"},{"internalType":"uint256","name":"openPrice","type":"uint256"},{"internalType":"uint256","name":"sizeUsd","type":"uint256"},{"internalType":"uint256","name":"timestamp","type":"uint256"},{"internalType":"uint256","name":"stopLossPrice","type":"uint256"},{"internalType":"uint256","name":"takeProfitPrice","type":"uint256"},{"internalType":"uint256","name":"liquidationPrice","type":"uint256"}],"internalType":"struct IBrokexStorage.Open","name":"","type":"tuple"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"assetIndex","type":"uint256"},{"internalType":"bool","name":"isLong","type":"bool"},{"internalType":"uint256","name":"leverage","type":"uint256"},{"internalType":"uint256","name":"orderPrice","type":"uint256"},{"internalType":"uint256","name":"sizeUsd","type":"uint256"},{"internalType":"uint256","name":"stopLoss","type":"uint256"},{"internalType":"uint256","name":"takeProfit","type":"uint256"}],"name":"placeOrder","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"user","type":"address"}],"name":"getUserOrderIds","outputs":[{"internalType":"uint256[]","name":"","type":"uint256[]"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"id","type":"uint256"}],"name":"getOrderById","outputs":[{"components":[{"internalType":"address","name":"trader","type":"address"},{"internalType":"uint256","name":"id","type":"uint256"},{"internalType":"uint256","name":"assetIndex","type":"uint256"},{"internalType":"bool","name":"isLong","type":"bool"},{"internalType":"uint256","name":"leverage","type":"uint256"},{"internalType":"uint256","name":"orderPrice","type":"uint256"},{"internalType":"uint256","name":"sizeUsd","type":"uint256"},{"internalType":"uint256","name":"timestamp","type":"uint256"},{"internalType":"uint256","name":"stopLoss","type":"uint256"},{"internalType":"uint256","name":"takeProfit","type":"uint256"},{"internalType":"uint256","name":"limitBucketId","type":"uint256"}],"internalType":"struct IBrokexStorage.Order","name":"","type":"tuple"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"assetIndex","type":"uint256"},{"internalType":"bytes","name":"proof","type":"bytes"},{"internalType":"bool","name":"isLong","type":"bool"},{"internalType":"uint256","name":"leverage","type":"uint256"},{"internalType":"uint256","name":"sizeUsd","type":"uint256"},{"internalType":"uint256","name":"stopLoss","type":"uint256"},{"internalType":"uint256","name":"takeProfit","type":"uint256"}],"name":"openPosition","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"openId","type":"uint256"},{"internalType":"bytes","name":"proof","type":"bytes"}],"name":"closePosition","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"orderId","type":"uint256"}],"name":"cancelOrder","outputs":[],"stateMutability":"nonpayable","type":"function"}]')
        self.LIQUIDITY_ABI = json.loads('[{"inputs":[{"internalType":"uint256","name":"usdtAmount","type":"uint256"}],"name":"depositLiquidity","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"lpAmount","type":"uint256"}],"name":"withdrawLiquidity","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"account","type":"address"}],"name":"balanceOf","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"getLpPrice","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"}]')
        self.CLAIM_ABI = json.loads('[{"name":"claim","type":"function","stateMutability":"nonpayable","inputs":[],"outputs":[]}]')
        self.supabase = create_client(self.SUPABASE_URL, self.SUPABASE_KEY)

    def clear_terminal(self):
        os.system('cls' if os.name == 'nt' else 'clear')

    def display_banner(self):
        banner = """
        ██╗ ██████╗ ██████╗ ██╗
        ██║██╔═══██╗██╔══██╗██║
        ██║██║   ██║██████╔╝██║
        ██║██║   ██║██╔═══╝ ██║
        ██║╚██████╔╝██║     ██║
        ╚═╝ ╚═════╝ ╚═╝     ╚═╝
        Is Her
        Pharos Brokex 
        """
        print(Fore.MAGENTA + banner + Style.RESET_ALL)

    @staticmethod
    def with_retry(max_retries, base_delay):
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                for attempt in range(1, max_retries + 1):
                    try:
                        return func(*args, **kwargs)
                    except Exception as e:
                        log_print(f"{Fore.YELLOW}[Retry {attempt}/{max_retries}] Error: {e}{Style.RESET_ALL}")
                        if attempt == max_retries:
                            raise
                        sleep_time = base_delay * (2 ** (attempt - 1)) + random.uniform(0, 1)
                        log_print(f"{Fore.YELLOW}🔁 Retrying in {sleep_time:.2f} seconds...{Style.RESET_ALL}")
                        time.sleep(sleep_time)
            return wrapper
        return decorator

    @with_retry(max_retries=5, base_delay=2)
    def connect_web3(self):
        web3 = Web3(Web3.HTTPProvider(self.RPC_URL))
        if not web3.is_connected():
            raise Exception("Gagal konek ke RPC")
        return web3

    @with_retry(max_retries=5, base_delay=2)
    def wait_tx_receipt_and_status(self, web3, tx_hash):
        log_print(f"{Fore.YELLOW}⏳ Waiting tx receipt: 0x{tx_hash.hex()} ...{Style.RESET_ALL}")
        receipt = web3.eth.wait_for_transaction_receipt(tx_hash, timeout=180)
        if receipt.status == 1:
            log_print(f"{Fore.GREEN}✅ TX success{Style.RESET_ALL}")
            return True
        else:
            log_print(f"{Fore.RED}❌ TX failed{Style.RESET_ALL}")
            return False

    @with_retry(max_retries=5, base_delay=2)
    def get_nonce(self, web3, wallet):
        nonce = web3.eth.get_transaction_count(wallet, 'pending')
        self.nonce_cache[wallet] = max(self.nonce_cache.get(wallet, nonce), nonce)
        return self.nonce_cache[wallet]

    @with_retry(max_retries=5, base_delay=2)
    def send_raw_tx(self, web3, signed_tx):
        return web3.eth.send_raw_transaction(signed_tx.raw_transaction)

    @with_retry(max_retries=5, base_delay=2)
    def update_asset_data_from_websocket(self):
        log_print(f"{Fore.CYAN}🔄 Menghubungkan ke WebSocket untuk memperbarui data aset...{Style.RESET_ALL}")
        try:
            ws = websocket.create_connection(self.WEBSOCKET_URL, timeout=15)
            message = ws.recv()
            ws.close()
            payload = json.loads(message)
            temp_data = {}
            for obj in payload.values():
                if obj.get('instruments'):
                    instrument = obj['instruments'][0]
                    asset_id = obj.get('id')
                    price = instrument.get('currentPrice')
                    name = obj.get('name') or instrument.get('tradingPair')
                    if asset_id is not None and price is not None and name:
                        if '/' in name: name = name.upper()
                        temp_data[str(asset_id)] = {"name": name, "price": float(price)}
            if not temp_data:
                raise Exception("Data aset dari WebSocket kosong.")
            self.asset_data = temp_data
            log_print(f"{Fore.GREEN}✅ Data untuk {len(self.asset_data)} aset berhasil diperbarui.{Style.RESET_ALL}")
            return True
        except Exception as e:
            log_print(f"{Fore.RED}❌ Gagal mengambil data dari WebSocket: {e}{Style.RESET_ALL}")
            raise e

    @with_retry(max_retries=5, base_delay=2)
    def get_usdt_balance(self, web3):
        usdt_contract = web3.eth.contract(address=self.USDT_ADDRESS, abi=self.USDT_ABI)
        balance = usdt_contract.functions.balanceOf(self.wallet_address).call()
        return balance / 10**self.USDT_DECIMALS

    @with_retry(max_retries=5, base_delay=2)
    def approve_usdt(self, web3, spender_address):
        usdt_contract = web3.eth.contract(address=self.USDT_ADDRESS, abi=self.USDT_ABI)
        log_print(f"{Fore.CYAN}🔓 Melakukan approve maksimum USDT untuk spender {spender_address}...{Style.RESET_ALL}")
        nonce = self.get_nonce(web3, self.wallet_address)
        tx = usdt_contract.functions.approve(spender_address, self.MAX_UINT256).build_transaction({
            "chainId": self.CHAIN_ID, "gas": 300000, "gasPrice": self.GAS_PRICE, "nonce": nonce,
        })
        signed_tx = web3.eth.account.sign_transaction(tx, self.private_key)
        tx_hash = self.send_raw_tx(web3, signed_tx)
        return self.wait_tx_receipt_and_status(web3, tx_hash)

    @with_retry(max_retries=5, base_delay=2)
    def claim_usdt(self, web3):
        log_print(f"{Fore.CYAN}🔄 Mencoba untuk claim USDT...{Style.RESET_ALL}")
        router_contract = web3.eth.contract(address=self.ROUTER_CONTRACT, abi=self.CLAIM_ABI)
        balance = self.get_usdt_balance(web3)
        if balance >= 1000:
            log_print(f"{Fore.GREEN}✅ Skip claim - Saldo USDT cukup ({balance:.2f}).{Style.RESET_ALL}")
            return True
        nonce = self.get_nonce(web3, self.wallet_address)
        tx = router_contract.functions.claim().build_transaction({
            'from': self.wallet_address, 'nonce': nonce, 'chainId': self.CHAIN_ID, 'gas': self.GAS_LIMIT, 'gasPrice': self.GAS_PRICE,
        })
        signed_tx = web3.eth.account.sign_transaction(tx, self.private_key)
        tx_hash = self.send_raw_tx(web3, signed_tx)
        return self.wait_tx_receipt_and_status(web3, tx_hash)

    @with_retry(max_retries=5, base_delay=2)
    def add_liquidity(self, web3):
        usdt_amount_to_add = round(random.uniform(10.1, 15.5), 2)
        log_print(f"{Fore.CYAN}💧 Mencoba menambah likuiditas sebesar {usdt_amount_to_add:.2f} USDT...{Style.RESET_ALL}")
        balance = self.get_usdt_balance(web3)
        if balance < usdt_amount_to_add:
            log_print(f"{Fore.YELLOW}⚠️ Saldo USDT ({balance:.2f}) tidak cukup. Mencoba claim...{Style.RESET_ALL}")
            if not self.claim_usdt(web3):
                log_print(f"{Fore.RED}❌ Gagal claim USDT. Melewati aksi add liquidity.{Style.RESET_ALL}")
                return False
            balance = self.get_usdt_balance(web3)
            if balance < usdt_amount_to_add:
                log_print(f"{Fore.RED}❌ Saldo USDT ({balance:.2f}) masih tidak cukup setelah claim.{Style.RESET_ALL}")
                return False
        usdt_contract = web3.eth.contract(address=self.USDT_ADDRESS, abi=self.USDT_ABI)
        allowance = usdt_contract.functions.allowance(self.wallet_address, self.LIQUIDITY_CONTRACT_ADDRESS).call()
        if allowance < usdt_amount_to_add * 10**self.USDT_DECIMALS:
            if not self.approve_usdt(web3, self.LIQUIDITY_CONTRACT_ADDRESS):
                return False
            time.sleep(10)
        nonce = self.get_nonce(web3, self.wallet_address)
        tx = web3.eth.contract(address=self.LIQUIDITY_CONTRACT_ADDRESS, abi=self.LIQUIDITY_ABI).functions.depositLiquidity(int(usdt_amount_to_add * 10**self.USDT_DECIMALS)).build_transaction({
            "chainId": self.CHAIN_ID, "gas": 500000, "gasPrice": self.GAS_PRICE, "nonce": nonce,
        })
        signed_tx = web3.eth.account.sign_transaction(tx, self.private_key)
        tx_hash = self.send_raw_tx(web3, signed_tx)
        return self.wait_tx_receipt_and_status(web3, tx_hash)

    @with_retry(max_retries=5, base_delay=2)
    def withdraw_liquidity(self, web3):
        liquidity_contract = web3.eth.contract(address=self.LIQUIDITY_CONTRACT_ADDRESS, abi=self.LIQUIDITY_ABI)
        log_print(f"{Fore.CYAN}💸 Mencoba menarik sebagian likuiditas...{Style.RESET_ALL}")
        lp_balance_wei = liquidity_contract.functions.balanceOf(self.wallet_address).call()
        if lp_balance_wei == 0:
            log_print(f"{Fore.YELLOW}ℹ️ Tidak memiliki token LP untuk ditarik.{Style.RESET_ALL}")
            return True
        withdraw_percentage = random.randint(10, 50)
        lp_to_withdraw_wei = (lp_balance_wei * withdraw_percentage) // 100
        log_print(f"{Fore.CYAN}ℹ️ Saldo LP: {lp_balance_wei/10**18:.4f}. Akan ditarik: {lp_to_withdraw_wei/10**18:.4f} LP ({withdraw_percentage}%){Style.RESET_ALL}")
        nonce = self.get_nonce(web3, self.wallet_address)
        tx = liquidity_contract.functions.withdrawLiquidity(lp_to_withdraw_wei).build_transaction({
            "chainId": self.CHAIN_ID, "gas": 500000, "gasPrice": self.GAS_PRICE, "nonce": nonce,
        })
        signed_tx = web3.eth.account.sign_transaction(tx, self.private_key)
        tx_hash = self.send_raw_tx(web3, signed_tx)
        return self.wait_tx_receipt_and_status(web3, tx_hash)

    @with_retry(max_retries=5, base_delay=2)
    def place_limit_order(self, web3):
        if not self.asset_data:
            log_print(f"{Fore.RED}❌ Data aset tidak tersedia untuk limit order.{Style.RESET_ALL}")
            return False
        asset_index_str = random.choice(list(self.asset_data.keys()))
        asset_info = self.asset_data[asset_index_str]
        asset_name, current_price = asset_info["name"], asset_info["price"]
        usd_size_float = round(random.uniform(10.1, 20.5), 2)
        if self.get_usdt_balance(web3) < usd_size_float:
            log_print(f"{Fore.YELLOW}⚠️ Saldo USDT tidak cukup untuk limit order.{Style.RESET_ALL}")
            return False
        is_long = random.choice([True, False])
        leverage = random.randint(2, 10)
        target_price_float = current_price * random.uniform(0.995, 1.005)
        target_price = int(target_price_float * 10**18)
        sl_price = int(target_price * (0.95 if is_long else 1.05))
        tp_price = int(target_price * (1.05 if is_long else 0.95))
        log_print(f"{Fore.CYAN}⌛ Menempatkan Limit Order: {asset_name.upper()} | {'LONG' if is_long else 'SHORT'}, Size={usd_size_float:.2f} USDT, Lev={leverage}x, Target=${target_price_float:.4f}{Style.RESET_ALL}")
        if self.get_usdt_balance(web3) < usd_size_float:
             log_print(f"{Fore.YELLOW}⚠️ Saldo USDT tidak cukup untuk limit order.{Style.RESET_ALL}")
             return False
        usdt_contract = web3.eth.contract(address=self.USDT_ADDRESS, abi=self.USDT_ABI)
        allowance = usdt_contract.functions.allowance(self.wallet_address, self.BROKEX_ADDRESS).call()
        if allowance < usd_size_float * 10**self.USDT_DECIMALS:
            if not self.approve_usdt(web3, self.BROKEX_ADDRESS):
                return False
            time.sleep(10)
        nonce = self.get_nonce(web3, self.wallet_address)
        tx = web3.eth.contract(address=self.BROKEX_ADDRESS, abi=self.BROKEX_ABI).functions.placeOrder(
            int(asset_index_str), is_long, leverage, target_price, int(usd_size_float * 10**self.USDT_DECIMALS), sl_price, tp_price
        ).build_transaction({
            "chainId": self.CHAIN_ID, "gas": 1500000, "gasPrice": self.GAS_PRICE, "nonce": nonce,
        })
        signed_tx = web3.eth.account.sign_transaction(tx, self.private_key)
        tx_hash = self.send_raw_tx(web3, signed_tx)
        if self.wait_tx_receipt_and_status(web3, tx_hash):
            log_print(f"{Fore.GREEN}✅ Limit Order {asset_name.upper()} sukses! Tx: 0x{tx_hash.hex()}{Style.RESET_ALL}")
            return True
        else:
            log_print(f"{Fore.RED}❌ Limit Order {asset_name.upper()} gagal. Tx: 0x{tx_hash.hex()}{Style.RESET_ALL}")
            return False

    @with_retry(max_retries=5, base_delay=2)
    def open_market_position(self, web3):
        if not self.asset_data:
            log_print(f"{Fore.RED}❌ Data aset tidak tersedia untuk market order.{Style.RESET_ALL}")
            return False
        asset_index_str = random.choice(list(self.asset_data.keys()))
        asset_info = self.asset_data[asset_index_str]
        asset_name, current_price = asset_info["name"], asset_info["price"]
        usd_size_float = round(random.uniform(10.1, 20.5), 2)
        if self.get_usdt_balance(web3) < usd_size_float:
            log_print(f"{Fore.YELLOW}⚠️ Saldo USDT tidak cukup untuk market order.{Style.RESET_ALL}")
            return False
        is_long = random.choice([True, False])
        leverage = random.randint(2, 10)
        sl_price = int(current_price * (0.95 if is_long else 1.05) * 10**18)
        tp_price = int(current_price * (1.05 if is_long else 0.95) * 10**18)
        log_print(f"{Fore.CYAN}📊 Membuka Posisi Market: {asset_name.upper()} | {'LONG' if is_long else 'SHORT'}, Size={usd_size_float:.2f} USDT, Lev={leverage}x{Style.RESET_ALL}")
        log_print(f"{Fore.CYAN}   Oracle: Mengambil proof...{Style.RESET_ALL}")
        proof_response = requests.get(self.ORACLE_PROOF_URL.format(asset_index_str), timeout=10)
        if proof_response.status_code != 200:
            log_print(f"{Fore.RED}   ❌ Oracle: Gagal mendapatkan proof, status {proof_response.status_code}{Style.RESET_ALL}")
            return False
        proof = proof_response.json().get('proof')
        if not proof:
            log_print(f"{Fore.RED}   ❌ Oracle: Proof tidak ditemukan dalam respons.{Style.RESET_ALL}")
            return False
        log_print(f"{Fore.GREEN}   ✅ Oracle: Proof berhasil didapatkan.{Style.RESET_ALL}")
        usdt_contract = web3.eth.contract(address=self.USDT_ADDRESS, abi=self.USDT_ABI)
        allowance = usdt_contract.functions.allowance(self.wallet_address, self.BROKEX_ADDRESS).call()
        if allowance < usd_size_float * 10**self.USDT_DECIMALS:
            if not self.approve_usdt(web3, self.BROKEX_ADDRESS):
                return False
            time.sleep(10)
        nonce = self.get_nonce(web3, self.wallet_address)
        tx = web3.eth.contract(address=self.BROKEX_ADDRESS, abi=self.BROKEX_ABI).functions.openPosition(
            int(asset_index_str), bytes.fromhex(proof[2:]), is_long, leverage, int(usd_size_float * 10**self.USDT_DECIMALS), sl_price, tp_price
        ).build_transaction({
            "chainId": self.CHAIN_ID, "gas": 2000000, "gasPrice": self.GAS_PRICE, "nonce": nonce
        })
        signed_tx = web3.eth.account.sign_transaction(tx, self.private_key)
        tx_hash = self.send_raw_tx(web3, signed_tx)
        if self.wait_tx_receipt_and_status(web3, tx_hash):
            log_print(f"{Fore.GREEN}✅ Posisi Market {asset_name.upper()} berhasil dibuka! Tx: 0x{tx_hash.hex()}{Style.RESET_ALL}")
            return True
        else:
            log_print(f"{Fore.RED}❌ Posisi Market {asset_name.upper()} gagal. Tx: 0x{tx_hash.hex()}{Style.RESET_ALL}")
            return False

    @with_retry(max_retries=3, base_delay=5)
    def close_position(self, web3, position_id, asset_index):
        log_print(f"{Fore.CYAN}🔐 Mencoba menutup posisi ID: {position_id}...{Style.RESET_ALL}")
        log_print(f"{Fore.CYAN}   Oracle: Mengambil proof untuk menutup...{Style.RESET_ALL}")
        proof_response = requests.get(self.ORACLE_PROOF_URL.format(asset_index), timeout=10)
        if proof_response.status_code != 200:
            log_print(f"{Fore.RED}   ❌ Oracle: Gagal mendapatkan proof, status {proof_response.status_code}{Style.RESET_ALL}")
            return False
        proof = proof_response.json().get('proof')
        if not proof:
            log_print(f"{Fore.RED}   ❌ Oracle: Proof tidak ditemukan dalam respons.{Style.RESET_ALL}")
            return False
        log_print(f"{Fore.GREEN}   ✅ Oracle: Proof berhasil didapatkan.{Style.RESET_ALL}")
        nonce = self.get_nonce(web3, self.wallet_address)
        tx = web3.eth.contract(address=self.BROKEX_ADDRESS, abi=self.BROKEX_ABI).functions.closePosition(
            position_id, bytes.fromhex(proof[2:])
        ).build_transaction({
            "chainId": self.CHAIN_ID, "gas": 2000000, "gasPrice": self.GAS_PRICE, "nonce": nonce
        })
        signed_tx = web3.eth.account.sign_transaction(tx, self.private_key)
        tx_hash = self.send_raw_tx(web3, signed_tx)
        if self.wait_tx_receipt_and_status(web3, tx_hash):
            log_print(f"{Fore.GREEN}✅ Posisi ID {position_id} berhasil ditutup! Tx: 0x{tx_hash.hex()}{Style.RESET_ALL}")
            return True
        else:
            log_print(f"{Fore.RED}❌ Gagal menutup posisi ID {position_id}. Tx: 0x{tx_hash.hex()}{Style.RESET_ALL}")
            return False

    @with_retry(max_retries=5, base_delay=2)
    def check_and_manage_open_positions(self, web3):
        log_print(f"{Fore.CYAN}🔍 Memeriksa PnL posisi terbuka...{Style.RESET_ALL}")
        brokex_contract = web3.eth.contract(address=self.BROKEX_ADDRESS, abi=self.BROKEX_ABI)
        try:
            open_ids = brokex_contract.functions.getUserOpenIds(self.wallet_address).call()
            if not open_ids:
                log_print(f"{Fore.YELLOW}ℹ️ Tidak ada posisi terbuka untuk diperiksa.{Style.RESET_ALL}")
                return
            for pos_id in open_ids:
                pos = brokex_contract.functions.getOpenById(pos_id).call()
                asset_index, is_long, leverage, open_price_wei, size_usd_wei = pos[2], pos[3], pos[4], pos[5], pos[6]
                asset_str_index = str(asset_index)
                if asset_str_index not in self.asset_data:
                    log_print(f"{Fore.YELLOW}⚠️ Tidak ditemukan data harga untuk aset ID {asset_index}, posisi dilewati.{Style.RESET_ALL}")
                    continue
                asset_name = self.asset_data[asset_str_index].get("name", f"Unknown (ID: {asset_index})")
                current_price = self.asset_data[asset_str_index].get("price")
                open_price = open_price_wei / 10**18
                size_usd = size_usd_wei / 10**self.USDT_DECIMALS
                if current_price == 0 or open_price == 0:
                    log_print(f"{Fore.YELLOW}⚠️ Data harga tidak valid untuk aset {asset_name}, posisi dilewati.{Style.RESET_ALL}")
                    continue
                pnl_usd = (current_price / open_price - 1) * size_usd if is_long else (open_price / current_price - 1) * size_usd
                margin = size_usd / leverage
                pnl_percentage = (pnl_usd / margin) * 100 if margin > 0 else 0
                pnl_color = Fore.GREEN if pnl_percentage >= 0 else Fore.RED
                log_print(f"{Fore.CYAN}  - Posisi {pos_id} [{asset_name}]: PnL saat ini: {pnl_color}{pnl_percentage:.2f}%{Style.RESET_ALL}")
                if pnl_percentage >= 100:
                    log_print(f"{Fore.GREEN}🚀 TAKE PROFIT: PnL untuk posisi {pos_id} mencapai {pnl_percentage:.2f}%! Menutup posisi...{Style.RESET_ALL}")
                    self.close_position(web3, pos_id, asset_index)
                    log_print(f"{Fore.YELLOW}⏳ Jeda 15 detik setelah menutup posisi...{Style.RESET_ALL}")
                    time.sleep(15)
                elif pnl_percentage <= -50:
                    log_print(f"{Fore.RED}🛡️ STOP LOSS: PnL untuk posisi {pos_id} mencapai {pnl_percentage:.2f}%! Menutup posisi...{Style.RESET_ALL}")
                    self.close_position(web3, pos_id, asset_index)
                    log_print(f"{Fore.YELLOW}⏳ Jeda 15 detik setelah menutup posisi...{Style.RESET_ALL}")
                    time.sleep(15)
        except Exception as e:
            log_print(f"{Fore.RED}❌ Error saat memeriksa PnL: {e}{Style.RESET_ALL}")

    @with_retry(max_retries=5, base_delay=2)
    def cancel_limit_order(self, web3, order_id):
        log_print(f"{Fore.CYAN}🗑️  Mencoba membatalkan limit order ID: {order_id}...{Style.RESET_ALL}")
        brokex_contract = web3.eth.contract(address=self.BROKEX_ADDRESS, abi=self.BROKEX_ABI)
        nonce = self.get_nonce(web3, self.wallet_address)
        tx = brokex_contract.functions.cancelOrder(order_id).build_transaction({
            "chainId": self.CHAIN_ID, "gas": 500000, "gasPrice": self.GAS_PRICE, "nonce": nonce
        })
        signed_tx = web3.eth.account.sign_transaction(tx, self.private_key)
        tx_hash = self.send_raw_tx(web3, signed_tx)
        if self.wait_tx_receipt_and_status(web3, tx_hash):
            log_print(f"{Fore.GREEN}✅ Order ID {order_id} berhasil dibatalkan! Tx: 0x{tx_hash.hex()}{Style.RESET_ALL}")
            return True
        else:
            log_print(f"{Fore.RED}❌ Gagal membatalkan order ID {order_id}. Tx: 0x{tx_hash.hex()}{Style.RESET_ALL}")
            return False

    @with_retry(max_retries=5, base_delay=2)
    def check_and_cancel_stale_orders(self, web3):
        log_print(f"{Fore.CYAN}🔍 Memeriksa limit order yang tertunda...{Style.RESET_ALL}")
        brokex_contract = web3.eth.contract(address=self.BROKEX_ADDRESS, abi=self.BROKEX_ABI)
        try:
            order_ids = brokex_contract.functions.getUserOrderIds(self.wallet_address).call()
            if not order_ids:
                log_print(f"{Fore.YELLOW}ℹ️ Tidak ada limit order yang tertunda.{Style.RESET_ALL}")
                return
            for order_id in order_ids:
                order = brokex_contract.functions.getOrderById(order_id).call()
                asset_index, target_price_wei = order[2], order[5]
                asset_str_index = str(asset_index)
                if asset_str_index not in self.asset_data:
                    log_print(f"{Fore.YELLOW}⚠️ Tidak ditemukan data harga untuk aset ID {asset_index} pada order {order_id}, dilewati.{Style.RESET_ALL}")
                    continue
                current_price = self.asset_data[asset_str_index].get("price")
                target_price = target_price_wei / 10**18
                if current_price == 0 or target_price == 0:
                    continue
                deviation_pct = (abs(current_price - target_price) / target_price) * 100
                log_print(f"{Fore.CYAN}  - Order {order_id}: Deviasi harga saat ini adalah {deviation_pct:.2f}% dari target.{Style.RESET_ALL}")
                if deviation_pct > self.STALE_ORDER_DEVIATION_PCT:
                    log_print(f"{Fore.YELLOW}   - Deviasi {deviation_pct:.2f}% melebihi ambang batas ({self.STALE_ORDER_DEVIATION_PCT}%). Membatalkan order...{Style.RESET_ALL}")
                    self.cancel_limit_order(web3, order_id)
                    log_print(f"{Fore.YELLOW}⏳ Jeda 15 detik setelah membatalkan order...{Style.RESET_ALL}")
                    time.sleep(15)
        except Exception as e:
            log_print(f"{Fore.RED}❌ Error saat memeriksa limit order: {e}{Style.RESET_ALL}")

    @with_retry(max_retries=5, base_delay=2)
    def check_my_liquidity(self, web3):
        liquidity_contract = web3.eth.contract(address=self.LIQUIDITY_CONTRACT_ADDRESS, abi=self.LIQUIDITY_ABI)
        lp_balance_wei = liquidity_contract.functions.balanceOf(self.wallet_address).call()
        if lp_balance_wei > 0:
            lp_price_wei = liquidity_contract.functions.getLpPrice().call()
            total_value_usdt = (lp_balance_wei / 10**18) * (lp_price_wei / 10**6)
            log_print(f"{Fore.CYAN}💧 Saldo Likuiditas: {lp_balance_wei/10**18:.4f} LP (~${total_value_usdt:.2f} USDT){Style.RESET_ALL}")
        return lp_balance_wei

    @with_retry(max_retries=5, base_delay=2)
    def check_and_join_competition(self, web3):
        response = self.supabase.table('traders').select('address').eq('address', self.wallet_address.lower()).execute()
        if response.data:
            log_print(f"{Fore.CYAN}🏆 Status Kompetisi: Sudah bergabung.{Style.RESET_ALL}")
            return True
        log_print(f"{Fore.CYAN}🏆 Status Kompetisi: Belum bergabung. Mencoba untuk join...{Style.RESET_ALL}")
        response = self.supabase.table('traders').insert({'address': self.wallet_address.lower(), 'pnl': 0}).execute()
        if (hasattr(response, 'data') and response.data and response.data[0]['address'] == self.wallet_address.lower()) or (hasattr(response, 'error') and response.error is None):
            log_print(f"{Fore.GREEN}✅ Sukses bergabung dalam kompetisi!{Style.RESET_ALL}")
            return True
        else:
            error_message = response.error.message if hasattr(response, 'error') and response.error else "Unknown error"
            log_print(f"{Fore.RED}❌ Gagal join kompetisi: {error_message}{Style.RESET_ALL}")
            return False

    @with_retry(max_retries=3, base_delay=2)
    def check_competition_rank(self):
        log_print(f"{Fore.CYAN}📊 Memeriksa peringkat dan poin kompetisi...{Style.RESET_ALL}")
        try:
            response = self.supabase.table('traders').select('address, pnl').order('pnl', desc=True).execute()
            if not response.data:
                log_print(f"{Fore.YELLOW}ℹ️ Leaderboard kompetisi masih kosong.{Style.RESET_ALL}")
                return
            leaderboard = response.data
            my_rank = -1
            my_pnl = 0
            for i, trader in enumerate(leaderboard):
                if trader['address'].lower() == self.wallet_address.lower():
                    my_rank = i + 1
                    my_pnl = trader['pnl']
                    break
            if my_rank != -1:
                log_print(f"{Fore.GREEN}🏆 Peringkat Anda: #{my_rank} | Poin (PnL): {my_pnl:.4f}{Style.RESET_ALL}")
            else:
                log_print(f"{Fore.YELLOW}ℹ️ Anda belum masuk dalam peringkat kompetisi.{Style.RESET_ALL}")
        except Exception as e:
            log_print(f"{Fore.RED}❌ Error saat memeriksa peringkat kompetisi: {e}{Style.RESET_ALL}")

    def read_private_keys(self, file_path="pk.txt"):
        try:
            with open(file_path, 'r') as f:
                keys = [line.strip() for line in f if line.strip()]
                if not keys:
                    log_print(f"{Fore.RED}❌ File 'pk.txt' kosong atau tidak berisi kunci yang valid.{Style.RESET_ALL}")
                    return []
                log_print(f"{Fore.GREEN}🔑 Ditemukan {len(keys)} kunci privat di 'pk.txt'.{Style.RESET_ALL}")
                return keys
        except FileNotFoundError:
            log_print(f"{Fore.RED}❌ Error: File 'pk.txt' tidak ditemukan. Mohon buat file tersebut dan isi dengan kunci privat Anda.{Style.RESET_ALL}")
            return []

    def get_address_from_pk(self, private_key):
        account = Account.from_key(private_key)
        return account.address

    def run(self):
        while True:
            self.clear_terminal()
            self.display_banner()
            w3 = None
            for attempt in range(1, self.MAX_RETRIES + 1):
                try:
                    w3 = self.connect_web3()
                    log_print(f"{Fore.GREEN}✅ Connected to chain ID: {self.CHAIN_ID}{Style.RESET_ALL}")
                    break
                except Exception as e:
                    log_print(f"{Fore.RED}❌ Failed to connect to RPC on attempt {attempt}/{self.MAX_RETRIES}: {e}{Style.RESET_ALL}")
                    if attempt < self.MAX_RETRIES:
                        delay = self.RETRY_BASE_DELAY * (2 ** (attempt - 1))
                        log_print(f"{Fore.YELLOW}🔄 Retrying in {delay} seconds...{Style.RESET_ALL}")
                        time.sleep(delay)
                    else:
                        log_print(f"{Fore.RED}❌ Max retries reached. Aborting run.{Style.RESET_ALL}")
                        return

            private_keys = self.read_private_keys()
            if not private_keys:
                log_print(f"{Fore.YELLOW}Program akan berhenti karena tidak ada kunci privat untuk diproses.{Style.RESET_ALL}")
                break

            for index, pk in enumerate(private_keys):
                log_print(f"{Fore.MAGENTA}======================================================{Style.RESET_ALL}")
                log_print(f"{Fore.MAGENTA}🚀 Memulai proses untuk Akun {index + 1}/{len(private_keys)}{Style.RESET_ALL}")
                log_print(f"{Fore.MAGENTA}======================================================{Style.RESET_ALL}")

                try:
                    self.wallet_address = self.get_address_from_pk(pk)
                    self.private_key = pk
                    self.account = Account.from_key(pk)
                    log_print(f"{Fore.WHITE}═╣ Processing Wallet: {self.wallet_address} ╠═{Style.RESET_ALL}")

                    if not self.update_asset_data_from_websocket():
                        log_print(f"{Fore.RED}❌ Gagal mengambil data aset. Melewati Brokex Ecosystem.{Style.RESET_ALL}")
                        continue

                    if self.check_and_join_competition(w3):
                        self.check_competition_rank()

                    balance = self.get_usdt_balance(w3)
                    log_print(f"{Fore.YELLOW}💰 Saldo USDT: {balance:.4f}{Style.RESET_ALL}")

                    if balance < self.MIN_USDT_BALANCE:
                        log_print(f"{Fore.YELLOW}⚠️ Saldo USDT ({balance:.2f}) tidak cukup. Mencoba claim...{Style.RESET_ALL}")
                        if not self.claim_usdt(w3):
                            log_print(f"{Fore.RED}❌ Gagal claim USDT. Melewati Brokex Ecosystem.{Style.RESET_ALL}")
                            continue
                        balance = self.get_usdt_balance(w3)
                        log_print(f"{Fore.YELLOW}💰 Saldo USDT setelah claim: {balance:.4f}{Style.RESET_ALL}")
                        if balance < self.MIN_USDT_BALANCE:
                            log_print(f"{Fore.RED}❌ Saldo masih tidak cukup setelah claim. Melewati Brokex Ecosystem.{Style.RESET_ALL}")
                            continue

                    log_print(f"{Fore.CYAN}▶️ Memulai pengecekan awal...{Style.RESET_ALL}")
                    self.check_and_manage_open_positions(w3)
                    time.sleep(5)
                    self.check_and_cancel_stale_orders(w3)
                    time.sleep(5)
                    self.check_my_liquidity(w3)

                    log_print(f"{Fore.CYAN}▶️ Memulai urutan aksi utama...{Style.RESET_ALL}")
                    core_actions = [
                        ("Buka Posisi Market", self.open_market_position),
                        ("Tempatkan Limit Order", self.place_limit_order),
                        ("Buka Posisi Market", self.open_market_position),
                        ("Tempatkan Limit Order", self.place_limit_order),
                        ("Buka Posisi Market", self.open_market_position),
                        ("Tempatkan Limit Order", self.place_limit_order),
                        ("Buka Posisi Market", self.open_market_position),
                        ("Tempatkan Limit Order", self.place_limit_order),
                        ("Buka Posisi Market", self.open_market_position),
                        ("Tempatkan Limit Order", self.place_limit_order),
                        ("Menambah Likuiditas", self.add_liquidity)
                    ]
                    random.shuffle(core_actions)
                    actions = core_actions + [("Menarik Likuiditas", self.withdraw_liquidity)]

                    for i, (name, func) in enumerate(actions, 1):
                        log_print(f"{Fore.CYAN}--- Aksi {i}/{len(actions)}: {name} ---{Style.RESET_ALL}")
                        if func.__name__ == 'withdraw_liquidity':
                            if self.check_my_liquidity(w3) == 0:
                                log_print(f"{Fore.YELLOW}ℹ️ Tidak ada likuiditas untuk ditarik, aksi dilewati.{Style.RESET_ALL}")
                                continue
                        try:
                            if not func(w3):
                                log_print(f"{Fore.YELLOW}⚠️ Aksi '{name}' tidak berhasil, lanjut ke aksi berikutnya.{Style.RESET_ALL}")
                        except Exception as e:
                            log_print(f"{Fore.RED}⚠️ Error pada aksi {name}: {e}{Style.RESET_ALL}")
                        if i < len(actions):
                            delay = random.randint(15, 25)
                            log_print(f"{Fore.YELLOW}⏳ Jeda {delay} detik...{Style.RESET_ALL}")
                            time.sleep(delay)

                    log_print(f"{Fore.GREEN}✅ Selesai memproses Brokex Ecosystem untuk wallet ini.\n{Style.RESET_ALL}")

                except Exception as e:
                    log_print(f"{Fore.RED}❌ Terjadi kesalahan fatal pada Akun {index + 1}: {e}{Style.RESET_ALL}")
                    log_print(f"{Fore.YELLOW}Lanjut ke akun berikutnya...{Style.RESET_ALL}")

                if index < len(private_keys) - 1:
                    inter_account_delay = random.randint(30, 60)
                    log_print(f"{Fore.BLUE}--- Jeda {inter_account_delay} detik sebelum beralih ke akun berikutnya ---{Style.RESET_ALL}")
                    time.sleep(inter_account_delay)

            w3 = None  

            log_print(f"{Fore.CYAN}======================================================{Style.RESET_ALL}")
            log_print(f"{Fore.CYAN}✅ Semua akun telah selesai diproses.{Style.RESET_ALL}")
            log_print(f"{Fore.CYAN}⏰ Akan tidur selama 24 jam sebelum memulai lagi...{Style.RESET_ALL}")
            log_print(f"{Fore.CYAN}======================================================{Style.RESET_ALL}")
            time.sleep(24 * 60 * 60)

if __name__ == "__main__":
    bot = BrokexBot()  
    bot.run()

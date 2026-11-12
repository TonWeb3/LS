import time
from web3 import AsyncWeb3
from .config import settings
from typing import Dict, Optional

AGGREGATOR_ABI = [
    {"inputs": [], "name": "latestRoundData", "outputs": [{"internalType": "uint80", "name": "roundId", "type": "uint80"}, {"internalType": "int256", "name": "answer", "type": "int256"}, {"internalType": "uint256", "name": "startedAt", "type": "uint256"}, {"internalType": "uint256", "name": "updatedAt", "type": "uint256"}, {"internalType": "uint80", "name": "answeredInRound", "type": "uint80"}], "stateMutability": "view", "type": "function"},
    {"inputs": [], "name": "decimals", "outputs": [{"internalType": "uint8", "name": "", "type": "uint8"}], "stateMutability": "view", "type": "function"}
]

class ChainlinkFetcher:
    def __init__(self):
        self.cached_decimals = None
        self.cached_result = {"price": None, "updatedAt": None, "source": "chainlink"}
        self.cached_fetched_at_ms = 0
        self.min_fetch_interval_ms = 2000
        self.preferred_rpc_url = None

    def get_ordered_rpcs(self):
        alchemy = [settings.alchemy_rpc_url()] if settings.alchemy_rpc_url() else []
        from_list = settings.POLYGON_RPC_URLS
        single = [settings.POLYGON_RPC_URL] if settings.POLYGON_RPC_URL else []
        defaults = [
            "https://rpc.ankr.com/polygon",
            "https://1rpc.io/matic",
            "https://polygon.llamarpc.com"
        ]
        all_rpcs = list(dict.fromkeys(alchemy + from_list + single + defaults))
        all_rpcs = [r for r in all_rpcs if "polygon.drpc.org" not in r]
        if self.preferred_rpc_url and self.preferred_rpc_url in all_rpcs:
            all_rpcs.remove(self.preferred_rpc_url)
            return [self.preferred_rpc_url] + all_rpcs
        return all_rpcs

    async def fetch_chainlink_btc_usd(self) -> Dict:
        now = time.time() * 1000
        if self.cached_fetched_at_ms and now - self.cached_fetched_at_ms < self.min_fetch_interval_ms:
            return self.cached_result

        rpcs = self.get_ordered_rpcs()
        if not rpcs:
            return {"price": None, "updatedAt": None, "source": "missing_config"}

        aggregator_address = settings.CHAINLINK_BTC_USD_AGGREGATOR
        for rpc in rpcs:
            try:
                w3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(rpc, request_kwargs={'timeout': 3.0}))
                contract = w3.eth.contract(address=AsyncWeb3.to_checksum_address(aggregator_address), abi=AGGREGATOR_ABI)

                if self.cached_decimals is None:
                    self.cached_decimals = await contract.functions.decimals().call()

                round_data = await contract.functions.latestRoundData().call()
                answer = round_data[1]
                updated_at = round_data[3]

                price = answer / (10 ** self.cached_decimals)
                self.cached_result = {
                    "price": price,
                    "updatedAt": updated_at * 1000,
                    "source": "chainlink"
                }
                self.cached_fetched_at_ms = now
                self.preferred_rpc_url = rpc
                return self.cached_result
            except Exception as e:
                self.cached_decimals = None
                continue

        return self.cached_result

chainlink_fetcher = ChainlinkFetcher()

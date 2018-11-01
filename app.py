import json
import aiohttp
import asyncio
import zmq.asyncio
import sys
import argparse
import ipdb
import logging
import time
import websockets
from zmapi import fix
from time import time, gmtime
from datetime import datetime
from asyncio import ensure_future as create_task
from zmapi.utils import delayed
from zmapi.controller import RESTConnectorCTL, ConnectorCTL
from zmapi.logging import setup_root_logger, disable_logger
from urllib.parse import urlencode, urlparse
from pprint import pprint, pformat
from base64 import b64decode
from zlib import decompress, MAX_WBITS


################################## CONSTANTS ##################################


CAPABILITIES = sorted([
    fix.ZMCap.UnsyncMDSnapshot,
    fix.ZMCap.GetTickerFields,
    fix.ZMCap.MDSubscribe,
    fix.ZMCap.ListDirectory,
    fix.ZMCap.PubOrderBookIncremental,
])

TICKER_FIELDS = []


MODULE_NAME = "bittrex-md"
ENDPOINT_NAME = "bittrex"


################################# GLOBAL STATE ################################


class GlobalState:
    pass
g = GlobalState()
g.loop = asyncio.get_event_loop()
g.ctx = zmq.asyncio.Context()
g.startup_time = datetime.utcnow()
g.seq_no = 0

g.startup_event = asyncio.Event()

L = logging.root


###############################################################################

class MyController(RESTConnectorCTL):


    def __init__(self, sock_dn, ctx):
        super().__init__(sock_dn, ctx)
        # max 600 requests in any rolling 10 minute period
        self._add_throttler(r".*", 600, 10 * 60)


    def _process_fetched_data(self, data, url):
        return json.loads(data.decode())


    async def ZMGetStatus(self, ident, msg_raw, msg):
        res = {}
        res["Header"] = header = {}
        header["MsgType"] = fix.MsgType.ZMGetStatusResponse
        d = {}
        d["module_name"] = MODULE_NAME
        d["enpoint_name"] = ENDPOINT_NAME
        d["session_id"] = g.session_id
        res["Body"] = [d]
        return res


    async def ZMListDirectory(self, ident, msg_raw, msg):
        url = "https://bittrex.com/api/v1.1/public/getcurrencies"
        data = await self._http_get_cached(url, 86400)
        data = sorted(data["result"], key=lambda x: x["Currency"])
        res = {}
        res["Header"] = header = {}
        header["MsgType"] = fix.MsgType.ZMListDirectoryResponse
        res["Body"] = body = {}
        group = []
        for x in data:
            d = {}
            d["ZMNodeName"] = x["Currency"]
            d["ZMInstrumentID"] = x["Currency"]
            d["Text"] = x["CurrencyLong"]
            group.append(d)
        body["ZMNoDirEntries"] = group
        return res


    async def SecurityListRequest(self, ident, msg_raw, msg):
        body = msg["Body"]
        ins_id = body.get("ZMInstrumentID")
        res = {}
        res["Header"] = header = {}
        header["MsgType"] = fix.MsgType.SecurityList
        res["Body"] = body = {}
        body["NoRelatedSym"] = group = []
        url = "https://bittrex.com/api/v1.1/public/getmarkets"
        data = await self._http_get_cached(url, 86400)
        data = data["result"]
        if ins_id:
            print(ins_id)
            data = [x for x in data if x["MarketName"] == 
                    ins_id]
            assert len(data) == 1, len(data)
        for t in data:
            d = {}
            ds = "{} / {}".format(t["MarketCurrencyLong"], t["BaseCurrencyLong"])
            d["SecurityDesc"] = ds
            d["MinPriceIncrement"] = 0.0001
            d["ZMInstrumentID"] = t["MarketName"]
            group.append(d)
        return res


    async def ZMGetInstrumentFields(self, ident, msg_raw, msg):
        res = {}
        res["Header"] = header = {}
        header["MsgType"] = fix.MsgType.ZMGetInstrumentFieldsResponse
        res["Body"] = TICKER_FIELDS
        print(fix.MsgType.ZMGetInstrumentFieldsResponse)
        return res


    async def _get_snapshot(self, ins_id, sub_def):
        res = {}

        res["ZMNoMDFullEntries"] = group = []
        ticks = sub_def["NoMDEntryTypes"]
        market_depth = sub_def.get("MarketDepth", 0)
        
        get_ob = True
        if market_depth == 1 and "*" not in ticks \
                and "0" not in ticks and "1" not in ticks:
            get_ob = False

        get_quotes = False
        if "*" in ticks or \
                "7" in ticks or \
                "8" in ticks or \
                "2" in ticks or \
                "4" in ticks or \
                "B" in ticks or \
                "9" in ticks:
            get_quotes = True

        async with aiohttp.ClientSession() as session:

            res["LastUpdateTime"] = 0

            if get_quotes:
                url = "https://bittrex.com/api/v1.1/public/getmarketsummary?market={}"
                url = url.format(ins_id)
                data = await self._http_get(url, session=session)
                pprint(data)
                if "*" in ticks or "7" in ticks:
                    d = {}
                    d["MDEntryType"] = "7"
                    d["MDEntryPx"] = float(data["result"][0]["High"])
                    group.append(d)
                if "*" in ticks or "8" in ticks:
                    d = {}
                    d["MDEntryType"] = "8"
                    d["MDEntryPx"] = float(data["result"][0]["Low"])
                    group.append(d)
                if "*" in ticks or "B" in ticks:
                    d = {}
                    d["MDEntryType"] = "B"
                    d["MDEntryPx"] = float(data["result"][0]["Volume"])
                    group.append(d)
                ts = datetime.strptime(data["result"][0]["TimeStamp"], 
                        "%Y-%m-%dT%H:%M:%S.%f")
                ts = int(ts.timestamp() * 1000) * 100000
                res["LastUpdateTime"] = ts

            if get_ob:
                url = "https://bittrex.com/api/v1.1/public/getorderbook?market={}&type=both"
                url = url.format(ins_id)
                data = await self._http_get(url, session=session)
                if market_depth == 0:
                    market_depth = sys.maxsize
                buys = data["result"]["buy"]
                buys = [(float(x["Rate"]), float(x["Quantity"])) for x in buys]
                buys = sorted(buys)[::-1]
                sells = data["result"]["sell"]
                sells = [(float(x["Rate"]), float(x["Quantity"])) for x in sells]
                sells = sorted(sells)
                for i, (price, size) in enumerate(buys):
                    if i == market_depth:
                        break
                    d = {}
                    d["MDEntryType"] = "0"
                    d["MDEntryPx"] = price
                    d["MDEntrySize"] = size
                    d["MDPriceLevel"] = i + 1
                    group.append(d)
                for i, (price, size) in enumerate(sells):
                    if i == market_depth:
                        break
                    d = {}
                    d["MDEntryType"] = "1"
                    d["MDEntryPx"] = price
                    d["MDEntrySize"] = size
                    d["MDPriceLevel"] = i + 1
                    group.append(d)
            return res


    async def MarketDataRequest(self, ident, msg_raw, msg):
        body = msg["Body"]
        srt = body["SubscriptionRequestType"]
        if srt not in "012":
            raise MarketDataRequestRejectException(
                    fix.UnsupportedSubscriptionRequestType, srt)
        ins_id = body["ZMInstrumentID"]
        ticks = body.get("NoMDEntryTypes")
        res = {}
        res["Header"] = header = {}
        header["MsgType"] = fix.MsgType.ZMMarketDataRequestResponse
        res["Body"] = body = {}
        if srt == "0":
            snap = await self._get_snapshot(ins_id, msg["Body"])
            print(snap)
            body["ZMSnapshots"] = [snap]
        elif srt == "1":
            await g.pub.subscribe(ins_id)
        return res


    async def ZMListCapabilities(self, ident, msg_raw, msg):
        res = {}
        res["Header"] = header = {}
        header["MsgType"] = fix.MsgType.ZMListCapabilitiesResponse
        res["Body"] = body = {}
        body["ZMNoCaps"] = CAPABILITIES
        return res


###############################################################################


class Publisher:


    def __init__(self, sock):
        self._sock = sock
        self._protocol_version = "1.5"
        self._connection = '[{"name": "c2"}]'
        self._url = "https://beta.bittrex.com/signalr"
        self._endpoint = ""
        self._transport = ""
        self._token = ""
        self._ws = None
        self._ut_map = {
            0: "0",
            1: "2",
            2: "1"
        }
        self._ot_map = {
            "BUY": "1",
            "SELL": "2",
        }
        self._cache = {}


    def _process_message(self, msg):
        try:
            deflated_msg = decompress(b64decode(msg, validate=True), -MAX_WBITS)
        except SyntaxError:
            deflated_msg = decompress(b64decode(msg, validate=True))
        return json.loads(deflated_msg.decode())

    
    async def _data_received(self, msg):
        if not msg:
            return
        sub_msgs = msg.get("M", [])
        if "R" in msg and type(msg["R"]) is not bool:
            decoded_msg = self._process_message(msg["R"])
            await self._handle_order_book_snapshot(decoded_msg)
        else:
            for sub_msg in sub_msgs:
                msg_type = sub_msg["M"]
                if msg_type == "uS":
                    for payload in sub_msg["A"]:
                        decoded_msg = self._process_message(payload)
                        await self._handle_summary_deltas(decoded_msg)
                elif msg_type == "uE":
                    for payload in sub_msg["A"]:
                        decoded_msg = self._process_message(payload)
                        await self._handle_order_book_update(decoded_msg)
                else:
                    L.debug(pformat(sub_msg))
            if not sub_msgs:
                L.debug(pformat(msg))
            

    def _create_url(self):
        query = urlencode({
            "connectionData": self._connection,
            "clientProtocol": self._protocol_version,
            "transport": self._transport,
            "connectionToken": self._token,
        })
        endpoint = self._endpoint
        url = "{}/{}?{}".format(self._url, self._endpoint, query)
        url = f"{url}"
        return url


    def _create_msg(self, method, args=None):
        msg = {
            "H": "c2",
            "M": method,
            "A": args if args else [],
            "I" : 0,
        }
        return json.dumps(msg)


    async def _fetch(self, session, url):
        async with session.get(url) as response:
            return await response.json()


    async def _negotiate_ws(self):
        print("sending request..")
        self._endpoint = "negotiate"
        url = self._create_url()

        async with aiohttp.ClientSession() as session:
            return await self._fetch(session, url)


    async def _connect(self):
        res = await self._negotiate_ws()
        scheme = "wss" if urlparse(self._url).scheme == "https" else "ws"
        self._url = f"{scheme}://beta.bittrex.com/signalr"
        self._transport = "webSockets"
        self._token = res["ConnectionToken"]
        self._endpoint = "connect"
        url = self._create_url()

        self._ws = await websockets.connect(url)
        L.debug("Connecting to ws ...")
        greeting = await self._ws.recv()
        L.debug("Connected ...")
        msg = self._create_msg("SubscribeToSummaryDeltas")
        await self._ws.send(msg)


    async def _restart(self):
        if self._ws:
            L.debug("Closing old ws ...")
            await self._ws.close()
        while True:
            try:
                await self._connect()
            except Exception as err:
                L.error(f"Error connectng to websocket: {err}")
            break
        g.startup_event.set()
        await self._recv_forever()



    async def subscribe(self, ins_id):
        L.debug(f"subscribing to {ins_id}")
        qes = self._create_msg("queryExchangeState", [ins_id])
        sted = self._create_msg("SubscribeToExchangeDeltas", [ins_id])
        await self._ws.send(qes)
        await self._ws.send(sted)


    async def _handle_summary_deltas(self, data):
        seq_no = g.seq_no
        g.seq_no += 1
        res = {}
        res["Header"] = header = {}
        header["MsgSeqNum"] = seq_no
        header["ZMSendingTime"] = int(datetime.utcnow().timestamp() * 1e9)
        res["Body"] = body = {}
        body["ZMNoMDIncEntries"] = group = []

        for d in data["D"]:

            tid = g.ctl.insid_to_tid.get(d["M"])
            c = {}
            c["7"] = d["H"]
            c["8"] = d["L"]
            c["B"] = d["V"]
            self._cache[d["M"]] = c
            if tid is None:
                continue
            
            c = {}
            c["MDEntryType"] = "7"
            c["MDEntryPx"] = d["H"]
            c["TransactTime"] = d["T"] * 1000000
            c["ZMTickerID"] = tid
            group.append(c)

            c = {}
            c["MDEntryType"] = "8"
            c["MDEntryPx"] = d["L"]
            c["TransactTime"] = d["T"] * 1000000
            c["ZMTickerID"] = tid
            group.append(c)
            
            c = {}
            c["MDEntryType"] = "B"
            c["MDEntryPx"] = d["V"]
            c["TransactTime"] = d["T"] * 1000000
            c["ZMTickerID"] = tid
            group.append(c)

        res = " " + json.dumps(res)
        res = [b"X", res.encode()]
        await self._sock.send_multipart(res)

    
    async def _handle_order_book_snapshot(self, data):
        tid = g.ctl.insid_to_tid[data["M"]]
        seq_no = g.seq_no
        g.seq_no += 1
        res = {}
        res["Header"] = header = {}
        header["MsgSeqNum"] = seq_no
        header["ZMSendingTime"] = int(datetime.utcnow().timestamp() * 1e9)
        res["Body"] = body = {}
        body["ZMNoMDFullEntries"] = group = []

        for i, d in enumerate(data.get("S", [])):
            c = {}
            c["MDEntryType"] = "1"
            c["MDEntrySize"] = float(d["Q"])
            c["MDEntryPx"] = float(d["R"])
            c["MDPriceLevel"] = i
            group.append(c)

        for i, d in enumerate(data.get("Z", [])):
            c = {}
            c["MDEntryType"] = "1"
            c["MDEntrySize"] = d["Q"]
            c["MDEntryPx"] = d["R"]
            c["MDPriceLevel"] = i
            group.append(c)


        res["ZMTickerID"] = tid
        res = " " + json.dumps(res)
        res = [b"W", res.encode()]
        await self._sock.send_multipart(res)


    async def _handle_order_book_update(self, data):
        tid = g.ctl.insid_to_tid[data["M"]]
        seq_no = g.seq_no
        g.seq_no += 1
        res = {}
        res["Header"] = header = {}
        header["MsgSeqNum"] = seq_no
        header["ZMSendingTime"] = int(datetime.utcnow().timestamp() * 1e9)
        res["Body"] = body = {}
        body["ZMNoMDIncEntries"] = group = []

        for d in data.get("Z", []):
            c = {}
            c["MDUpdateAction"] = self._ut_map[d["TY"]]
            c["MDEntryType"] = "0"
            c["MDEntryPx"] = d["R"]
            c["MDEntrySize"] = d["Q"]
            c["ZMTickerID"] = tid
            group.append(c)

        for d in data.get("S", []):
            c = {}
            c["MDUpdateAction"] = self._ut_map[d["TY"]]
            c["MDEntryType"] = "1"
            c["MDEntryPx"] = d["R"]
            c["MDEntrySize"] = d["Q"]
            c["ZMTickerID"] = tid
            group.append(c)
        
        for d in data.get("f", []):
            c = {}
            agg = self._ot_map.get(d["OT"])
            if agg:
                c["AggressorSide"] = agg 
            c["MDEntryType"] = "2"
            c["MDEntryPx"] = d["R"]
            c["MDEntrySize"] = d["Q"]
            c["TransactTime"] = d["T"] * 1000000
            group.append(c)


        res = " " + json.dumps(res)
        res = [b"X", res.encode()]
        await self._sock.send_multipart(res)


    async def _recv_forever(self):
        L.debug("running recv loop ...")
        while True:
            msg = await self._ws.recv() 
            await self._data_received(json.loads(msg))


    async def run(self):
        await self._restart()



###############################################################################


def parse_args():
    parser = argparse.ArgumentParser(description="bittrex md connector")
    parser.add_argument("ctl_addr", help="address to bind to for ctl socket")
    parser.add_argument("pub_addr", help="address to bind to for pub socket")
    parser.add_argument("--log-level", default="INFO", help="logging level")
    parser.add_argument("--log-websockets", action="store_true",
                        help="add websockets logger")
    args = parser.parse_args()
    try:
        args.log_level = int(args.log_level)
    except ValueError:
        pass
    return args


def setup_logging(args):
    if not args.log_websockets:
        disable_logger("websockets")
    setup_root_logger(args.log_level)


def init_zmq_sockets(args):
    g.sock_ctl = g.ctx.socket(zmq.ROUTER)
    g.sock_ctl.bind(args.ctl_addr)
    g.sock_pub = g.ctx.socket(zmq.PUB)
    g.sock_pub.bind(args.pub_addr)


def main():
    args = parse_args()
    init_zmq_sockets(args)
    setup_logging(args)
    g.ctl = MyController(g.sock_ctl, g.ctx)
    g.pub = Publisher(g.sock_pub)
    tasks = [
        delayed(g.ctl.run, g.startup_event),
        g.pub.run(),
    ]
    g.loop.run_until_complete(asyncio.gather(*tasks))
    g.ctx.destroy()

if __name__ == "__main__":
    main()


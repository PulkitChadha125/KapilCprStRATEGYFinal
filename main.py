import pandas as pd
import datetime  # full module
import polars as pl
import polars_talib as plta
import json
# from datetime import datetime, timedelta
import time
import traceback
import sys
from FyresIntegration import *
# Ensure the SDK path is included for import
sys.path.append('.')
# Now import the SDK
from xtspythonclientapisdk.Connect import XTSConnect
Future_instrument_id_list=[]
Equity_instrument_id_list=[]
result_dict = {}
xts_marketdata = None
xt=None
credentials_dict_fyers=None

FyerSymbolList=[]

def get_api_credentials_Fyers():
    credentials_dict_fyers = {}
    try:
        df = pd.read_csv('FyersCredentials.csv')
        for index, row in df.iterrows():
            title = row['Title']
            value = row['Value']
            credentials_dict_fyers[title] = value
    except pd.errors.EmptyDataError:
        print("The CSV FyersCredentials.csv file is empty or has no data.")
    except FileNotFoundError:
        print("The CSV FyersCredentials.csv file was not found.")
    except Exception as e:
        print("An error occurred while reading the CSV FyersCredentials.csv file:", str(e))
    return credentials_dict_fyers

#get equity symbols
def get_equity_symbols():
    url = "https://public.fyers.in/sym_details/NSE_CM_sym_master.json"
    response = requests.get(url)
    data = response.json()
    df = pd.DataFrame.from_dict(data, orient='index')
    return df





def delete_file_contents(file_name):
    try:
        # Open the file in write mode, which truncates it (deletes contents)
        with open(file_name, 'w') as file:
            file.truncate(0)
        print(f"Contents of {file_name} have been deleted.")
    except FileNotFoundError:
        print(f"File {file_name} not found.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

def interactivelogin():
    from selenium import webdriver
    from selenium.webdriver.common.keys import Keys
    import time
    from selenium.webdriver.common.by import By
    import json
    import pyotp
    from xtspythonclientapisdk.Connect import XTSConnect

    global xt
    # URL="http://122.184.68.130:3008//interactive/thirdparty?appKey=93e98b500aaeb837ead698&returnURL=http://122.184.68.130:3008//interactive/testapi#!/logIn"
    URL="https://strade.shareindia.com//interactive/thirdparty?appKey=a743d238d50923fc2dd127&returnURL=https://strade.shareindia.com/interactive/testapi"
    driver = webdriver.Chrome()
    driver.get(URL)
    time.sleep(2)
    search = driver.find_element(by=By.NAME, value="userID")
    search.send_keys("66BP01")
    search.send_keys(Keys.RETURN)
    time.sleep(1)
    driver.find_element(by=By.ID, value="confirmimage").click()
    search = driver.find_element(by=By.ID, value="login_password_field")
    search.send_keys("Rohit@987")
    driver.find_element("xpath", "/html/body/ui-view/div[1]/div/div/div/div[2]/form/div[4]/div[2]/button").click()
    time.sleep(2)
    totpField = driver.find_element(by=By.NAME, value="efirstPin")
    totp = pyotp.TOTP('OZYCSOBXOIQWSLBJKBYVKNZBNUSX2MD2GRRTKOJEJUYXO5KANNDQ')
    TOTP = totp.now()
    time.sleep(2)
    totpField.send_keys(TOTP)
    driver.find_element(by=By.CLASS_NAME, value="PlaceButton").click()
    time.sleep(3)
    json_list = []
    json_list = driver.find_element(By.TAG_NAME,"pre").get_attribute('innerHTML')
    aDict = json.loads(json_list)
    sDict = json.loads(aDict['session'])
    accessToken=sDict['accessToken']
    print("accessToken:",accessToken)

    driver.close()

    xt = XTSConnect(apiKey="a743d238d50923fc2dd127",secretKey="Yvak100@qS",
                    source="WEBAPI",accessToken=accessToken)
    try:
        response = xt.interactive_login()
        # print("response: ", response)
        set_marketDataToken = response['result']['token']
        set_muserID = response['result']['userID']
        print("Login: ", response)
        print(f"UserId: {set_muserID}")
        print(f"Token: {set_marketDataToken}")
        
    except Exception as e:
        print("Error during interactive_login:", e)
        import traceback
        traceback.print_exc()



 

#  INTERAACTIVE LOGIN ABOVE

def place_order(nfo_ins_id,order_quantity,order_side,price,unique_key):
    val=None
    if order_side == "BUY":
        val=xt.TRANSACTION_TYPE_BUY
    elif order_side == "SELL":
        val=xt.TRANSACTION_TYPE_SELL

        
    response=xt.place_order (
        exchangeSegment=xt.EXCHANGE_NSEFO,
        exchangeInstrumentID=nfo_ins_id,
        productType=xt.PRODUCT_MIS,
        orderType=xt.ORDER_TYPE_LIMIT,
        orderSide=val,
        timeInForce=xt.VALIDITY_DAY,
        disclosedQuantity=0,
        orderQuantity=order_quantity,
        limitPrice=price,
        stopPrice=0,
        apiOrderSource="WEBAPI",
        orderUniqueIdentifier="454845",
        clientID="66BP01" )

    print("Place Order: ", response)
    write_to_order_logs(f"Broker Order Response: [{datetime.now()}]  {order_side} quantity: {order_quantity} price: {price} response: {response}")
    print("-" * 50) 
    write_to_order_logs("-" * 50)
    

def write_to_order_logs(message):
    with open('OrderLog.txt', 'a') as file:  # Open the file in append mode
        file.write(message + '\n')

def get_user_settings():
    global result_dict, instrument_id_list, Equity_instrument_id_list, Future_instrument_id_list, FyerSymbolList
    from datetime import datetime
    import pandas as pd

    delete_file_contents("OrderLog.txt")

    try:
        csv_path = 'TradeSettings.csv'
        df = pd.read_csv(csv_path)
        df.columns = df.columns.str.strip()

        result_dict = {}
        instrument_id_list = []
        Equity_instrument_id_list = []
        Future_instrument_id_list = []
        FyerSymbolList = []

        for index, row in df.iterrows():
            symbol = row['Symbol']
            expiry = row['EXPIERY']  # Format: 29-05-2025

            # Convert expiry to API format: DDMonYYYY (e.g., 29May2025)
            expiry_api_format = datetime.strptime(expiry, "%d-%m-%Y").strftime("%d%b%Y")
            expiry_date = datetime.strptime(expiry, "%d-%m-%Y")

            # Construct Fyers Future Symbol: NSE:<SYMBOL><YY><MON>FUT
            
            
            

            # Fetch FUTSTK instrument ID
            fut_response = xts_marketdata.get_future_symbol(
                exchangeSegment=2,  # NSEFO
                series='FUTSTK',
                symbol=symbol,
                expiryDate=expiry_api_format
            )

            if fut_response['type'] == 'success' and 'result' in fut_response:
                result_item = fut_response['result'][0]
                NSEFOinstrument_id = int(result_item['ExchangeInstrumentID'])
                lot_size = int(result_item.get('LotSize', 0))
            else:
                print(f"[ERROR] Could not get FUTSTK instrument ID for {symbol} {expiry_api_format}")
                NSEFOinstrument_id = None
                lot_size = None

            # Fetch EQ instrument ID (NSECM)
            eq_response = xts_marketdata.get_equity_symbol(
                exchangeSegment=1,  # NSECM
                series='EQ',
                symbol=symbol
            )

            if eq_response['type'] == 'success' and 'result' in eq_response:
                NSECMinstrument_id = int(eq_response['result'][0]['ExchangeInstrumentID'])
            else:
                print(f"[ERROR] Could not get EQ instrument ID for {symbol}")
                NSECMinstrument_id = None
            
            try:
    # Parse '26-06-2025' → datetime object
                expiry_date = datetime.strptime(expiry, '%d-%m-%Y')
    # Format as '25JUN'
                new_date_string = expiry_date.strftime('%y%b').upper()
                fyers_fut_symbol = f"NSE:{symbol}{new_date_string}FUT"
            except ValueError as e:
                print(f"[ERROR] Failed to parse expiry for symbol {symbol}: {expiry}. Error: {e}")
                fyers_fut_symbol = None

            symbol_dict = {
                "Symbol": symbol, "unique_key": f"{symbol}_{expiry}",
                "Expiry": expiry,
                "Quantity": int(row['Quantity']), "LotSize": lot_size,
                "Timeframe": int(row['Timeframe']),
                "MA1": int(row['MA1']), "MA2": int(row['MA2']),
                "RSI_Period": int(row['RSI_Period']), "RSI_Buy": int(row['RSI_Buy']),
                "RSI_Sell": int(row['RSI_Sell']), "TargetBuffer": float(row['TargetBuffer']),
                "StartTime": datetime.strptime(row["StartTime"], "%H:%M:%S").time(),
                "StopTime": datetime.strptime(row["Stoptime"], "%H:%M:%S").time(),
                "SquareOffTime": datetime.strptime(row["SquareOffTime"], "%H:%M").time(),
                "PercentagePrice": float(row['PercentagePrice']),
                "PerVal": None, "TakeTrade": None, "R1_S1_CONDITION": None,
                "NSEFOexchangeInstrumentID": NSEFOinstrument_id,
                "NSECMexchangeInstrumentID": NSECMinstrument_id,
                "PrevOpen": None, "PrevHigh": None, "PrevLow": None, "PrevClose": None,
                "ma1Val": None, "ma2Val": None, "RsiVal": None, "last_run_time": None,
                "PvtPoint": None, "BottomRange": None, "TopRange": None,
                "R1": None, "R2": None, "R3": None, "last_close": None,
                "S1": None, "S2": None, "S3": None, "AllowedDiff": None,
                "ActualDiff": None, "Trade": None, "TargetExecuted": False,
                "EQltp": None, "Futltp": None, "buytargetvalue": None,
                "selltargetvalue": None, "dayOpen": None, "AllowedTradeType": None,
                "Allowed_S1_Pivot": None, "Allowed_R1_Pivot": None,
                "Allowed_S1_Pivot_value": None, "Allowed_R1_Pivot_value": None,
                "last_high": None, "last_low": None, "SquareOffExecuted": False,
                "Series": None, "Candletimestamp": None,
                "FyersTf": row['FyersTf'],
                "FyresSymbol": f"NSE:{symbol}-EQ",
                "FyresLtp": None,
                "FyersFutSymbol": fyers_fut_symbol,
                "FyersFutLtp": None
            }

            result_dict[symbol_dict["unique_key"]] = symbol_dict

            if NSECMinstrument_id:
                Equity_instrument_id_list.append({
                    "exchangeSegment": 1,
                    "exchangeInstrumentID": NSECMinstrument_id
                })

            if NSEFOinstrument_id:
                Future_instrument_id_list.append({
                    "exchangeSegment": 2,
                    "exchangeInstrumentID": NSEFOinstrument_id
                })

            FyerSymbolList.append(symbol_dict["FyresSymbol"])
            FyerSymbolList.append(symbol_dict["FyersFutSymbol"])


        print("result_dict: ", result_dict)
        print("-" * 50)
        print("Future_instrument_id_list: ", Future_instrument_id_list)
        print("-" * 50)
        print("Equity_instrument_id_list: ", Equity_instrument_id_list)
        print("-" * 50)

    except Exception as e:
        print("Error happened in fetching symbol", str(e))



def get_api_credentials():
    credentials = {}
    try:
        df = pd.read_csv('Credentials.csv')
        for index, row in df.iterrows():
            title = row['Title']
            value = row['Value']
            credentials[title] = value
    except pd.errors.EmptyDataError:
        print("The CSV file is empty or has no data.")
    except FileNotFoundError:
        print("The CSV file was not found.")
    except Exception as e:
        print("An error occurred while reading the CSV file:", str(e))
    return credentials




def format_date_time(date_time):
    """
    Format datetime object to required format: MMM DD YYYY HHMMSS
    """
    return datetime.strftime("%b %d %Y %H%M%S")

RUN_INTERVAL_SECONDS=None

def login_marketdata_api():
    """
    Login to the Market Data API and return the XTSConnect object.
    """
    global xts_marketdata
    global RUN_INTERVAL_SECONDS
    try:
        credentials = get_api_credentials()
        RUN_INTERVAL_SECONDS = int(credentials.get("RunInterval", 180))
        source = "WEBAPI"
        market_data_app_key = credentials.get("Market_Data_API_App_Key")
        market_data_app_secret = credentials.get("Market_Data_API_App_Secret")
        
        if not market_data_app_key or not market_data_app_secret:
            print("Missing Market Data API credentials in Credentials.csv")
            return None
            
        xts_marketdata = XTSConnect(market_data_app_key, market_data_app_secret, source,accessToken=None)
        response = xts_marketdata.marketdata_login()
        # print("Market Data Login Response:", response)
        
        if response and 'result' in response:
            print("Market Data login successful")
            return xts_marketdata
        else:
            print("Market Data login failed")
            return None
            
    except Exception as e:
        print(f"Error during market data login: {str(e)}")
        traceback.print_exc()
        return None



def fetch_historical_ohlc(xts_marketdata, exchangeSegment, exchangeInstrumentID, startTime, endTime, compressionValue):
    """
    Fetch and format historical OHLC data for a given instrument
    Returns a pandas DataFrame with columns: [Timestamp, Open, High, Low, Close, Volume, oi]
    """
    try:
        response = xts_marketdata.get_ohlc(
            exchangeSegment=exchangeSegment,
            exchangeInstrumentID=exchangeInstrumentID,
            startTime=startTime,
            endTime=endTime,
            compressionValue=compressionValue
        )

        if response['type'] == 'success' and 'result' in response:
            raw_data = response['result'].get('dataReponse', '')
            if not raw_data:
                print("No OHLC data found.")
                return None

            data_list = raw_data.strip().split(',')
            split_data = [item.split('|')[:-1] for item in data_list]

            df = pd.DataFrame(split_data, columns=['Timestamp', 'open', 'high', 'low', 'close', 'volume', 'oi'])

            # Convert data types
            df = df.astype({
                'Timestamp': int,
                'open': float,
                'high': float,
                'low': float,
                'close': float,
                'volume': int,
                'oi': int
            })

            df['Timestamp'] = pd.to_datetime(df['Timestamp'], unit='s')

            # print(df.head())  # Display first few rows
            return df

        else:
            print("OHLC API returned no data or an error.")
            return None

    except Exception as e:
        print(f"Error fetching OHLC data: {str(e)}")
        traceback.print_exc()
        return None

def get_previous_day_ohlc(symbol, instrument_id):
    try:
        # Read Reference.csv
        ref_df = pd.read_csv('Reference.csv')
        
        # Find matching symbol in Reference.csv
        symbol_data = ref_df[(ref_df['TckrSymb'] == symbol) & (ref_df['SctySrs'] == 'EQ')]

        
        if symbol_data.empty:
            print(f"No data found in Reference.csv for {symbol}")
            return None, None, None, None
            
        # Get OHLC values from Reference.csv
        open_ = float(symbol_data['OpnPric'].iloc[0])
        high = float(symbol_data['HghPric'].iloc[0])
        low = float(symbol_data['LwPric'].iloc[0])
        close = float(symbol_data['ClsPric'].iloc[0])
        series = symbol_data['SctySrs'].iloc[0]

        print(f"[{symbol}] Previous day OHLC from Reference.csv => O:{open_}, H:{high}, L:{low}, C:{close}")
        return open_, high, low, close, series

    except Exception as e:
        print(f"Failed reading OHLC from Reference.csv for {symbol}: {str(e)}")
        traceback.print_exc()
        return None, None, None, None, None


def chunk_instruments(instrument_list, chunk_size=50):
    for i in range(0, len(instrument_list), chunk_size):
        yield instrument_list[i:i + chunk_size]



def Equity_MarketQuote(xts_marketdata):
    global Equity_instrument_id_list, result_dict

    if not Equity_instrument_id_list:
        print("Instrument list is empty, skipping quote fetch.")
        return

    # Mapping: InstrumentID → Symbol
    symbol_by_id = {
    params.get("NSECMexchangeInstrumentID"): (symbol, params)
    for symbol, params in result_dict.items()
    if params.get("NSECMexchangeInstrumentID") and params.get("TakeTrade") == True 
        }



    for chunk in chunk_instruments(Equity_instrument_id_list, 50):
        try:
            response = xts_marketdata.get_quote(
                Instruments=chunk,
                xtsMessageCode=1501,
                publishFormat='JSON'
            )
            # print(f"response: {response}")
            

            if response and response.get("type") == "success":
                
                quote_strings = response["result"].get("listQuotes", [])

                for quote_str in quote_strings:
                    try:
                        item = json.loads(quote_str)
                        instrument_id = item.get("ExchangeInstrumentID")

                        if instrument_id in symbol_by_id:
                            symbol, params = symbol_by_id[instrument_id]
                            ltp = item.get("LastTradedPrice")
                            params["EQltp"] = int(ltp)  # ✅ Now valid and consistent
                            params["dayOpen"] = item.get("Open")
                            print(f"[params[dayOpen]] {symbol}: {params["dayOpen"]}")
                            print(f"[params[EQltp]] {symbol}: {params["EQltp"]}")

                    except Exception as inner_err:
                        print(f"[WARN] Skipping malformed quote: {inner_err}")
                        continue
            else:
                print(f"[ERROR] Unexpected quote response: {response}")

        except Exception as e:
            print(f"[ERROR] While fetching quote chunk: {e}")
            traceback.print_exc()



def Future_MarketQuote(xts_marketdata):
    global Future_instrument_id_list, result_dict

    if not Future_instrument_id_list:
        print("Instrument list is empty, skipping quote fetch.")
        return

    # Mapping: InstrumentID → Symbol
    symbol_by_id = {
    params.get("NSEFOexchangeInstrumentID"): (symbol, params)
    for symbol, params in result_dict.items()
    if params.get("NSEFOexchangeInstrumentID") and params.get("TakeTrade") == True and params.get("R1_S1_CONDITION") == True
        }



    for chunk in chunk_instruments(Future_instrument_id_list, 50):
        try:
            response = xts_marketdata.get_quote(
                Instruments=chunk,
                xtsMessageCode=1501,
                publishFormat='JSON'
            )

            if response and response.get("type") == "success":
                quote_strings = response["result"].get("listQuotes", [])

                for quote_str in quote_strings:
                    try:
                        item = json.loads(quote_str)
                        instrument_id = item.get("ExchangeInstrumentID")

                        if instrument_id in symbol_by_id:
                            symbol, params = symbol_by_id[instrument_id]
                            ltp = item.get("LastTradedPrice")
                            params["Futltp"] = int(ltp)  # ✅ Now valid and consistent
                            # print(f"[params[Futltp]] {symbol}: {params["Futltp"]}")

                    except Exception as inner_err:
                        print(f"[WARN] Skipping malformed quote: {inner_err}")
                        continue
            else:
                print(f"[ERROR] Unexpected quote response: {response}")

        except Exception as e:
            print(f"[ERROR] While fetching quote chunk: {e}")
            traceback.print_exc()




allowed_trades_saved = False



def UpdateData():
    global result_dict

    for symbol, ltp in shared_data.items(): 
        for key, value in result_dict.items():
            if value.get('FyresSymbol') == symbol:
                value['FyresLtp'] = float(ltp)
                print(f"[EQ] Updated {symbol} with LTP: {ltp}")
                break  # Optional: skip if you assume each symbol is unique
            elif value.get('FyersFutSymbol') == symbol:
                value['FyersFutLtp'] = float(ltp)
                print(f"[FUT] Updated {symbol} with LTP: {ltp}")
                break

    

   




def main_strategy():
    global allowed_trades_saved
    try:
        global xts_marketdata
        
        if not xts_marketdata:
            print("Market Data API not initialized")
            return
            
        end_date = datetime.now()
        start_date = end_date - timedelta(days=10)

        start_time_str = start_date.strftime("%b %d %Y 090000")
        end_time_str = end_date.strftime("%b %d %Y 153000")

        now = datetime.now()
        now_time = now.time()
        
        # print(f"\nFetching data from {start_time_str} to {end_time_str}")
        
        
        # Process each symbol from TradeSettings.csv
        for unique_key, params in result_dict.items():
            # initialize loop-specific variables to avoid UnboundLocalError
            symbol_name = params["Symbol"]
            last_close = None
            fetch_duration = 0.0
            
            if not (params["StartTime"] <= now_time <= params["StopTime"]):
                print(f"[{symbol_name}] Not within trading hours. Skipping...")
                continue

            if params.get("PrevOpen") is None:
                try:
                    start_time = datetime.now()
                    o, h, l, c,series = get_previous_day_ohlc(symbol_name, params["NSECMexchangeInstrumentID"])
                    if None in (o, h, l, c):
                        print(f"[WARN] Incomplete OHLC data for {symbol_name}. Retrying in 1 second...")
                        time.sleep(1)
                        o, h, l, c,series = get_previous_day_ohlc(symbol_name, params["NSECMexchangeInstrumentID"])

                        if None in (o, h, l, c):
                            print(f"[ERROR] OHLC data still missing for {symbol_name}. Skipping...")
                            with open("OrderLog.txt", "a") as log_file:
                                log_file.write(f"[{datetime.now()}] OHLC data not found for {symbol_name}\n")
                            continue  # move to next symbol

                     # Store OHLC
                    params["PrevOpen"] = o
                    params["PrevHigh"] = h
                    params["PrevLow"] = l
                    params["PrevClose"] = c
                    params["Series"] = series

                    # Calculated values
                    close = c
                    allowed_diff = close * params["PercentagePrice"] / 100
                    pivot = (h + l + c) / 3
                    top = (h + l) / 2
                    bottom = (pivot - top) + pivot
                    diff_c_b = abs(pivot - bottom)
                    diff_t_c = abs(top - pivot)
                    params["Allowed_S1_Pivot_value"]=close*1.5*0.01

                    params["Allowed_R1_Pivot_value"]=close*1.5*0.01
                   
                    

                    params["ActualDiff_Pivot_Bottom"] = diff_c_b
                    params["ActualDiff_Top_Pivot"] = diff_t_c
                    params["PvtPoint"] = pivot
                    params["BottomRange"] = bottom
                    params["TopRange"] = top
                    params["AllowedDiff"] = allowed_diff


                    
                # TakeTrade condition
                    params["TakeTrade"] = diff_c_b <= allowed_diff and diff_t_c <= allowed_diff

                # Support/Resistance
                    params["R1"] = (2 * pivot) - l
                    params["R2"] = pivot + (h - l)
                    params["R3"] = h + 2 * (pivot - l)
                    params["S1"] = (2 * pivot) - h
                    params["S2"] = pivot - (h - l)
                    params["S3"] = l - 2 * (h - pivot)
                    target_buffer = params["TargetBuffer"] 
                    


                    params["Allowed_R1_Pivot"] =abs(params["R1"]-pivot)
                    params["Allowed_S1_Pivot"] =abs(pivot-params["S1"])

                    params[ "R1_S1_CONDITION"] = params["Allowed_R1_Pivot"] < params["Allowed_R1_Pivot_value"] and params["Allowed_S1_Pivot"] < params["Allowed_S1_Pivot_value"]


                    params["buytargetvalue"] = params["R2"] * params["TargetBuffer"]*0.01 
                    params["buytargetvalue"] = params["R2"]-params["buytargetvalue"]

                    params["selltargetvalue"] = params["S2"] * params["TargetBuffer"]*0.01 
                    params["selltargetvalue"] = params["S2"]+params["selltargetvalue"]

                    endtime=datetime.now()
                    latency=endtime-start_time
                    print(f"Latency for {symbol_name}: {latency}")

                except Exception as e:
                    print(f"Error fetching previous OHLC for {symbol_name}: {str(e)}")
                    traceback.print_exc()
                    continue
            
            
            # Skip if not yet time to run based on timeframe
            # last_run = params.get("last_run_time")
            # if last_run and (now - last_run).total_seconds() < timeframe:
            #     continue
        # Equity_MarketQuote(xts_marketdata)
        Future_MarketQuote(xts_marketdata) 
        UpdateData()
        fetch_start = time.time()

        for unique_key, params in result_dict.items():
            if now_time>=params["SquareOffTime"] and params["SquareOffExecuted"] == False and params["Trade"] != None:  
                print(f"[{params['Symbol']}] Squareoff time reached. Executing squareoff.")
                params["SquareOffExecuted"] = True
                if params["Trade"] == "BUY":
                    print(f"[{params['Symbol']}] Executing BUY position squareoff")
                    place_order(nfo_ins_id=params["NSEFOexchangeInstrumentID"],order_quantity=params["OrderQuantity"],order_side="SELL",
                                price=params['FyersFutLtp'],unique_key="1234")
                    write_to_order_logs(f"[{datetime.now()}] {params['Symbol']} BUY position squareoff at {params['last_close']}")
                elif params["Trade"] == "SELL":
                    print(f"[{params['Symbol']}] Executing SELL position squareoff")
                    place_order(nfo_ins_id=params["NSEFOexchangeInstrumentID"],order_quantity=params["OrderQuantity"],
                                order_side="BUY",price=params['FyersFutLtp'],unique_key="1234")
                    write_to_order_logs(f"[{datetime.now()}] {params['Symbol']} SELL position squareoff at {params['last_close']}")
                params["Trade"] = "TAKENOMORETRADES"
                params["TargetExecuted"] = True
                print(f"[{params['Symbol']}] Squareoff executed successfully.")
                
                
        
        for unique_key, params in result_dict.items():
            if not (params["StartTime"] <= now_time <= params["StopTime"]):
                continue

            if params.get("TakeTrade") != True or params.get("R1_S1_CONDITION") != True:
                continue

            print(f"Symbol:[{params['Symbol']}] TakeTrade:[{params['TakeTrade']}] R1_S1_CONDITION:[{params['R1_S1_CONDITION']}]")    

            

            #               Run only if the current time has crossed the next scheduled fetch time
           
            if params.get("last_run_time") is None or datetime.now() >= params["last_run_time"]:
                try:
                    symbol_name = params["Symbol"]
                    NSECMinstrument_id = params["NSECMexchangeInstrumentID"]
                    timeframe = params["Timeframe"]
                    # params["last_run_time"] = now
                    # Fetch historical data
                    if params["TakeTrade"] == True and params["R1_S1_CONDITION"] == True:
                        ohlc_data = fetch_historical_ohlc(
                                xts_marketdata=xts_marketdata,
                            exchangeSegment="NSECM",
                        exchangeInstrumentID=NSECMinstrument_id,
                        startTime=start_time_str,
                        endTime=end_time_str,
                        compressionValue=timeframe
                        )
                      
                        fohlc_data=fetchOHLC(symbol=params["FyresSymbol"],tf=params["FyersTf"])
                        # print("fohlc_data: ",fohlc_data)
                        last_candle_fyres = fohlc_data.iloc[-1]
                        # print("Last Candle fyers:")
                        # print(last_candle_fyres)
                        Candletimestamp = pd.to_datetime(fohlc_data['date'].iloc[-1])
                        second_last_candle_fyres = fohlc_data.iloc[-2]
                        # print("Second Last Candle fyers:")
                        # print(second_last_candle_fyres)

                        # print("fohlc_data: ",fohlc_data)
                        # print("last_candle: ",last_candle_fyres)


                        
                    

                except Exception as e:
                    print(f"Error fetching OHLC data for {symbol_name}: {str(e)}")
                    traceback.print_exc()
                    continue
                    
                if ohlc_data is not None and not ohlc_data.empty:
                    print(f"Successfully fetched OHLC data for {symbol_name}:")
                    # print("ohlc_data: ", ohlc_data)
                    
                else:
                    print(f"Failed to fetch data for {symbol_name}")

                ohlc_data.columns = [col.lower() for col in ohlc_data.columns]
                # Candletimestamp = pd.to_datetime(fohlc_data['date'].iloc[-1]).replace(tzinfo=None)
                # params["Candletimestamp"] = Candletimestamp

                    # Convert pandas OHLC to polars
                fohlc_data = fohlc_data.astype({
                    col: "float64" if fohlc_data[col].dtype.name.startswith("Float") else
                        "int64" if fohlc_data[col].dtype.name.startswith("Int") else
                        fohlc_data[col].dtype
                    for col in fohlc_data.columns
                })

                pl_df = pl.from_pandas(fohlc_data)

                    # Calculate EMA using values from settings (MA1 and MA2)
                pl_df = pl_df.with_columns([
                        pl.col("close").ta.ema(int(params["MA1"])).alias(f"ema_{params['MA1']}"),
                        pl.col("close").ta.ema(int(params["MA2"])).alias(f"ema_{params['MA2']}"),
                        pl.col("close").ta.rsi(int(params["RSI_Period"])).alias("rsi_14")
                    ])
                
                    # Save last EMA values into result_dict
                params["ma1Val"] = pl_df.select(f"ema_{params['MA1']}")[-2, 0]
                params["ma2Val"] = pl_df.select(f"ema_{params['MA2']}")[-2, 0]
                params["RsiVal"] = pl_df.select("rsi_14")[-2, 0]
                params["last_close"] = pl_df.select("close")[-2, 0]
                params["last_high"] = pl_df.select("high")[-2, 0]
                params["last_low"] = pl_df.select("low")[-2, 0]
                 

                    # Show first few rows
                fetch_end = time.time()
                fetch_duration = fetch_end - fetch_start
                Candletimestamp = pd.to_datetime(fohlc_data['date'].iloc[-1]).replace(tzinfo=None)
                params["Candletimestamp"] = Candletimestamp
                params["last_run_time"] = Candletimestamp + timedelta(minutes=params["FyersTf"])
                params["last_run_time"] = params["last_run_time"].replace(tzinfo=None)

                # print fetch timing and last close right after fetch
                print(f"Symbol: {symbol_name}, Next run time: {params['last_run_time']}, Total Time taken by api to fetch data: {fetch_duration:.2f} seconds")
                print(f"last_close: {params['last_close']}")
            
            

            # Get required values
            # ✅ Last candle close
            ema1 = params["ma1Val"]       # ✅ EMA1 value
            ema2 = params["ma2Val"]       # ✅ EMA2 value
            rsi_val = params["RsiVal"]
            prev_high = params["PrevHigh"]
            prev_low = params["PrevLow"]
            r1 = params["R1"]
            r2 = params["R2"]
            s1 = params["S1"]
            s2 = params["S2"]

            
            params["OrderQuantity"]= int(params["Quantity"]*params["LotSize"])
            # if params["dayOpen"] > params["PvtPoint"] and params["AllowedTradeType"] == None:
            #     params["AllowedTradeType"] = "BUY"
            # if params["dayOpen"] < params["PvtPoint"] and params["AllowedTradeType"] == None:
            #     params["AllowedTradeType"] = "SELL"

            print(f"""
                    Symbol: {params['Symbol']}
                    Order Quantity: {params['OrderQuantity']}
                    EMA1: {ema1}
                    EMA2: {ema2}
                    RSI Value: {rsi_val}
                    Previous High: {prev_high}
                    Previous Low: {prev_low}
                    Resistance 1 (R1): {r1}
                    Resistance 2 (R2): {r2}
                    Support 1 (S1): {s1}
                    Support 2 (S2): {s2}
                    Target Buffer: {params["TargetBuffer"]}
                    Buy Target Value: {params["buytargetvalue"]}
                    Sell Target Value: {params["selltargetvalue"]}
                    Last Close: {params["last_close"]}
            
                    Futltp: {params['Futltp']}
                    TargetExecuted: {params["TargetExecuted"]}
                    Trade: {params["Trade"]}
                    TakeTrade: {params["TakeTrade"]}
                    R1_S1_CONDITION: {params["R1_S1_CONDITION"]}
                    Allowed_R1_Pivot: {params["Allowed_R1_Pivot"]}
                    Allowed_S1_Pivot: {params["Allowed_S1_Pivot"]}
                    Allowed_R1_Pivot_value: {params["Allowed_R1_Pivot_value"]}
                    Allowed_S1_Pivot_value: {params["Allowed_S1_Pivot_value"]}
                    dayOpen: {params["dayOpen"]}
                    last_high: {params["last_high"]}
                    last_low: {params["last_low"]}
                    FyresLtp: {params["FyresLtp"]}
                    FyersFutLtp:{params["FyersFutLtp"]}
                    """)
            print("-" * 50)  # dashed line separator


                # Target Check
            if(params["FyresLtp"] is not None and (params['FyresLtp']>=params["buytargetvalue"] or params["last_high"] >=params["buytargetvalue"] ) and 
                params["TargetExecuted"] == False):
                print(f"[{params['Symbol']}] price: {params['FyresLtp']} or {params['last_high']} reached Buy Target Value= {params["buytargetvalue"]}")
                params["TargetExecuted"] = True
                write_to_order_logs(f"[{datetime.now()}] Candletimestamp: {params["Candletimestamp"]} {params['Symbol']} buytargetvalue REACHED price: {params['FyresLtp']} or last_high: {params['last_high']}, buytargetvalue: {params["buytargetvalue"]}")
                if params["Trade"] == "BUY":
                    print(f"[{params['Symbol']}] Buy Target  executed")
                    params["Trade"] = "TAKENOMORETRADES"
                    place_order(nfo_ins_id=params["NSEFOexchangeInstrumentID"],order_quantity=params["OrderQuantity"],
                                order_side="SELL",price=params['FyersFutLtp'],unique_key="1234")
                    write_to_order_logs(f"[{datetime.now()}] Candletimestamp: {params["Candletimestamp"]} {params['Symbol']} price: {params['FyresLtp']} or last_high: {params['last_high']} reached Buy Target Value= {params["buytargetvalue"]}")

            if (params["FyresLtp"] is not None and (params['FyresLtp']<=params["selltargetvalue"] or params["last_low"]<=params["selltargetvalue"]) and 
                params["TargetExecuted"] == False ):
                print(f"[{params['Symbol']}] price: {params['FyresLtp']} or {params['last_low']} reached Sell Target Value= {params["selltargetvalue"]}")
                params["TargetExecuted"] = True
                write_to_order_logs(f"[{datetime.now()}] Candletimestamp: {params["Candletimestamp"]} {params['Symbol']} selltargetvalue REACHED last_low: {params["last_low"]} or FyresLtp: {params['FyresLtp']}, selltargetvalue: {params["selltargetvalue"] }")
                if params["Trade"] == "SELL":
                    print(f"[{params['Symbol']}] Sell Target  executed")
                    params["Trade"] = "TAKENOMORETRADES"
                    place_order(nfo_ins_id=params["NSEFOexchangeInstrumentID"],order_quantity=params["OrderQuantity"],
                                order_side="BUY",price=params['FyersFutLtp'],unique_key="1234")
                    write_to_order_logs(f"[{datetime.now()}] Candletimestamp: {params["Candletimestamp"]} {params['Symbol']} selltargetvalue REACHED last_low: {params["last_low"]} or EQltp: {params['FyresLtp']}, selltargetvalue: {params["selltargetvalue"] }")
                
                
                # main entry condition 
            if params["TargetExecuted"] == False and params["Trade"] == None:
                print(f"{datetime.now()} [{params['Symbol']}] Checking entry condition.. ")
                    #  buy condition
                if (params["TakeTrade"] == True  and params["R1_S1_CONDITION"] == True and params["last_close"] > ema1 and
                    params["last_close"] > ema2 and params["last_close"] > r1 and
                    params["last_close"]>prev_high and ema1>ema2 ) and params["Trade"] == None:
                    
                    print(f"[{params['Symbol']}] Buy condition met")
                    params["Trade"] = "BUY"
                    print(f"[{params['Symbol']}] BUY @ {params['Symbol']}  {params["last_close"]}")
                    place_order(nfo_ins_id=params["NSEFOexchangeInstrumentID"],order_quantity=params["OrderQuantity"],
                                order_side="BUY",price=params['FyersFutLtp'],unique_key="1234")
                    write_to_order_logs(f"[{datetime.now()}] Candletimestamp: {params["Candletimestamp"]} BUY @ {params['Symbol']}  {params["last_close"]} ,ema1: {ema1}, ema2: {ema2}, r1: {r1}, prev_high: {prev_high}, ema1>ema2: {ema1>ema2}, last_close>ema1: {params['last_close']>ema1}, last_close>ema2: {params['last_close']>ema2}, last_close>r1: {params['last_close']>r1}")

                    # sell condition
                if (params["TakeTrade"] == True  and params["R1_S1_CONDITION"] == True and params["last_close"] < ema1 and params["last_close"] < ema2 and
                    params["last_close"] < s1 and params["last_close"]<prev_low and ema1<ema2 ) and params["Trade"] == None:
                    print(f"[{params['Symbol']}] Sell condition met")
                    params["Trade"] = "SELL"
                    place_order(nfo_ins_id=params["NSEFOexchangeInstrumentID"],order_quantity=params["OrderQuantity"],
                                order_side="SELL",price=params["FyersFutLtp"],unique_key="1234")
                    write_to_order_logs(f"[{datetime.now()}] Candletimestamp: {params["Candletimestamp"]} SELL @ {params['Symbol']}  {params["last_close"]} ,ema1: {ema1}, ema2: {ema2}, s1: {s1}, prev_low: {prev_low}, ema1<ema2: {ema1<ema2}, last_close<ema1: {params['last_close']<ema1}, last_close<ema2: {params['last_close']<ema2}, last_close<s1: {params['last_close']<s1}")

                # REENTRY TRIGGERED LOGIC
            if params["Trade"] == "REENTERYCHECKED":
                print(f"[{params['Symbol']}] REENTRY TRIGGERED LOGIC completed")

                if (params["TakeTrade"] == True  and params["R1_S1_CONDITION"] == True and params["last_close"] > ema1 and params["last_close"] > ema2 and
                    params["last_close"] > r1 and params["last_close"]>prev_high and ema1>ema2 ) :
                        print(f"[{params['Symbol']}] Buy re-entry condition met")
                        params["Trade"] = "BUY"
                        print(f"[{params['Symbol']}] BUY re-entry condition met")
                        place_order(nfo_ins_id=params["NSEFOexchangeInstrumentID"],order_quantity=params["OrderQuantity"],
                                    order_side="BUY",price=params['FyersFutLtp'],unique_key="1234")
                        write_to_order_logs(f"[{datetime.now()}] Candletimestamp: {params["Candletimestamp"]} {params['Symbol']} BUY re-entry {params['last_close']}, ema1: {ema1}, ema2: {ema2}, r1: {r1}, prev_high: {prev_high}, ema1>ema2: {ema1>ema2}, last_close>ema1: {params['last_close']>ema1}, last_close>ema2: {params['last_close']>ema2}, last_close>r1: {params['last_close']>r1}")
                    
                if params["Trade"] == "REENTERYCHECKED":
                    if (params["TakeTrade"] == True  and params["R1_S1_CONDITION"] == True and params["last_close"] < ema1 and params["last_close"] < ema2 and
                        params["last_close"] < s1 and params["last_close"]<prev_low and ema1<ema2 ) :
                        print(f"[{params['Symbol']}] Sell re-entry condition met")
                        params["Trade"] = "SELL"
                        print(f"[{params['Symbol']}] SELL re-entry condition met")
                        place_order(nfo_ins_id=params["NSEFOexchangeInstrumentID"],order_quantity=params["OrderQuantity"],
                                    order_side="SELL",price=params['FyersFutLtp'],unique_key="1234")
                        write_to_order_logs(f"[{datetime.now()}] Candletimestamp: {params["Candletimestamp"]} {params['Symbol']} SELL re-entry {params['last_close']}, ema1: {ema1}, ema2: {ema2}, s1: {s1}, prev_low: {prev_low}, ema1<ema2: {ema1<ema2}, last_close<ema1: {params['last_close']<ema1}, last_close<ema2: {params['last_close']<ema2}, last_close<s1: {params['last_close']<s1}")


                    # Stoploss  
            if params["Trade"] == "BUY":
                print(f"[{params['Symbol']}] Stoploss  LOGIC checking for BUY")

                if params["last_close"] < ema1 and params["RsiVal"] <= params["RSI_Buy"] and params["last_close"] >0 and params["RsiVal"] >0 :      
                    print(f"[{params['Symbol']}]Buy Stoploss executed")
                    params["Trade"] = "BUYSTOPLOSS"
                    print(f"[{params['Symbol']}] BUY Stoploss executed")    
                    place_order(nfo_ins_id=params["NSEFOexchangeInstrumentID"],order_quantity=params["OrderQuantity"],order_side="SELL",
                                 price=params['FyersFutLtp'],unique_key="1234")  
                    write_to_order_logs(f"[{datetime.now()}] Candletimestamp: {params["Candletimestamp"]} {params['Symbol']} BUY Stoploss Last Close: {params["last_close"]}, ema1: {ema1}, rsi_val: {rsi_val}")


            if params["Trade"] == "SELL":
                print(f"[{params['Symbol']}] Stoploss  LOGIC checking for SELL")
                
                if params["last_close"] > ema1 and params["RsiVal"] >= params["RSI_Sell"] and params["last_close"] >0 and params["RsiVal"] >0 :
                    print(f"[{params['Symbol']}]Sell Stoploss executed")
                    params["Trade"] = "SELLSTOPLOSS"
                    print(f"[{params['Symbol']}] SELL Stoploss executed")
                    place_order(nfo_ins_id=params["NSEFOexchangeInstrumentID"],order_quantity=params["OrderQuantity"],
                                order_side="BUY",price=params['FyersFutLtp'],unique_key="1234")
                    write_to_order_logs(f"[{datetime.now()}] Candletimestamp: {params["Candletimestamp"]} {params['Symbol']} SELL Stoploss Last Close: {params["last_close"]}, ema1: {ema1}, rsi_val: {rsi_val}")


                    # REENTERY LOGIC
            if params["Trade"] == "BUYSTOPLOSS":
                print(f"[{params['Symbol']}] REENTERY LOGIC checking for BUY")
                if params["last_close"] < min(prev_high,r1) :
                    print(f"[{params['Symbol']}]Price below both prev_day_high and r1, checking for buy re-entry")
                    params["Trade"] = "REENTERYCHECKED"
                    print(f"[{params['Symbol']}] BUY Reentry condition check completed")
                    write_to_order_logs(f"[{datetime.now()}] Candletimestamp: {params["Candletimestamp"]} {params['Symbol']} BUY Reentry check completed {params["last_close"]},min(prev_high,r1): {min(prev_high,r1)}")


            if params["Trade"] == "SELLSTOPLOSS":
                print(f"[{params['Symbol']}] REENTERY LOGIC checking for SELL")
                if params["last_close"] > max(prev_low,s1) :
                    print(f"[{params['Symbol']}]Price above both prev_day_low and s1, checking for sell re-entry")
                    params["Trade"] = "REENTERYCHECKED"
                    print(f"[{params['Symbol']}] SELL Reentry condition check completed")
                    write_to_order_logs(f"[{datetime.now()}] Candletimestamp: {params["Candletimestamp"]} {params['Symbol']} SELL Reentry check completed {params["last_close"]},max(prev_low,s1): {max(prev_low,s1)}")

                    
        



                # Optional: print last row with EMAs
                # print(pl_df.tail(5))
                # pl_df.write_csv(f"{symbol_name}.csv")

                # ltp= fetch_MarketQuote(xts_marketdata)
                # print(f"ltp {symbol_name}: ", ltp)
            
            # print("result_dict: ", result_dict)
        if not allowed_trades_saved:
            allowed_trades = []

            for symbol, params in result_dict.items():
                if params.get("TakeTrade") == True and params.get("R1_S1_CONDITION") == True:
                    allowed_trades.append({
                    "Symbol":params["Symbol"],
                    "PrevOpen" : params["PrevOpen"],
                    "PrevHigh": params["PrevHigh"],
                    "PrevLow": params["PrevLow"],
                    "PrevClose": params["PrevClose"],
                    "Series": params["Series"],
                    "PvtPoint": params["PvtPoint"],
                    "BottomRange": params["BottomRange"],
                    "TopRange": params["TopRange"],
                    "ActualDiff_Pivot_Bottom": params["ActualDiff_Pivot_Bottom"],
                    "ActualDiff_Top_Pivot": params["ActualDiff_Top_Pivot"],
                    "AllowedDiff": params["AllowedDiff"],
                    "R1": params["R1"],
                    "R2": params["R2"],
                    "R3": params["R3"],
                    "S1": params["S1"],
                    "S2": params["S2"],
                    "S3": params["S3"],
                    "buytargetvalue": params["buytargetvalue"],
                    "selltargetvalue": params["selltargetvalue"],
                    "TargetBuffer": params["TargetBuffer"],
                    "dayOpen": params["dayOpen"],
                    "AllowedTradeType": params["AllowedTradeType"],
                    "R1_S1_CONDITION": params["R1_S1_CONDITION"],
                    "Allowed_R1_Pivot": params["Allowed_R1_Pivot"],
                    "Allowed_S1_Pivot": params["Allowed_S1_Pivot"],
                    "Allowed_R1_Pivot_value": params["Allowed_R1_Pivot_value"],
                    "Allowed_S1_Pivot_value": params["Allowed_S1_Pivot_value"]
                })

            if allowed_trades:
                df = pd.DataFrame(allowed_trades)
                df.to_csv("AllowedTrades.csv", index=False)
                print("[✅] AllowedTrades.csv saved with tradeable symbols.")
                allowed_trades_saved = True  # ✅ Prevent saving again
            else:
                # allowed_trades_saved = True
                print("[ℹ️] No TakeTrade=True and R1_S1_CONDITION=True. Skipping AllowedTrades.csv.")
            
    except Exception as e:
        print("Error in main strategy:", str(e))
        traceback.print_exc()

if __name__ == "__main__":
    # # Initialize settings and credentials
    #   # <-- Add this line
    credentials_dict_fyers = get_api_credentials_Fyers()
    redirect_uri = credentials_dict_fyers.get('redirect_uri')
    client_id = credentials_dict_fyers.get('client_id')
    secret_key = credentials_dict_fyers.get('secret_key')
    grant_type = credentials_dict_fyers.get('grant_type')
    response_type = credentials_dict_fyers.get('response_type')
    state = credentials_dict_fyers.get('state')
    TOTP_KEY = credentials_dict_fyers.get('totpkey')
    FY_ID = credentials_dict_fyers.get('FY_ID')
    PIN = credentials_dict_fyers.get('PIN')
    # Automated login and initialization steps
    automated_login(client_id=client_id, redirect_uri=redirect_uri, secret_key=secret_key, FY_ID=FY_ID,
                                     PIN=PIN, TOTP_KEY=TOTP_KEY)
    get_api_credentials()
    xts_marketdata = login_marketdata_api()
    interactivelogin()
    get_user_settings()

    
    # Initialize Market Data API
    fyres_websocket(FyerSymbolList)
    if xts_marketdata:
        while True:
            now =   datetime.now()   
            print(f"\nStarting main strategy at {datetime.now()}")
            main_strategy()
            time.sleep(2)
    else:
        print("Failed to initialize Market Data API. Exiting...")



# sha
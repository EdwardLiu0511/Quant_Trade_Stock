import quandl
import pymysql.cursors
import pymysql
import pandas as pd
import math
import urllib
import requests
from bs4 import BeautifulSoup
from os import walk
from sqlalchemy import create_engine
import numpy as np
import matplotlib.pyplot as plt


class Utils(object):

    sp500_stocks = ['ABT', 'ABBV', 'ACN', 'ACE', 'ADBE', 'ADT', 'AAP', 'AES', 'AET', 'AFL', 'AMG', 'A', 'GAS', 'APD',
                    'ARG', 'AKAM', 'AA', 'AGN', 'ALXN', 'ALLE', 'ADS', 'ALL', 'ALTR', 'MO', 'AMZN', 'AEE', 'AAL', 'AEP',
                    'AXP', 'AIG', 'AMT', 'AMP', 'ABC', 'AME', 'AMGN', 'APH', 'APC', 'ADI', 'AON', 'APA', 'AIV', 'AMAT',
                    'ADM', 'AIZ', 'T', 'ADSK', 'ADP', 'AN', 'AZO', 'AVGO', 'AVB', 'AVY', 'BHI', 'BLL', 'BAC', 'BK',
                    'BCR', 'BXLT', 'BAX', 'BBT', 'BDX', 'BBBY', 'BRK-B', 'BBY', 'BLX', 'HRB', 'BA', 'BWA', 'BXP', 'BSK',
                    'BMY', 'BRCM', 'BF-B', 'CHRW', 'CA', 'CVC', 'COG', 'CAM', 'CPB', 'COF', 'CAH', 'HSIC', 'KMX', 'CCL',
                    'CAT', 'CBG', 'CBS', 'CELG', 'CNP', 'CTL', 'CERN', 'CF', 'SCHW', 'CHK', 'CVX', 'CMG', 'CB', 'CI',
                    'XEC', 'CINF', 'CTAS', 'CSCO', 'C', 'CTXS', 'CLX', 'CME', 'CMS', 'COH', 'KO', 'CCE', 'CTSH', 'CL',
                    'CMCSA', 'CMA', 'CSC', 'CAG', 'COP', 'CNX', 'ED', 'STZ', 'GLW', 'COST', 'CCI', 'CSX', 'CMI', 'CVS',
                    'DHI', 'DHR', 'DRI', 'DVA', 'DE', 'DLPH', 'DAL', 'XRAY', 'DVN', 'DO', 'DTV', 'DFS', 'DISCA',
                    'DISCK', 'DG', 'DLTR', 'D', 'DOV', 'DOW', 'DPS', 'DTE', 'DD', 'DUK', 'DNB', 'ETFC', 'EMN', 'ETN',
                    'EBAY', 'ECL', 'EIX', 'EW', 'EA', 'EMC', 'EMR', 'ENDP', 'ESV', 'ETR', 'EOG', 'EQT', 'EFX', 'EQIX',
                    'EQR', 'ESS', 'EL', 'ES', 'EXC', 'EXPE', 'EXPD', 'ESRX', 'XOM', 'FFIV', 'FB', 'FAST', 'FDX', 'FIS',
                    'FITB', 'FSLR', 'FE', 'FSIV', 'FLIR', 'FLS', 'FLR', 'FMC', 'FTI', 'F', 'FOSL', 'BEN', 'FCX', 'FTR',
                    'GME', 'GPS', 'GRMN', 'GD', 'GE', 'GGP', 'GIS', 'GM', 'GPC', 'GNW', 'GILD', 'GS', 'GT', 'GOOGL',
                    'GOOG', 'GWW', 'HAL', 'HBI', 'HOG', 'HAR', 'HRS', 'HIG', 'HAS', 'HCA', 'HCP', 'HCN', 'HP', 'HES',
                    'HPQ', 'HD', 'HON', 'HRL', 'HSP', 'HST', 'HCBK', 'HUM', 'HBAN', 'ITW', 'IR', 'INTC', 'ICE', 'IBM',
                    'IP', 'IPG', 'IFF', 'INTU', 'ISRG', 'IVZ', 'IRM', 'JEC', 'JBHT', 'JNJ', 'JCI', 'JOY', 'JPM', 'JNPR',
                    'KSU', 'K', 'KEY', 'GMCR', 'KMB', 'KIM', 'KMI', 'KLAC', 'KSS', 'KRFT', 'KR', 'LB', 'LLL', 'LH',
                    'LRCX', 'LM', 'LEG', 'LEN', 'LVLT', 'LUK', 'LLY', 'LNC', 'LLTC', 'LMT', 'L', 'LOW', 'LYB', 'MTB',
                    'MAC', 'M', 'MNK', 'MRO', 'MPC', 'MAR', 'MMC', 'MLM', 'MAS', 'MA', 'MAT', 'MKC', 'MCD', 'MHFI',
                    'MCK', 'MJN', 'MMV', 'MDT', 'MRK', 'MET', 'KORS', 'MCHP', 'MU', 'MSFT', 'MHK', 'TAP', 'MDLZ', 'MON',
                    'MNST', 'MCO', 'MS', 'MOS', 'MSI', 'MUR', 'MYL', 'NDAQ', 'NOV', 'NAVI', 'NTAP', 'NFLX', 'NWL',
                    'NFX', 'NEM', 'NWSA', 'NEE', 'NLSN', 'NKE', 'NI', 'NE', 'NBL', 'JWN', 'NSC', 'NTRS', 'NOC', 'NRG',
                    'NUE', 'NVDA', 'ORLY', 'OXY', 'OMC', 'OKE', 'ORCL', 'OI', 'PCAR', 'PLL', 'PH', 'PDCO', 'PAYX',
                    'PNR', 'PBCT', 'POM', 'PEP', 'PKI', 'PRGO', 'PFE', 'PCG', 'PM', 'PSX', 'PNW', 'PXD', 'PBI', 'PCL',
                    'PNC', 'RL', 'PPG', 'PPL', 'PX', 'PCP', 'PCLN', 'PFG', 'PG', 'PGR', 'PLD', 'PRU', 'PEG', 'PSA',
                    'PHM', 'PVH', 'QRVO', 'PWR', 'QCOM', 'DGX', 'RRC', 'RTN', 'O', 'RHT', 'REGN', 'RF', 'RSG', 'RAI',
                    'RHI', 'ROK', 'COL', 'ROP', 'ROST', 'RLC', 'R', 'CRM', 'SNDK', 'SCG', 'SLB', 'SNI', 'STX', 'SEE',
                    'SRE', 'SHW', 'SIAL', 'SPG', 'SWKS', 'SLG', 'SJM', 'SNA', 'SO', 'LUV', 'SWN', 'SE', 'STJ', 'SWK',
                    'SPLS', 'SBUX', 'HOT', 'STT', 'SRCL', 'SYK', 'STI', 'SYMC', 'SYY', 'TROW', 'TGT', 'TEL', 'TE',
                    'TGNA', 'THC', 'TDC', 'TSO', 'TXN', 'TXT', 'HSY', 'TRV', 'TMO', 'TIF', 'TWX', 'TWC', 'TJK', 'TMK',
                    'TSS', 'TSCO', 'RIG', 'TRIP', 'FOXA', 'TSN', 'TYC', 'UA', 'UNP', 'UNH', 'UPS', 'URI', 'UTX', 'UHS',
                    'UNM', 'URBN', 'VFC', 'VLO', 'VAR', 'VTR', 'VRSN', 'VZ', 'VRTX', 'VIAB', 'V', 'VNO', 'VMC', 'WMT',
                    'WBA', 'DIS', 'WM', 'WAT', 'ANTM', 'WFC', 'WDC', 'WU', 'WY', 'WHR', 'WFM', 'WMB', 'WEC', 'WYN',
                    'WYNN', 'XEL', 'XRX', 'XLNX', 'XL', 'XYL', 'YHOO', 'YUM', 'ZBH', 'ZION', 'ZTS','TELA']

    @staticmethod
    def save_eod_price(stocks, begin_date, end_date):

        quandl.ApiConfig.api_key = "kZCJvR1xGx9V6xXoCUN9"

        for ticker in stocks:

            print 'now moves to: '+ticker
            try:
                data = quandl.get_table('WIKI/PRICES', ticker = ticker, date = {'gte': begin_date, 'lte':end_date})
            except:
                print 'error in '+ticker
                continue
            # Connect to the database
            connection = pymysql.connect(host='localhost',
                             user='root',
                             password='86511936',
                             db='trade',
                             cursorclass=pymysql.cursors.DictCursor)
            try:
                with connection.cursor() as cursor:
                    for index,price in pd.DataFrame(data).iterrows():

                        sql = "INSERT INTO trade_EOD_Prices(ticker,date_id,open_price,high,low,close_price,volume,ex_dividend,split_ratio,adj_open,adj_high,adj_low,adj_close,adj_volume) VALUES(%s,STR_TO_DATE(%s,'%%Y-%%m-%%d'),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                        ticker=price['ticker']
                        date=str(price['date']).split(' ')[0]
                        open_price=float(price['open'])
                        high = float(price['high'])
                        low = float(price['low'])
                        close_price = float(price['close'])
                        volume = float(price['volume'])
                        ex_dividend = float(price['ex-dividend'])
                        split_ratio = float(price['split_ratio'])
                        adj_open = float(price['adj_open'])
                        adj_high = float(price['adj_high'])
                        adj_low = float(price['adj_low'])
                        adj_close = float(price['adj_close'])
                        adj_volume = float(price['adj_volume'])

                        cursor.execute(sql,(ticker,date,open_price, high,low,close_price,volume,ex_dividend,split_ratio,adj_open,adj_high,adj_low,adj_close,adj_volume))
                    connection.commit()
            except:
                  print ticker+" has error here, duplicate"
                  continue
            finally:
                connection.close()

    @staticmethod
    def get_fx_rates(start_day, end_day):
        quandl.ApiConfig.api_key = "kZCJvR1xGx9V6xXoCUN9"
        list=['CUR/JPY','CUR/AUD','CUR/EUR','CUR/CNY','CUR/GBP','CUR/HKD','CUR/CAD','CUR/INR']
        connection = pymysql.connect(host='localhost',
                                     user='root',
                                     password='86511936',
                                     db='trade',
                                     cursorclass=pymysql.cursors.DictCursor)
        res_list=[]
        for rate in list:
            try:
                mydata = quandl.get(rate,start_date=start_day, end_date=end_day)
                res_list.append(pd.DataFrame(mydata))
            except:
                print 'failed to get rates in %s, from %s to %s'%(rate,start_day, end_day)
                continue
        dataframe={}
        for data, name in zip(res_list,list):
            dataframe[name]=data['RATE']

        dataframe['DATE_ID']=pd.DataFrame(res_list[0]).index
        dataframe=pd.DataFrame(dataframe)
        try:
            with connection.cursor() as cursor:
                for index, value in dataframe.iterrows():
                    sql="INSERT INTO trade.trade_FX_Rates(date_id,fx_us_eur,fx_us_cny,fx_us_jpy,fx_us_gbp,fx_us_hko,fx_us_cad,fx_us_aud,fx_us_inr) VALUES(STR_TO_DATE(%s,'%%Y-%%m-%%d'), %s, %s, %s, %s, %s, %s, %s, %s)"
                    date_id=str(value['DATE_ID']).split(' ')[0]
                    fx_us_eur=float(value['CUR/EUR']) if not math.isnan(value['CUR/EUR']) else None
                    fx_us_cny=float(value['CUR/CNY']) if not math.isnan(value['CUR/CNY']) else None
                    fx_us_jpy=float(value['CUR/JPY']) if not math.isnan(value['CUR/JPY']) else None
                    fx_us_gbp=float(value['CUR/GBP']) if not math.isnan(value['CUR/GBP']) else None
                    fx_us_hko=float(value['CUR/HKD']) if not math.isnan(value['CUR/HKD']) else None
                    fx_us_cad=float(value['CUR/CAD']) if not math.isnan(value['CUR/CAD']) else None
                    fx_us_aud=float(value['CUR/AUD']) if not math.isnan(value['CUR/AUD']) else None
                    fx_us_inr=float(value['CUR/INR']) if not math.isnan(value['CUR/INR']) else None
                    cursor.execute(sql,(date_id,fx_us_eur,fx_us_cny,fx_us_jpy,fx_us_gbp,fx_us_hko,fx_us_cad,fx_us_aud,fx_us_inr))
                connection.commit()
        except:
            print 'failed to save rates to table trade_FX_Rates in from %s to %s' % (start_day, end_day)
        finally:
                connection.close()

    @staticmethod
    def insert_eod_today_next_diff(startday,endday):

        connection = pymysql.connect(host='localhost',
                                     user='root',
                                     password='86511936',
                                     db='trade',
                                     cursorclass=pymysql.cursors.DictCursor)
        SQL="SELECT ticker, date_id, adj_close FROM trade.trade_EOD_Prices WHERE date_id >=STR_TO_DATE(%s,'%%Y-%%m-%%d') AND date_id<=STR_TO_DATE(%s,'%%Y-%%m-%%d') order by ticker, date_id "
        cursor=connection.cursor()
        cursor.execute(SQL, (startday, endday))
        res_db = cursor.fetchall()
        data_frame=pd.DataFrame(res_db)
        columns=[]
        for line in cursor.description:
            columns.append(line[0])
        data_frame.columns=columns
        cursor.close()
        print len(data_frame.index)
        SQL2="UPDATE trade.trade_EOD_Prices SET next_today_diff=ROUND(%s,5)*100 WHERE date_id =STR_TO_DATE(%s,'%%Y-%%m-%%d') AND ticker=%s"

        for index,data in data_frame.iterrows():
            try :
                ticker=data[2]
                date=data[1]
                adj_close=data[0]
                if index+1>=len(data_frame.index):
                    break
                next_adj_close=data_frame.loc[index+1,'ticker']
                if next_adj_close is not None:
                    diff=(next_adj_close-adj_close)/adj_close
                    with connection.cursor() as cursor2:
                        cursor2.execute(SQL2,(str(diff),date,ticker))
                print str(index)+'successes'
            except:
                print '%s, %s has errors '%(ticker,date)
                continue
        connection.commit()
        connection.close()

    @staticmethod
    def crawler_save_stock_fundamental_data(url="http://www.stockpup.com/data/", save_dir="/home/eliu/stock_fundamentals/"):
        page = requests.get(url)
        soup = BeautifulSoup(page.content, 'html.parser')
        html= list(soup.children)[2]
        body=list(html.children)[3]
        contents=body.find_all('p')[7:]
        urls_csv=[]
        for content in contents:
            try:
                triple=content.find_all('a')[1]
                href_str=str(triple.get('href'))
                href_str=href_str.replace("/data/", "")
                urls_csv.append(href_str)
            except:
                print 'error'
        print 'Totally : '+str(len(urls_csv))
        testfile = urllib.URLopener()
        for csv in urls_csv:
            testfile.retrieve(url+csv,save_dir+csv)


    @staticmethod
    def save_stock_fundamental_to_db(inputdir="/home/eliu/stock_fundamentals/",mode='append'):
        columns = ["date_id", "Shares", "Shares_split_adjusted", "Split_factor", "Assets",
                   "Current_Assets", "Liabilities", "Current_Liabilities", "Shareholders_equity",
                   "Non_controlling_interest", "Preferred_equity", "Goodwill__intangibles",
                   "Long_term_debt", "Revenue", "Earnings",
                   "Earnings_available_for_common_stockholders", "EPS_basic", "EPS_diluted",
                   "Dividend_per_share", "Cash_from_operating_activities",
                   "Cash_from_investing_activities", "Cash_from_financing_activities",
                   "Cash_change_during_period", "Cash_at_end_of_period", "Capital_expenditures",
                   "Price", "Price_high", "Price_low", "ROE", "ROA", "Book_value_of_equity_per_share",
                   "PB_ratio", "PE_ratio", "Cumulative_dividends_per_share", "Dividend_payout_ratio",
                   "Long_term_debt_to_equity_ratio", "Equity_to_assets_ratio", "Net_margin",
                   "Asset_turnover", "Free_cash_flow_per_share", "Current_ratio"]
        engine = create_engine('mysql://{0}:{1}@{2}:{3}/{4}'.format('root', '86511936', 'localhost', '3306', 'trade'))

        for (dirpath, dirnames, filenames) in walk(inputdir):
            for filename in filenames:
                df = pd.read_csv(inputdir + filename, header=None,names=columns)
                df['ticker'] = pd.Series(filename.split("_")[0], index=df.index)
                df=df[['ticker']+columns]
                df=df.loc[1:,]
                for col in columns[1:]:
                    df[col].replace('None',np.nan,inplace=True)
                try:
                    df.to_sql(con=engine, name='trade_Fundament_Data', if_exists=mode,index=False)
                except :
                    print 'error in '+filename
                    continue
    @staticmethod
    def draw_count_diff():
        connection = pymysql.connect(host='localhost',
                                     user='root',
                                     password='86511936',
                                     db='trade',
                                     cursorclass=pymysql.cursors.DictCursor)
        SQL="select count(*) as counts, round(next_today_diff) as diffs from trade_EOD_Prices where round(next_today_diff)>-30 and round(next_today_diff)<30 group by round(next_today_diff)"
        with connection.cursor() as cursor:
            cursor.execute(SQL)
            res_db=cursor.fetchall()
            data_frame = pd.DataFrame(res_db)
        columns=[]
        for line in cursor.description:
            columns.append(line[0])
        data_frame.columns=columns
        print columns
        data_frame.plot(kind='bar',x='diffs',y='counts',legend=True,xlim=(-30,30))
        plt.show()
    @staticmethod
    def insert_label(startday, endday):
        connection = pymysql.connect(host='localhost',
                                     user='root',
                                     password='86511936',
                                     db='trade',
                                     cursorclass=pymysql.cursors.DictCursor)
        SQL="select ticker, date_id, next_today_diff from trade.trade_EOD_Prices Where date_id >=STR_TO_DATE(%s,'%%Y-%%m-%%d') AND date_id<=STR_TO_DATE(%s,'%%Y-%%m-%%d')"
        SQL2="UPDATE trade.trade_EOD_Prices SET target_label=%s WHERE ticker=%s and date_id=STR_TO_DATE(%s,'%%Y-%%m-%%d')"
        try:
            with connection.cursor() as cursor:
                cursor.execute(SQL,(startday,endday))
                res_db=cursor.fetchall()
                for row in res_db:
                    ticker=row['ticker']
                    date=row['date_id']
                    diff=row['next_today_diff']
                    label=''
                    if diff>=0 and diff<0.5:
                        label='+0'
                    elif diff>=0.4 and diff<1:
                        label='+1'
                    elif diff>=1 and diff<2:
                        label='+2'
                    elif diff>=2.0:
                        label='+3'
                    elif diff <=-2.0:
                        label='-3'
                    elif diff<=-1 and diff >-2.0:
                        label='-2'
                    elif diff<=-0.4 and diff >-1:
                        label='-1'
                    elif diff<0 and diff >-0.4:
                        label='-0'
                    with connection.cursor() as cursor2:
                        cursor2.execute(SQL2,(label,ticker,date))
                connection.commit()
        except:
            print 'errors in update label. '
        finally:
            connection.close()


    @staticmethod
    def SMA(ndays, dataframe):

        if ndays==5:

             name='SMA_5'

        if ndays==15:

             name='SMA_15'

        if ndays==30:

            name='SMA_30'

        if ndays==90:

            name='SMA_90'

        SMA = pd.Series(pd.rolling_mean(dataframe['close_price'], ndays), name=name)

        dataframe=dataframe.join(SMA)

        return dataframe


    @staticmethod
    def get_EOD_Prices(startday, endday,ticker):

        connection = pymysql.connect(host='localhost',
                                     user='root',
                                     password='86511936',
                                     db='trade',
                                     cursorclass=pymysql.cursors.DictCursor)
        SQL="SELECT ticker,date_id,open_price,high,low,close_price,volume,ex_dividend,split_ratio,adj_open,adj_high,adj_low,adj_close,adj_volume, target_label FROM trade.trade_EOD_Prices WHERE date_id >=STR_TO_DATE(%s,'%%Y-%%m-%%d') AND date_id<=STR_TO_DATE(%s,'%%Y-%%m-%%d') AND ticker=%s"

        dataframe=None

        try:
            with connection.cursor() as cursor:

                cursor.execute(SQL,(startday,endday,ticker))

                res_db=cursor.fetchall()

                dataframe = pd.DataFrame(res_db)

        except:

            print 'error in getting dataframe from trade.Eod_prices.'

        finally:

            connection.close()

        return dataframe



    # Exponentially-weighted Moving Average
    @staticmethod
    def EWMA(ndays,data):

        EMA = pd.Series(pd.ewma(data['close_price'], span=ndays, min_periods=ndays - 1), name='EWMA_' + str(ndays))

        data = data.join(EMA)

        return data

    # Rate of Change (ROC)
    @staticmethod
    def ROC(data, n):

        N = data['close_price'].diff(n)

        D = data['close_price'].shift(n)

        ROC = pd.Series(N / D, name='Rate_of_Change_'+str(n))

        data = data.join(ROC)

        return data

    # Compute the Bollinger Bands
    @staticmethod
    def BBANDS(data, ndays):

        MA = pd.Series(pd.rolling_mean(data['close_price'], ndays))

        SD = pd.Series(pd.rolling_std(data['close_price'], ndays))

        b1 = MA + (2 * SD)

        B1 = pd.Series(b1, name='Upper_BollingerBand_'+str(ndays))

        data = data.join(B1)

        b2 = MA - (2 * SD)

        B2 = pd.Series(b2, name='Lower_BollingerBand_'+str(ndays))

        data = data.join(B2)

        return data

    # Commodity Channel Index
    @staticmethod
    def CCI(data, ndays):

        TP = (data['high'] + data['low'] + data['close_price']) / 3

        CCI = pd.Series((TP - pd.rolling_mean(TP, ndays)) / (0.015 * pd.rolling_std(TP, ndays)),name='CCI_'+str(ndays))

        data = data.join(CCI)

        return data

    # Force Index
    @staticmethod
    def ForceIndex(data, ndays):

        FI = pd.Series(data['close_price'].diff(ndays) * data['volume'], name='ForceIndex_'+str(ndays))

        data = data.join(FI)

        return data

    # Ease of Movement
    @staticmethod
    def EVM(data, ndays):

        dm = ((data['high'] + data['low']) / 2) - ((data['high'].shift(1) + data['low'].shift(1)) / 2)

        br = (data['volume'] / 100000000) / ((data['high'] - data['low']))

        EVM = dm / br

        EVM_MA = pd.Series(pd.rolling_mean(EVM, ndays), name='EVM_'+str(ndays))

        data = data.join(EVM_MA)

        return data

class test(object):

    test=Utils()
    quandl.ApiConfig.api_key = "kZCJvR1xGx9V6xXoCUN9"
    #test.save_eod_price(test.sp500_stocks,'2017-06-24','2017-6-30')
    #test.get_fx_rates("1995-01-01","2017-06-23")
    #test.insert_eod_today_next_diff("1990-01-01","1995-01-01")
    #test.crawler_save_stock_fundamental_data()
    #test.save_stock_fundamental_to_db()
    #test.draw_count_diff()
    #test.insert_label('1995-01-01','2017-06-30')

    dataframe=test.get_EOD_Prices('2017-01-01','2017-6-30','FB')

    dataframe=test.SMA(5,dataframe)

    dataframe=test.SMA(15,dataframe)

    dataframe = test.SMA(30, dataframe)

    dataframe = test.EWMA(5, dataframe)

    dataframe = test.EWMA(15, dataframe)

    dataframe = test.EWMA(30, dataframe)

    dataframe=test.ROC(dataframe,5)

    dataframe=test.ROC(dataframe,15)

    dataframe=test.BBANDS(dataframe,5)

    dataframe=test.BBANDS(dataframe,15)

    dataframe=test.CCI(dataframe,5)

    dataframe=test.CCI(dataframe,15)

    dataframe=test.ForceIndex(dataframe,5)

    dataframe=test.ForceIndex(dataframe,14)

    dataframe=test.EVM(dataframe,5)

    dataframe=test.EVM(dataframe,14)

    print dataframe.head(100)

    print dataframe.columns








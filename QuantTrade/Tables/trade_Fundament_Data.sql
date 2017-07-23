Create Table IF NOT EXISTS trade.trade_Fundament_Data(  
	                                                    ticker VARCHAR(10) NOT NULL,
	                                                    date_id DATE NOT NULL,
													    Shares DOUBLE,
														Shares_split_adjusted DOUBLE,
														Split_factor DOUBLE, 
														Assets DOUBLE,
														Current_Assets DOUBLE,
														Liabilities DOUBLE,
														Current_Liabilities DOUBLE,
														Shareholders_equity DOUBLE,
														Non_controlling_interest DOUBLE,
														Preferred_equity DOUBLE,
														Goodwill__intangibles DOUBLE,
														Long_term_debt DOUBLE,
														Revenue DOUBLE,
														Earnings DOUBLE,
														Earnings_available_for_common_stockholders DOUBLE,
														EPS_basic DOUBLE,
														EPS_diluted DOUBLE,
														Dividend_per_share DOUBLE,
														Cash_from_operating_activities DOUBLE,
														Cash_from_investing_activities DOUBLE,
														Cash_from_financing_activities DOUBLE,
														Cash_change_during_period DOUBLE,
														Cash_at_end_of_period DOUBLE,
														Capital_expenditures DOUBLE,
														Price DOUBLE,
														Price_high DOUBLE,
														Price_low DOUBLE,
														ROE DOUBLE,
														ROA DOUBLE,
														Book_value_of_equity_per_share DOUBLE,
														PB_ratio DOUBLE,
														PE_ratio decimal(30,10) default null,
														Cumulative_dividends_per_share DOUBLE,
														Dividend_payout_ratio DOUBLE,
														Long_term_debt_to_equity_ratio DOUBLE,
														Equity_to_assets_ratio DOUBLE,
														Net_margin DOUBLE,
														Asset_turnover DOUBLE,
														Free_cash_flow_per_share DOUBLE,
														Current_ratio DOUBLE,
														CONSTRAINT pk_Fundament_data PRIMARY KEY (ticker,date_id)
													  );
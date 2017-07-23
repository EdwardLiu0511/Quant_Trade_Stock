Create Table IF NOT EXISTS trade.trade_EOD_Prices(
                        ticker  VARCHAR(10) NOT NULL,
                        date_id  DATE NOT NULL,
                         open_price DOUBLE,
                         high    DOUBLE,
                         low     DOUBLE,
                         close_price DOUBLE,
                         volume  DOUBLE,
                         ex_dividend  DOUBLE,
                         split_ratio  DOUBLE,
                         adj_open DOUBLE,
                         adj_high DOUBLE,
                         adj_low DOUBLE,
                         adj_close DOUBLE,
                         adj_volume DOUBLE,
                        CONSTRAINT pk_EOD_Prices PRIMARY KEY (ticker,date_id)
);

CREATE INDEX idx_date_id ON trade.trade_EOD_Prices(date_id);

ALTER TABLE trade.trade_EOD_Prices ADD COLUMN next_today_diff DOUBLE;

ALTER TABLE trade.trade_EOD_Prices ADD COLUMN target_label VARCHAR(3);

###########

ALTER TABLE trade.trade_EOD_Prices ADD COLUMN SMA_5 DOUBLE;

ALTER TABLE trade.trade_EOD_Prices ADD COLUMN SMA_15 DOUBLE;

ALTER TABLE trade.trade_EOD_Prices ADD COLUMN SMA_30 DOUBLE;

ALTER TABLE trade.trade_EOD_Prices ADD COLUMN SMA_90 DOUBLE;

##########


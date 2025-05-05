CREATE TABLE "_blocks" (
  baseFeePerGas bigint null, -- может быть None
  blobGasUsed bigint default null,
  difficulty int4 not null,
  excessBlobGas int4 default null,
  extraData varchar(128),
  gasLimit bigint not null,
  gasUsed bigint not null,
  hash varchar(66) not null unique,
  miner varchar(42) not null,
  mixHash varchar(66) not null,
  nonce bigint not null,
  number bigint primary key unique,
  size int4 not null,
  timestamp int4 not null,
  totalDifficulty numeric not null,
  eth_price numeric default null
);


CREATE TABLE "_transactions" (
  id bigserial primary key,
  blockHash varchar(66) not null,
  blockNumber int4 not null references _blocks(number),
  chainId int4 default null,
  from_ varchar(42) not null,
  gas bigint not null,
  gasPrice bigint not null,
  hash varchar(66) not null unique,
  input text,
  maxFeePerGas bigint default null,
  maxPriorityFeePerGas bigint default null,
  nonce int4 not null,
  r varchar(66) not null,
  s varchar(66) not null,
  to_ varchar(42),
  transactionIndex int4 not null,
  type int4 not null,
  v int4 not null,
  value numeric not null,
  yParity int4 default null,
  -- из tx_receipt
  contractAddress varchar(42),
  cumulativeGasUsed bigint not null,
  gasUsed bigint not null,
  status int4 not null,
  executed_in numeric(8, 3) not null,
  -- из block
  timestamp int4 not null,
  -- block_baseFeePerGas bigint not null,
  -- block_gasUsed bigint not null,
  -- block_gasLimit bigint not null
  bribe_eth numeric default null
);

CREATE TABLE "_logs" (
  transaction_id bigint not null references _transactions(id),
  transactionHash varchar(66) not null,
  transactionIndex int4 not null,
  blockNumber int4 not null,
  timestamp int4 not null,
  address varchar(42) not null,
  topics_0 varchar(66),
  topics_1 varchar(66),
  topics_2 varchar(66),
  topics_3 varchar(66),
  pair_created varchar(42),
  -- data text,
  decoded_data jsonb,
  logIndex int4 not null,
  unique(transaction_id, logIndex)
);


CREATE TABLE "_transfers" (
  -- id serial primary key,
  transaction_id bigint not null references _transactions(id),
  transactionHash varchar(66) not null,
  transactionIndex int4 not null,
  blockNumber int4 not null,
  timestamp int4 not null,
  address varchar(42),
  from_ varchar(42),
  to_ varchar(42),
  value numeric default null,
  value_usd numeric default null,
  value_usd_per_token_unit numeric default null,
  extra_data jsonb, -- {rt, rp}
  category varchar(50),
  logIndex int4 default null,
  status int4 default null,
  pair_address varchar(42),
  token1_currency varchar(42),
  exchange varchar(50),
  total_supply numeric default null,
  reserves_token numeric default null,
  reserves_token_usd numeric default null,
  reserves_pair numeric default null,
  reserves_pair_usd numeric default null,
  token_symbol varchar(20),
  token_decimals int4 default null
);


create index on _transactions(blockNumber);
create index on _transactions(transactionIndex);
create index on _transactions(blockNumber desc, transactionIndex desc);
create index on _transfers(transaction_id);
create index on _transfers(from_);
create index on _transfers(to_);
create index on _transfers(address);
create index on _transfers(blockNumber);
create index on _transfers(transaction_id, address);
create index on _transfers(blockNumber desc, transactionIndex asc);
create index on _transfers(transactionHash);
create index on _logs(transaction_id);
create index on _logs(address);
create index on _logs(topics_0);
create index on _logs(topics_1);
create index on _logs(transaction_id, address);
create index on _logs(blockNumber);
create index on _logs(pair_created);



CREATE TABLE "watch" (
  id serial primary key,
  address varchar(42) not null unique,
  pair varchar(42),
  block_first_mint int4 not null,
  block_first_trade int4 default null,
  mint_transaction varchar(66),
  verified_at_block int4 default null,
  status int2 -- 0/1 watching status
);


CREATE TABLE "watch_predicts" (
  id serial primary key,
  watch_id int4 not null references watch(id),
  block int4 not null,
  http_data jsonb,
  executed_in numeric(8, 3)
);


CREATE TABLE "token_objects" (
  id serial primary key,
  address varchar(42) not null unique,
  block_number int4 not null,
  data bytea -- binary
);


-- база данных нужна, чтобы заносить какой токен сейчас компилируется для LastTrades(), чтобы не накладывать много нагрузки на потоки
CREATE TABLE "last_trades_token_status" (
  address varchar(42) not null unique,
  script varchar(50), -- из какого файла запущено (eth_newHeads или last_trades)
  started timestamp not null default now()
);



CREATE TABLE "trades" (
  id serial primary key,
  address varchar(42) not null,
  pair varchar(42) not null,
  token1_address varchar(42) not null, -- адрес WETH/USDC/USDT пары
  exchange varchar(50) not null,
  trans_block_number int4 not null, -- номер блока в момент создания транзакции
  trans_block_number_timestamp int4 not null,
  trans_value_usd_per_token_unit numeric not null,
  trans_liquidity_rp_usd numeric,
  block_number int4 default null, -- номер блока, подтвержденного в блокчейне
  block_number_timestamp int4 default null,
  value numeric default null, -- в полном формате без деления на token_decimals
  invested_usd numeric default null, -- заполняется при side=b
  withdrew_usd numeric default null, -- заполняется при side=s, сколько денег было получено после сделки
  gas_usd numeric default null,
  realized_profit numeric default null,
  http_data jsonb,
  side varchar(1) not null, -- b/s
  status int2 default null, -- None/0/1 (после None идет запрос в блокчейн для рассчета value, invested_usd, gas_usd)
  hash varchar(66) not null unique,
  strategy varchar(100),
  tag varchar(50),
  backtest int2 default 0
);


CREATE TABLE "token_history_values" (
  id serial primary key,
  address varchar(42) not null,
  pair varchar(42) not null,
  block_number int4 not null,
  value_native_per_token_unit numeric not null,
  value_usd_per_token_unit numeric not null
);


-- статы адресов в token_stat.py
CREATE TABLE "stat_wallets_address" (
  id serial primary key,
  address varchar(42) not null,
  token_address varchar(42) not null,
  last_block int4 not null,
  stat jsonb,
  unique(address, token_address, last_block)
);
create index on stat_wallets_address(token_address);


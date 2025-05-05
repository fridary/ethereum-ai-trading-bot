# Ethereum Meme Coins AI Trading Bot

This project is an AI-powered, real-time trading framework for meme coins and altcoins on Ethereum decentralized exchanges (DEXs) like Uniswap, focusing on the rapidly evolving DeFi ecosystem. Meme coins have evolved from simple joke cryptocurrencies into a significant segment of the decentralized finance (DeFi) ecosystem. Meme coin and altcoin market has exploded with thousands of tokens, often built on the Ethereum blockchain. These tokens typically gain value through community engagement, viral marketing, and speculative trading rather than fundamental utility.

I wrote this system for myself from scratch, so it will not be possible to launch it quickly, since it is in its raw form. I was actively working on this in 2024, and now I have abandoned it, so I think I should post my source codes, because there are many useful utilities and functions for connecting to nodes and working with them, which will save you a lot of programming time, especially indexing the blockchain in a convenient structured form.

The decentralized exchange (DEX) environment has made launching new tokens incredibly accessible, creating opportunities for both legitimate projects and scams. Traders in this space face significant challenges:

- Extreme volatility with price movements of 1000%+ in minutes
- High frequency of new token launches (hundreds daily)
- Complex contract interactions requiring technical knowledge
- Prevalence of scams and rug pulls
- Need for rapid decision-making based on technical and social signals

This fast-paced, high-risk environment has created demand for sophisticated trading tools that leverage artificial intelligence and machine learning to identify opportunities and minimize risks.

## Project Overview

In recent months, meme coins have become a viral trend in the crypto world, generating high volatility and massive interest from both retail and algorithmic traders. Tokens like $PEPE, $DOGE, and $TURBO attract speculative capital and present unique opportunities for algorithmic arbitrage and pattern recognition.

This repository leverages machine learning, predictive analytics, and block-level real-time data from Ethereum nodes to identify profitable trading setups — in particular within the first few minutes of a token's launch or major on-chain activity. Here is AI-powered trading system for Ethereum-based meme coins and altcoins. The system continuously monitors the Ethereum blockchain for new token launches and trading opportunities, using machine learning models to predict potential profitable tokens while filtering out scams.

The framework combines several components:
- 🧠 AI-powered machine learning prediction models (CatBoost-based classifiers)
- 📦 Real-time block processing from Ethereum node (geth/erigon)
- 📈 Liquidity and price anomaly detection
- ⚡ Fast response to token events (Mints, Transfers, Sniper Wallets)
- 🧬 On-chain data indexing into PostgreSQL
- 🔍 Sniper wallet analysis, ROI, and behavioral statistics
- 🛠️ Modular architecture for strategy plug-ins

## System Architecture

### eth_newHeads.py

This is the entry point of the system that establishes a websocket connection to an Ethereum node to receive new block headers in real-time.

Main functionality:
1. Establishes websocket connection to Ethereum node
2. Subscribes to newHeads events (new blocks)
3. Processes each new block by passing it to sync_blocks.py
4. Ensures continuous operation with error handling

Key components:

- **WebSocket Connection**: Establishes a persistent connection to a local Ethereum node using the websockets library.
- **Event Subscription**: Subscribes to the "newHeads" event to receive notifications whenever a new block is added to the blockchain.
- **Thread Management**: Creates separate threads for processing each new block to prevent blocking the main event loop.
- **Error Handling**: Implements robust error recovery to maintain continuous operation.
- **Connection Pooling**: Utilizes a custom non-daemonic process pool to manage multiple concurrent operations.
- **System Integration**: Uses systemd journaling for system-level logging.

The script initializes the environment by:
1. Clearing any stale process states in the database
2. Cleaning log files for fresh start
3. Setting up the processing pool
4. Establishing the websocket connection
5. Starting the continuous processing loop

When a new block header is received, it creates a separate thread to process the block without blocking the main event loop, allowing the system to handle high throughput during periods of network congestion.

### sync_blocks.py

This is the core blockchain indexing system that:

1. Processes new blocks by extracting all transactions
2. Identifies token transfers, contract deployments, and liquidity events
3. Stores structured data in PostgreSQL
4. Identifies potential trading opportunities
5. Implements trading strategies
6. Executes trades when conditions are met

The module contains sophisticated logic for:

- **Transaction Parsing**: Decoding transaction data and event logs
- **Token Analysis**: Extracting token metadata, liquidity information, and trading patterns
- **Machine Learning Integration**: Generating features for prediction models
- **Trading Execution**: Implementing buying and selling logic

## Database Schema

The system uses a PostgreSQL database with the following main tables:

- `_blocks`: Stores block headers and metadata
- `_transactions`: Contains all transactions with detailed parameters
- `_logs`: Stores event logs emitted during transactions
- `_transfers`: Records all token transfers with value and metadata
- `watch`: Tracks tokens of interest for trading
- `trades`: Records executed trades and their performance

For the full schema, see `db.sql`.

## Features

- **Real-time Monitoring**: Processes new blocks within seconds of confirmation
- **AI-Powered Predictions**: Uses machine learning models to predict potential profitable tokens
- **Scam Detection**: Implements multiple heuristics to avoid common scam patterns
- **Multi-strategy Support**: Pluggable trading strategy framework
- **Automated Trading**: Executes trades directly through smart contracts
- **Performance Tracking**: Records and analyzes all trades for continuous improvement

## Trading Strategies

The system supports multiple trading strategies:

1. **Bollinger Bands**: Uses technical indicators for entry and exit points
2. **Machine Learning**: Leverages ML models trained on historical data

These are test strategies to show how lib works. Template's structure I took from [FreqTrade](https://github.com/freqtrade/freqtrade) crypto lib and used it for backtesting (you should modify it to use on DEX instead of CEX).

## Wallet Analysis and Profit Tracking

The `wallet_profit.py` module is a sophisticated tool for analyzing cryptocurrency wallet performance on the Ethereum blockchain. This component focuses on providing comprehensive profit/loss tracking, portfolio analysis, and visualizations for traders to understand their investment performance.

![Ethereum Trading Bot Dashboard](https://raw.githubusercontent.com/fridary/ethereum-ai-trading-bot/refs/heads/main/wallet_profit_0x529902e1bd782D52a5C80e993192056aa7bba247.png)

### Key Features

- **Complete Portfolio Analysis**: Tracks every token in a wallet, including ERC-20 tokens, with detailed metrics on investment performance
- **Real-time Market Data**: Connects to Ethereum nodes to fetch current token prices and liquidity information
- **Profit/Loss Calculation**: Sophisticated algorithms to calculate realized and unrealized profits
- **Investment Metrics**: Computes ROI, days held, investment amounts, and gas costs
- **Liquidity Analysis**: Monitors token liquidity to assess risk and exit potential
- **Transaction History**: Full transaction log with categorization (trades, sends, receives)
- **Performance Visualization**: Tabular visualization of wallet performance with color-coded risk indicators

### How It Works

The `Wallet` class performs several critical functions for the trading ecosystem:

1. **Transaction Retrieval**: Fetches historical transactions from the indexed PostgreSQL database
2. **Token Classification**: Categorizes tokens and their transaction types
3. **Price Calculation**: Determines historical and current token prices via DEX pool data
4. **Profit Calculation**: 
   - Calculates average entry prices per token
   - Computes realized profit from sells
   - Estimates unrealized profit on current holdings
   - Tracks ROI percentages for realized and unrealized positions
5. **Liquidity Assessment**: Monitors token liquidity reserves to evaluate risk
6. **Wallet Balances**: Verifies on-chain balances against calculated values

### Implementation Details

The module integrates several advanced features:

- **Multi-token Support**: Handles any ERC-20 token regardless of decimals or symbol
- **Thread Pooling**: Utilizes concurrent processing for efficient data retrieval
- **Time-based Analysis**: Tracks holding periods and mint-to-trade timeframes
- **Gas Cost Allocation**: Intelligently allocates gas costs to specific trades
- **Risk Indicators**: Color-coded system to highlight potential issues with tokens
- **Statistics Generation**: Computes aggregated portfolio performance metrics
- **DEX Integration**: Reads data from Uniswap V2/V3 and other decentralized exchanges
- **Profit Strategy**: Implements FIFO (First In, First Out) accounting for accurate profit tracking

### Usage in the Trading System

This wallet analysis module serves several purposes in the overall trading ecosystem:

1. **Performance Tracking**: Monitors the performance of AI-driven trading decisions
2. **Risk Assessment**: Identifies high-risk tokens based on liquidity or balance discrepancies
3. **Strategy Refinement**: Provides data for machine learning model optimization
4. **Portfolio Management**: Helps traders understand their overall exposure and performance
5. **Tax Reporting**: Generates transaction data suitable for tax reporting purposes

The tabular output shown in the screenshot provides a comprehensive overview of the wallet's performance, with columns for token value, investment amount, gas costs, balances, liquidity, trade counts, profit metrics, ROI percentages, and holding period.

### Technical Implementation

The module utilizes:
- Web3.py for blockchain interactions
- PostgreSQL for transaction storage
- Tabulate for visualization
- ThreadPool for concurrent processing
- Decimal for precise calculations
- Customized algorithms for market price determination

This component represents a critical part of the overall trading system, providing the performance metrics needed to evaluate the effectiveness of trading strategies and the overall health of the portfolio.

```python
# Example code to include the wallet analysis in your trading bot
from wallet_profit import Wallet

def analyze_performance(wallet_address, last_block=None):
    """
    Analyze the performance of a specific wallet address
    
    Args:
        wallet_address (str): The Ethereum address to analyze
        last_block (int, optional): The block to analyze up to
    
    Returns:
        dict: Performance statistics
    """
    with psycopg2.connect(**db_config) as conn:
        wallet = Wallet(
            testnet=node_url,
            conn=conn, 
            address=wallet_address,
            limit=500,
            last_block=last_block or w3.eth.block_number,
            verbose=0
        )
        wallet.calculate_wallet_profit()
        wallet.calculate_wallet_live()
        
        return wallet.stats
```


## Token Analytics Module

The `token_analytics.py` module provides comprehensive data analysis capabilities for Ethereum tokens, focusing on trader behavior patterns, market dynamics, and on-chain metrics. This component is crucial for understanding market participants and predicting potential price movements based on wallet behaviors.

Example token analytics:
![Ethereum Trading Bot Dashboard](https://raw.githubusercontent.com/fridary/ethereum-ai-trading-bot/refs/heads/main/token_analytics_XYO_1.png)

Top 15 traders by best roi_r (realized return %):
![Ethereum Trading Bot Dashboard](https://raw.githubusercontent.com/fridary/ethereum-ai-trading-bot/refs/heads/main/token_analytics_XYO_2.png)

### Key Features

- **Holistic Token Analysis**: Deep analysis of token holder distributions, transaction patterns, and market behaviors
- **Trader Classification**: Identifies different types of market participants (whales, retail, developers)
- **Fresh Wallet Detection**: Distinguishes between experienced traders and new market entrants
- **Liquidity Analysis**: Tracks liquidity changes over time and per trading participant
- **Transfer Pattern Analysis**: Detects suspicious transfer patterns that might indicate insider activity
- **Wallet Relationship Mapping**: Identifies connected wallets and potential team/developer addresses
- **Profit/Loss Tracking**: Monitors realized and unrealized profits across different wallet groups
- **Trading Behavior Metrics**: Quantifies trading frequency, sizes, and patterns by wallet group

### Advanced Analytics

The module implements several sophisticated analytical approaches:

#### On-Chain Behavior Analysis

The system can categorize token holders into distinct behavioral groups:
- **Whales**: Large position holders with significant market influence
- **Development Team**: Wallets likely belonging to project founders or developers
- **Market Makers**: Addresses showing consistent trading patterns to maintain liquidity
- **Major Investors**: Large initial buyers with strategic holding patterns
- **Retail Traders**: Smaller participants with typical trading behaviors

#### Fresh Wallet Indicators

One of the most powerful signals in meme coin trading is the "fresh wallet" metric - wallets that have never traded ERC-20 tokens before this particular token. The system tracks:

- Percentage of holders that are first-time ERC-20 traders
- Transaction volume from fresh wallets
- Holding patterns of new market entrants
- Correlation between fresh wallet activity and price movements

#### Supply Distribution Tracking

The module analyzes token supply distribution patterns:
- Percentage of supply held by top wallets
- Supply concentration vs. distribution metrics
- Changes in supply distribution over time
- Identification of potential token accumulation phases

#### Wallet Network Analysis

By tracking cross-wallet transfers and relationships:
- Maps networks of related wallets
- Identifies common funding sources
- Detects potential founder/team wallets
- Traces profit extraction patterns

### Strategic Insights

The analytics provided by this module serve several key strategic purposes:

1. **Accumulation Detection**: Identifies when sophisticated traders are quietly accumulating tokens before price increases
2. **Team Wallet Monitoring**: Tracks potential insider selling or accumulation
3. **Market Sentiment Analysis**: Gauges overall holder behavior and sentiment
4. **Risk Assessment**: Evaluates potential rug pull or scam indicators
5. **Exit Signal Detection**: Identifies when key market participants might be exiting positions

### Implementation Details

The module is built using several key components:

- Custom wallet profiling algorithms
- Statistical analysis of trading patterns
- Network analysis for wallet relationships
- Classification models for trader behavior
- Time-series analysis for pattern recognition

### Data Visualization

The module produces detailed tabular reports including:
- Top holders by various metrics (size, profit, trading frequency)
- Wallet statistics with ROI and profit metrics
- Supply distribution charts
- Trading activity timelines
- Wallet network relationship maps

### Practical Applications

By integrating this advanced analytics capability, the trading bot can detect subtle market signals that might indicate upcoming price movements before they become obvious to the broader market.

This component represents one of the most powerful aspects of the trading system, providing the intelligence layer that translates raw blockchain data into actionable trading insights.


## Token State Analysis Module

The `token_stat.py` module is a cornerstone of the trading system, providing comprehensive mathematical modeling of token behavior on the Ethereum blockchain. This critical component processes each new block to create a detailed statistical profile of token metrics, market behavior, and on-chain signals - preparing the essential features for AI model training and prediction.

### Core Functionality

The Token class implements a sophisticated analysis engine that:

1. **Tracks Token Lifecycle Events**: Monitors critical events such as ownership transfers, pair creation, liquidity additions, and trading activity
2. **Maps Holder Distribution**: Calculates precise token distribution across wallets, including percentage of supply held by different stakeholders
3. **Measures Price Impact**: Computes price movements relative to initial pricing and monitors liquidity ratios
4. **Detects Market Manipulation**: Identifies suspicious trading patterns, front-running, and MEV bot activity
5. **Generates AI Training Features**: Creates structured datasets for machine learning models to predict future price movements

### Key Metrics Calculated

The module tracks hundreds of metrics in three main categories:

#### Base Statistics
- Token contract properties and initialization parameters
- Ownership structure and transfer patterns
- Mint timing and transaction positioning
- Gas price metrics relative to block conditions
- Initial liquidity metrics and pair configuration

#### Transaction Analysis
- Price changes from initial value
- Volume patterns by time frames and blocks
- Trade direction ratios (buys vs. sells)
- Transaction positioning in blocks 
- Special wallet interactions (MEV bots, known traders)
- Profit/loss metrics across wallet groups

#### Wallet Analytics
- Holder behavior patterns (buy/sell frequencies)
- Wallet age and transaction histories
- ERC-20 token holding patterns
- External transfer patterns
- Wallet relationship networks

### AI Feature Preparation

For each block, the module prepares feature vectors for machine learning predictions:

```python
# Example prediction feature set structure
self.blocks_data[str(tx_block)] = {
    str(pair_address): {
        'token1_liquidity': balance_token1,
        'value_native_per_token_unit': value_native_per_token_unit,
        'value_usd_per_token_unit': value_usd_per_token_unit,
        'price_change': price_change_from_initial
    },
    'timestamp': block_timestamp,
    'traded_volume_buy': self.traded_volume_buy,
    'traded_volume_sell': self.traded_volume_sell,
    'trades_buy': self.trades_buy,
    'trades_sell': self.trades_sell,
    'transactions_count': len(self.transactions),
    'prediction': None,  # To be filled by AI model
    'prediction_all': None,  # Aggregate prediction
}
```

### Mathematical Models

The system implements several mathematical methods for market analysis:

1. **Liquidity Ratio Tracking**: Monitors the ratio of token reserves to paired currency reserves to detect potential rug pulls or liquidity drain attempts
2. **Profit Distribution Analysis**: Maps realized and unrealized profit across wallet categories to identify accumulation patterns
3. **Time-Series Feature Engineering**: Creates time-based patterns of transaction volume, frequency, and price movement
4. **Ownership Concentration Metrics**: Calculates Gini-inspired coefficients to measure token distribution equality
5. **Gas Price Analysis**: Correlates gas price strategies with token price movements to detect sophisticated trading strategies

### Integration with Blockchain

The module performs real-time blockchain queries for each analysis:

- **Contract State Reading**: Reads token balances and supply metrics
- **Event Log Processing**: Decodes transfer events, liquidity additions, and pair interactions
- **Token Price Calculation**: Calculates accurate token prices based on DEX pool reserves
- **Wallet Profiling**: Builds behavioral profiles of trading wallets based on historical actions

### Database Integration

The system maintains a comprehensive blockchain state in PostgreSQL:

- Token transaction history with detailed metadata
- Statistical profiles of wallets interacting with the token
- Historical price and liquidity data with millisecond precision

### Real-time Application

For every new Ethereum block, this module:

1. Updates token state and distribution metrics
2. Recalculates price points and liquidity parameters 
3. Adjusts wallet ROI and profit metrics
4. Prepares feature vectors for AI prediction
5. Identifies significant changes in market structure

This module serves as the mathematical foundation of the trading system, transforming raw blockchain data into structured information that enables the AI models to identify profitable trading opportunities in the high-velocity meme coin market.


## AI-Powered Prediction Models: analyse_30m.py and analyse_30m_x2.py

These modules form the core of the AI prediction system, implementing sophisticated machine learning models for meme coin and altcoin trading on Ethereum-based DEXs. They work together to analyze blockchain data, extract meaningful features, and predict potential price movements.

### analyse_30m.py

This module implements the primary feature engineering and prediction framework, focusing on extracting signals from on-chain data:

1. **Feature Engineering**: Converts raw blockchain data into structured features suitable for machine learning models:
   - Wallet behavior analysis (timing, frequency, volume)
   - Liquidity pool metrics and changes
   - Price action patterns
   - Technical indicators (Bollinger Bands, MACD, ADX)
   - Sniper wallet profiling and classification

2. **Sniper Analysis**: Implements comprehensive categorization of "sniper" wallets (early traders):
   - Track wallet profitability (ROI metrics, realized vs unrealized profit)
   - Profile trading patterns (buy/sell behavior, holding duration)
   - Analyze repeated investment behavior
   - Detect wallet relationships and potential insider activity
   - Calculate distribution and concentration metrics

3. **Technical Analysis**: Implements multiple trading indicators:
   - EMA (Exponential Moving Averages) across multiple timeframes
   - RSI (Relative Strength Index) calculations
   - MACD (Moving Average Convergence Divergence)
   - Bollinger Bands for volatility measurement
   - ADX (Average Directional Index) for trend strength

4. **CatBoost Integration**: Leverages the CatBoost gradient boosting framework for multi-output classification:
   - Predicts multiple target variables simultaneously
   - Handles categorical features automatically
   - Implements training/testing split methodology
   - Provides feature importance analysis
   - Optimizes for early stopping to prevent overfitting

5. **Statistical Profiling**: Generates exhaustive statistical profiles for each token:
   - Genesis trader analysis (first buyers)
   - Transaction timing relative to token launch
   - Liquidity removal detection
   - MEV bot interaction patterns
   - Gas price and transaction position analysis

### analyse_30m_x2.py

This module extends the core prediction system with advanced temporal analysis and multi-timeframe predictions:

1. **Multi-Timeframe Prediction**: Implements models for various time horizons:
   - Short-term (5m, 15m, 30m)
   - Medium-term (1h, 4h, 8h, 16h)
   - Long-term (2d, 5d, infinity)
   - Liquidity predictions (1h, 16h)

2. **Parallel Model Training**: Utilizes multiprocessing to simultaneously train multiple specialized models:
   - Trains dedicated models for each timeframe
   - Implements Pool-based parallelism for efficiency
   - Balances computational resources across models
   - Ensures consistent cross-validation methodology

3. **Feature Importance Analysis**: Conducts thorough analysis of predictive features:
   - Exports feature importance metrics to CSV files
   - Identifies the most powerful predictors for each timeframe
   - Dynamically ranks features by predictive power
   - Supports feature selection for model optimization

4. **Dataset Management**: Implements sophisticated data preparation functions:
   - Creates time-sliced datasets based on block timestamps
   - Filters data based on trade count thresholds
   - Handles missing values and outliers
   - Implements consistent train/test splitting

5. **Model Persistence**: Manages model serialization and versioning:
   - Saves optimized models with metadata
   - Implements standardized naming conventions
   - Supports model reloading for inference
   - Creates prediction pipelines for real-time trading

Together, these modules provide a comprehensive machine learning pipeline for predicting meme coin and altcoin performance across multiple timeframes. The system is designed to identify profitable trading opportunities while filtering out potential scams, providing actionable intelligence for DeFi traders in the highly volatile altcoin market.


## Data Collection and Token Classification System: collect.py

This module serves as the data harvesting and classification component of the trading framework, extracting valuable insights from the indexed PostgreSQL database to identify promising trading opportunities. The system processes blockchain data to categorize tokens based on specific performance metrics, creating curated lists for machine learning model training.

### Key Functionality

1. **Token Data Extraction**: Queries the indexed PostgreSQL database to retrieve transaction histories, price movements, and liquidity metrics for tokens created within a specific block range.

2. **Performance Analysis**: For each token, calculates critical metrics:
   - Price movement percentages at various timeframes (10m, 1h, 6h, 16h, etc.)
   - Liquidity evolution over time
   - Maximum price changes relative to initial pricing
   - Trade count and transaction volume

3. **Multi-criteria Classification**: Implements sophisticated filtering logic to categorize tokens into distinct groups:
   - High-profit tokens (achieved significant returns while maintaining liquidity)
   - Non-scam tokens (passed security checks and maintained minimum liquidity)
   - Scam tokens (failed security checks or showed suspicious patterns)
   - Low-volume tokens (insufficient trading activity for reliable analysis)

4. **External Security Validation**: Integrates with third-party security tools to enhance classification accuracy:
   - GoPlus Labs API for contract risk assessment
   - Honeypot detection services to identify malicious contracts
   - ABI verification to ensure contract functionality

5. **Data Export**: Saves categorized token lists as structured files for machine learning model training:
   - JSON files with token addresses grouped by category
   - CSV files with detailed metrics for feature engineering
   - Consolidated datasets for supervised learning with target columns

### Implementation Details

The module employs parallel processing to efficiently analyze thousands of tokens, using a connection pool to manage database queries across multiple worker processes. It implements progressive data collection with detailed progress tracking and implements sophisticated error handling to ensure reliability during long-running analysis sessions.

The classification logic has evolved through multiple versions (as evidenced by the commented code blocks), showing the iterative refinement of the token filtering criteria based on observed market behaviors and performance results.

This component forms a critical part of the trading system's intelligence layer, transforming raw blockchain data into actionable token categories that drive the machine learning prediction models and trading strategies.
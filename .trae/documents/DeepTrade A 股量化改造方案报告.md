将 DeepTrade 改造为 A 股量化交易机制需要针对市场机制、数据源和交易执行进行深度适配。以下是具体的改造方案：

## **1. 市场机制适配 (Core Logic)**
A 股与加密货币期货存在本质差异，需要修改 [quantitative_trading.go](file:///Users/zhangyang/Desktop/freedom/deeptrade/task/quantitative_trading.go) 中的核心逻辑：
- **T+1 制度**: 在 `PositionInfo` 中引入 `AvailableQty`（可用余额），区分“当前持仓”与“可卖持仓”。当天买入的股票需在 `memory` 中标记，次日才允许执行 `CLOSE` 操作。
- **交易时间限制**: 修改 `IsWork()` 函数，适配 A 股交易时间（周一至周五 9:30-11:30, 13:00-15:00），并处理节假日休市逻辑。
- **涨跌停处理**: 在 `MarketData` 中加入当日涨跌停价，防止 Agent 在涨停时无效买入或跌停时无效卖出。

## **2. 数据源改造 (Data Provider)**
由于 A 股无法通过币安 API 获取，需要重写 [market_data.go](file:///Users/zhangyang/Desktop/freedom/deeptrade/task/market_data.go)：
- **接口替换**: 引入 A 股行情接口（如 AkShare, Baostock, 或券商提供的 QMT/PTrade 接口）。
- **指标调整**: 移除资金费率（Funding Rate），替换为 A 股特有的**换手率、涨跌分布、北向资金、板块流向**等数据。
- **K 线频率**: A 股常用 1/5/15/60 分钟及日线数据，需调整 [technical_analysis.go](file:///Users/zhangyang/Desktop/freedom/deeptrade/task/technical_analysis.go) 的采样频率。

## **3. 交易执行层重构 (Execution)**
修改 [execution.go](file:///Users/zhangyang/Desktop/freedom/deeptrade/task/execution.go) 以适配股票交易：
- **订单单位**: 下单数量必须是 100 股（1 手）的整数倍。
- **移除杠杆与做空**: 除非使用融资融券，否则需移除 `Leverage` 逻辑，并将 `OPEN_SHORT` 等动作禁用或改为“融券卖出”。
- **实盘对接**: 通过封装好的 Python 桥接（如 EasyTrader）或券商官方 SDK 执行下单命令。

## **4. AI 提示词与决策优化 (LLM Prompt)**
更新 [utility.go](file:///Users/zhangyang/Desktop/freedom/deeptrade/utils/utility.go) 中的 `roleMsg`：
- **角色定位**: 将“期货分析师”改为“A 股量化策略专家”。
- **风险控制**: 告知 LLM A 股的 T+1 限制，使其在决策时更倾向于中短期波段而非超高频交易。
- **数据输入**: 将提示词中的 `binance` 数据项替换为 A 股财务报表摘要、所属行业板块、个股研报评分等文本信息。

## **5. 架构演进建议**
- **抽象化接口**: 定义 `Exchange` 接口，包含 `GetMarketData()` 和 `PlaceOrder()`，实现一套代码同时支持加密货币与 A 股。
- **回测验证**: A 股回测至关重要，建议先在 [REPORT.md](file:///Users/zhangyang/Desktop/freedom/deeptrade/REPORT.md) 记录的逻辑基础上，使用历史行情进行模拟运行。

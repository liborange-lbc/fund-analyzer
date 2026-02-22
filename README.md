# fund-analyzer

指数分析与策略回测工具，包含数据管理、回测、实盘策略与监控看板。

## 功能概览
- 指数趋势分析：风险画像与相关性矩阵
- 策略回测：定投、智能定投、均值回归、趋势跟随、均值差
- 实盘策略：联系人邮件订阅通知
- 监控看板：当日变化、风险预警、策略待处理事项
- 数据管理：指数与联系人表的 CRUD

## 技术栈
- FastAPI + SQLAlchemy + SQLite
- Pandas 数据处理
- ECharts + Tailwind CSS 前端

## 本地开发
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn fund_analyzer.main:app --host 0.0.0.0 --port 8000 --reload
```

浏览器访问：`http://localhost:8000`

## Docker 部署
```bash
docker build -t fund-analyzer .
docker run -d --name fund-analyzer -p 8000:8000 --restart=always fund-analyzer
```

SQLite 持久化：
```bash
docker run -d --name fund-analyzer \
  -p 8000:8000 \
  -e DATABASE_URL=sqlite:////app/data/fund_analyzer.db \
  -v /data/fund-analyzer:/app/data \
  --restart=always \
  fund-analyzer
```

## 环境变量（可选）
- `DATABASE_URL`：数据库连接串（默认 SQLite）
- `FRED_API_KEY`：FRED API Key（美股指数）
- `ALPHA_VANTAGE_API_KEY`：Alpha Vantage API Key
- `WECHAT_APPID`, `WECHAT_APPSECRET`：微信通知配置
- `SCHEDULE_HOUR`, `SCHEDULE_MINUTE`：每日任务时间

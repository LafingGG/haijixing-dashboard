# 海吉星果蔬项目 · 本地运营看板（Python + Streamlit）

这个项目会把你提供的 Excel（月度运营表）导入到本地 SQLite，然后用 Streamlit 做一个本地“驾驶舱”页面展示。

## 1) 环境准备
```bash
cd haijixing_dashboard
python -m venv .venv
# mac/linux
source .venv/bin/activate
# windows
# .venv\Scripts\activate

pip install -r requirements.txt
```

## 2) 导入 Excel 到本地数据库
把你的 Excel 放在任意位置，然后运行：
```bash
python -m etl.load_to_db "你的excel路径.xlsx"
```

导入成功后会生成/更新：`db/ops.sqlite`

## 3) 运行看板
```bash
streamlit run app.py
```

## 4) 你后续最常改的地方
- 字段映射/清洗：`etl/parse_excel.py`
- 图表与页面：`pages/`
- 首页：`pages/1_总览.py`

> 说明：电表读数在你的表里是“抄表点”（并非每天都有），页面里默认同时展示“电表读数曲线”和“抄表区间用电量（差分）”。

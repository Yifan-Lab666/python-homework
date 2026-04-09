      
import os
import logging
import pandas as pd
import re
import requests
from pyhive import presto
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
import config

# 配置 logging：输出到屏幕和 task.log 文件
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),  # 输出到屏幕
        logging.FileHandler(r'C:\Users\Admin\Desktop\task.log', encoding='utf-8')  # 输出到 task.log 文件
    ]
)

# ==================== 配置区 ====================
# 数据库连接配置（从环境变量读取）
DB_HOST = os.environ.get('DB_HOST', 'presto.infra.bi.moontontech.net')
DB_PORT = int(os.environ.get('DB_PORT', '80'))
DB_USERNAME = os.environ.get('DB_USERNAME', 'default_user')
DB_PASSWORD = os.environ.get('DB_PASSWORD', '')

# 英雄配置 CSV 文件路径
CONFIG_CSV_PATH = os.environ.get('CONFIG_CSV_PATH', 'hero.csv')

# 数据查询日期
DATE = os.environ.get('DATE', '2026-03-25')

# SQL 查询模板
SQL_TEMPLATE = r"""
WITH battle_data AS (
    SELECT  a.logymd,
            a.heroid,
            a.roadsort
    FROM    (
                SELECT  logymd,
                        battleid,
                        accountid,
                        zoneid,
                        heroid,
                        roadsort
                FROM    ml_ods.connserver_battleenddata
                WHERE   logymd = '{DATE}'
                AND     zoneid < 57000
                AND     battletype = 2
                AND     roadsort IN (1, 2, 3, 4, 5, 101, 105, 3000, 3001, 3002, 3003, 3004, 3005, 3101, 3105)
                AND     minranklevel >= 16
                AND     realpvptype IN (1, 2, 59, 90)
                AND     NOT EXISTS (
                            SELECT  1
                            FROM    ml_ods.battleserver_battle_end b
                            WHERE   b.logymd = '{DATE}'
                            AND     b.fake_ai != 0
                            AND     ml_ods.connserver_battleenddata.accountid = b.accountid
                            AND     ml_ods.connserver_battleenddata.zoneid = b.zoneid
                        )
            ) a
    INNER JOIN
            (
                SELECT  battleid
                FROM    ml_ods.battleserver_battle_end
                WHERE   logymd = '{DATE}'
                AND     zoneid < 57000
                AND     pvptype IN (2)
                AND     isinvalidbattle = 0
                GROUP BY battleid
                HAVING  COUNT(DISTINCT accountid) = 10
                        AND COUNT(DISTINCT CASE WHEN fake_ai = 0 THEN accountid END) = 10
                        AND COUNT(DISTINCT heroid) = 10
            ) b
    ON      a.battleid = b.battleid
)

SELECT  logymd,
        heroid,
        ROUND(COUNT(CASE WHEN roadsort IN (1, 5) THEN 1 END) * 1.0 / COUNT(1), 4) AS gold_lane_pct,
        ROUND(COUNT(CASE WHEN roadsort = 2 THEN 1 END) * 1.0 / COUNT(1), 4) AS mid_lane_pct,
        ROUND(COUNT(CASE WHEN roadsort IN (3, 3000, 3001, 3002, 3003, 3004, 3005, 3101, 3105) THEN 1 END) * 1.0 / COUNT(1), 4) AS roam_lane_pct,
        ROUND(COUNT(CASE WHEN roadsort = 4 THEN 1 END) * 1.0 / COUNT(1), 4) AS jungle_lane_pct,
        ROUND(COUNT(CASE WHEN roadsort IN (101, 105) THEN 1 END) * 1.0 / COUNT(1), 4) AS exp_lane_pct
FROM    battle_data
GROUP BY logymd, heroid
HAVING  COUNT(1) >= 10
ORDER BY heroid
LIMIT 50000
""".format(DATE=DATE)
# ==================== 函数定义 ====================

def read_sql_connection_string():
    """
    从环境变量读取数据库连接信息
    返回包含 host, port, user, password 的字典
    """
    return {
        "host": DB_HOST,
        "port": DB_PORT,
        "user": DB_USERNAME,
        "password": DB_PASSWORD
    }



def read_config_csv(csv_path):
    """
    读取英雄配置 CSV 文件
    返回包含 heroid, hero_name, config_roadsort 的 DataFrame
    """
    encodings = ['utf-8-sig', 'utf-8', 'utf-16', 'gbk', 'cp1252']
    df = None
    for enc in encodings:
        try:
            df = pd.read_csv(csv_path, encoding=enc, sep='\t', header=None)
            df = df.iloc[3:]  # 跳过前三行
            df = df.iloc[:, :62]  # 只取前62列
            df.columns = [f'col{i}' for i in range(len(df.columns))]
            df = df.rename(columns={'col0': 'ID', 'col1': 'mName', 'col59': 'RoadSort'})
            logging.info(f"✅ 配置 CSV 编码：{enc}")
            break
        except Exception as e:
            logging.warning(f"尝试编码 {enc} 失败: {e}")
            continue
    
    if df is None:
        raise Exception(f"无法读取 CSV 文件，尝试了所有编码格式")
    
    if 'ID' not in df.columns or 'mName' not in df.columns or 'RoadSort' not in df.columns:
        logging.error(f'Available columns: {list(df.columns)}')
        raise Exception('hero.csv 需要包含 ID、mName、RoadSort 三列')
    
    config_df = df[['ID', 'mName', 'RoadSort']].copy()
    config_df.columns = ['heroid', 'hero_name', 'config_roadsort']
    config_df['heroid'] = config_df['heroid'].astype(int)
    config_df['hero_name'] = config_df['hero_name'].astype(str)
    config_df['config_roadsort'] = config_df['config_roadsort'].astype(str)
    return config_df


def read_online_data(sql):
    """
    从 Presto 数据库读取线上英雄分路数据
    返回包含分路占比的 DataFrame
    """
    try:
        conn = presto.connect(
            host=DB_HOST,
            port=DB_PORT,
            username=DB_USERNAME
        )
        df = pd.read_sql(sql, conn, coerce_float=False)
        conn.close()
        return df
    except Exception as e:
        logging.error(f"❌ Presto 连接失败：{e}")
        raise


def get_feishu_webhook_url():
    """
    获取飞书 webhook URL
    优先使用配置区设置，其次环境变量
    """
    url = config.FEISHU_WEBHOOK_URL.strip()
    if url:
        return url
    return os.environ.get("FEISHU_WEBHOOK_URL", "").strip()


def send_feishu_webhook(text, webhook_url):
    """
    通过飞书 webhook 发送消息
    返回是否发送成功
    """
    if not webhook_url:
        logging.warning("⚠️ 未配置飞书 webhook URL，跳过推送")
        return False
    payload = {
        "msg_type": "text",
        "content": {
            "text": text
        }
    }
    try:
        resp = requests.post(webhook_url, json=payload, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data.get("errcode") == 0 or data.get("code") == 0 or data.get("StatusCode") == 0:
                logging.info("✅ 已通过飞书 webhook 发送结果")
                return True
            else:
                logging.error(f"❌ 飞书 webhook 请求失败: {data}")
                return False
        else:
            logging.error(f"❌ 飞书 webhook HTTP失败: {resp.status_code} {resp.text}")
            return False
    except Exception as e:
        logging.error(f"❌ 飞书 webhook 发送异常: {e}")
        return False


def parse_config_roadsort(road_sort_str):
    """
    解析配置中的 RoadSort 字符串
    返回主分路和副分路名称
    """
    mapping = {
        '1': '经验路',
        '2': '中路',
        '3': '游走',
        '4': '打野',
        '5': '金币路'
    }
    rs = str(road_sort_str).strip().strip(';')
    if rs == '' or rs.lower() in ['nan', 'none']:
        return None, None

    parts = [p.strip() for p in rs.split(';') if p.strip()]
    routes = [mapping.get(p, None) for p in parts if p and mapping.get(p)]
    if not routes:
        return None, None

    primary = routes[0]
    secondary = routes[1] if len(routes) >= 2 else None
    return primary, secondary


def detect_main_secondary(row):
    """
    根据分路占比计算线上主副分路
    返回主分路、副分路和分路排名
    """
    lanes = {
        "金币路": row.get("gold_lane_pct", 0),
        "中路": row.get("mid_lane_pct", 0),
        "游走": row.get("roam_lane_pct", 0),
        "打野": row.get("jungle_lane_pct", 0),
        "经验路": row.get("exp_lane_pct", 0)
    }
    sorted_lanes = sorted(lanes.items(), key=lambda x: x[1], reverse=True)
    primary = sorted_lanes[0][0]
    secondary = sorted_lanes[1][0] if sorted_lanes[1][1] > 0.30 else None
    return primary, secondary, sorted_lanes


def compare_config(actual_primary, actual_secondary, config_primary, config_secondary):
    """
    对比线上和配置的分路
    返回是否不一致和原因描述
    """
    mismatch = False
    reasons = []
    if actual_primary != config_primary:
        mismatch = True
        reasons.append(f"主路({actual_primary} != {config_primary})")
    if (actual_secondary or "") != (config_secondary or ""):
        mismatch = True
        reasons.append(f"副路({actual_secondary or '无'} != {config_secondary or '无'})")
    return mismatch, "|".join(reasons)


def main():
    logging.info("正在读取配置...")
    config_df = read_config_csv(CONFIG_CSV_PATH)

    logging.info("正在执行 SQL 查询...")
    online_df = read_online_data(SQL_TEMPLATE)

    logging.info(f"✅ 读取在线数据 {len(online_df)} 条")

    online_df["heroid"] = online_df["heroid"].astype(int)
    merged = online_df.merge(config_df, on="heroid", how="left")

    # 计算主/副分路
    info = merged.apply(
        lambda r: pd.Series(detect_main_secondary(r), index=["actual_primary", "actual_secondary", "lane_rank"]),
        axis=1
    )
    merged = pd.concat([merged, info], axis=1)

    # 解析配置主副分路
    cfg = merged["config_roadsort"].apply(parse_config_roadsort)
    merged[["config_primary_route", "config_secondary_route"]] = pd.DataFrame(cfg.tolist(), index=merged.index)

    # 对比
    cmp = merged.apply(
        lambda r: pd.Series(
            compare_config(r.actual_primary, r.actual_secondary, r.config_primary_route, r.config_secondary_route),
            index=["mismatch", "reason"]
        ),
        axis=1
    )
    merged = pd.concat([merged, cmp], axis=1)

    # 筛选不一致的记录
    diff = merged[merged["mismatch"] == True].copy()

    # 生成清晰的输出
    logging.info("\n" + "=" * 80)
    logging.info("【配置和线上数据不符的英雄】")
    logging.info("=" * 80)
    
    if diff.empty:
        logging.info("✅ 无不符合的英雄，配置和线上数据完全一致！")
    else:
        for idx, row in diff.iterrows():
            logging.info(f"\n英雄ID：{row['heroid']}")
            logging.info(f"英雄名字：{row.get('hero_name', '未知')}")
            logging.info(f"  线上主分路：{row['actual_primary']} | 线上副分路：{row['actual_secondary'] or '无'}")
            logging.info(f"  线上占比：金币{row.get('gold_lane_pct', 0):.2%} 中路{row.get('mid_lane_pct', 0):.2%} 游走{row.get('roam_lane_pct', 0):.2%} 打野{row.get('jungle_lane_pct', 0):.2%} 经验{row.get('exp_lane_pct', 0):.2%}")
            logging.info(f"  配置主分路：{row.get('config_primary_route', '无')} | 配置副分路：{row.get('config_secondary_route', '无') or '无'}")
            logging.info(f"  配置RoadSort：{row.get('config_roadsort', '')}")
            logging.info(f"  对比原因：{row.get('reason', '')}")
            logging.info("-" * 80)

    # 保存到 CSV（给你备份用）
    keep_cols = ["heroid", "hero_name", "config_roadsort", "config_primary_route", "config_secondary_route",
                 "actual_primary", "actual_secondary", "gold_lane_pct", "mid_lane_pct", "roam_lane_pct", "jungle_lane_pct", "exp_lane_pct", "reason"]
    diff_export = diff[keep_cols]
    output_file = "hero_route_diff_mismatch.csv"
    try:
        diff_export.to_csv(output_file, index=False, encoding="utf-8-sig")
    except PermissionError:
        # 如果文件被占用，改为时间戳文件名
        from datetime import datetime
        output_file = f"hero_route_diff_mismatch_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        diff_export.to_csv(output_file, index=False, encoding="utf-8-sig")
    logging.info(f"\n结果已保存到 {output_file}")
    logging.info(f"总计：{len(diff)} 个英雄主/副分路不一致\n")

    # 飞书通知
    webhook_url = get_feishu_webhook_url()
    lines = [
        "英雄分路对比结果：",
        f"总计：{len(diff)} 个英雄主/副分路不一致",
        ""  # 空行
    ]

    if diff.empty:
        lines.append("✅ 无不符合的英雄，配置和线上数据完全一致！")
    else:
        separator = "--------------------------------------------------------------------------------"
        for i, row in diff.head(20).iterrows():
            lines.append(f"英雄ID：{row['heroid']}")
            lines.append(f"英雄名字：{row.get('hero_name', '未知')}")
            lines.append(f"  线上主分路：{row['actual_primary']} | 线上副分路：{row['actual_secondary'] or '无'}")
            lines.append(f"  线上占比：金币{row.get('gold_lane_pct', 0):.2%} 中路{row.get('mid_lane_pct', 0):.2%} 游走{row.get('roam_lane_pct', 0):.2%} 打野{row.get('jungle_lane_pct', 0):.2%} 经验{row.get('exp_lane_pct', 0):.2%}")
            lines.append(f"  配置主分路：{row.get('config_primary_route', '无')} | 配置副分路：{row.get('config_secondary_route', '无') or '无'}")
            lines.append(f"  配置RoadSort：{row.get('config_roadsort', '')}")
            lines.append(f"  对比原因：{row.get('reason', '')}")
            lines.append(separator)

        if len(diff) > 20:
            lines.append(f"... 共{len(diff)}条，飞书消息仅展示前20条(避免超长)")

    text = "\n".join(lines)
    send_feishu_webhook(text, webhook_url)


def start_scheduler():
    """
    启动定时任务调度器
    设置每周一早上 10:30 执行 main() 函数
    启动时立即执行一次
    """
    # 启动时立即执行一次
    logging.info("🚀 脚本启动：立即执行一次英雄分路对比任务")
    main()

    # 创建调度器
    scheduler = BlockingScheduler()

    # 添加定时任务：每周一早上 10:30 执行
    scheduler.add_job(
        func=main,
        trigger=CronTrigger(day_of_week='mon', hour=10, minute=30),
        id='hero_route_check',
        name='英雄分路对比检查',
        max_instances=1  # 确保同时只运行一个实例
    )

    logging.info("⏰ 定时任务已启动：每周一早上 10:30 执行英雄分路对比检查")
    logging.info("📝 按 Ctrl+C 可以停止定时任务")

    try:
        scheduler.start()
    except KeyboardInterrupt:
        logging.info("⏹️ 定时任务已停止")
        scheduler.shutdown()


if __name__ == "__main__":
    # 启动定时任务调度器
    start_scheduler()

    
"""Built-in Bilibili gift catalog. Values are battery units (10 batteries = CNY 1)."""
from __future__ import annotations

if __package__:
    from .queue_rank_query import install_queue_rank_query_hook
else:
    from queue_rank_query import install_queue_rank_query_hook

# server.py imports this catalog before QueueManager is defined. Install the
# read-only query interface for both package imports and direct script runs.
install_queue_rank_query_hook()


GIFT_BATTERIES: dict[str, int | None] = {
    "足迹": 1, "人气票": 1, "山市晴岚": 660, "心动盲盒": 150, "粉丝团灯牌": 1,
    "舰长一号": 1980, "小花花": 1, "你真好看": 10, "开球": 1, "星愿水晶球": 1000,
    "牛哇牛哇": 1, "情书": 52, "退钱": 99, "星落入海": 6660, "发红包": None,
    "法兰西之剑": 690, "大英帝星": 690, "斗牛士军团": 690, "阿根廷球王": 690,
    "幸运盲盒": 50, "半场开香槟": 300, "bilibili星跃": 10000, "水晶鞋": 99,
    "撒花": 99, "私人飞机": 1000, "巨蟹娃娃": 1990, "落日飞车": 2000,
    "梦幻邮轮": 3000, "bilibili世界": 30000, "喜欢你": 99, "花式夸夸": 299,
    "心动时刻": 50, "旋转木马": 520, "飞屋环游": 5000, "打call": 2, "比心": 10,
    "甜滋滋": 50, "小电视飞船": 29999, "666": 10, "音乐盒": 99, "灿烂烟花": 520,
    "次元之城": 12450, "鼓鼓掌": 5, "告白花束": 199, "千纸鹤": 52,
    "傲娇的小猫": 99, "钻石戒指": 199, "梦游仙境": 3000, "为你摘星": 5200,
    "冰晶吊坠": 299, "送花花": 10, "月桂皇冠": 4000, "原地求婚": 5200,
    "极速超跑": 1000, "爱的乐章": 1990, "告白气球": 2000, "捏捏小脸": 99,
    "流星雨": 299, "星轨列车": 6666, "探索者启航": 22330, "梦幻游乐园": 30000,
    "心动卡": 1, "专属灯牌": 50, "泡泡机": 50, "爱之魔力": 280, "摩天轮": 1000,
    "转运锦鲤": 6660, "鎏金小电视": 29990, "领航者飞船": 12450, "干杯之旅": 100,
    "启航之旅": 1000, "提督一号": 19980, "总督一号": 199980, "友谊的小船": 49,
    "冲浪": 899, "海湾之旅": 7999, "鸿运小电视": 10000,
}


def batteries_for_gift(name: str, count: int = 1) -> int | None:
    value = GIFT_BATTERIES.get(str(name or "").strip())
    return None if value is None else value * max(1, int(count))

"""Bilibili Danmuji 初始领域模型。

基于《Bilibili_Danmuji_流程与接口整理 (1).docx》抽取：
- 架构分层
- 主流程阶段
- 事件路由
- 延时聚合策略
- 外部接口清单

目标：提供可序列化、可扩展的“初始模型”，便于后续生成代码/配置。
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any
import json


class Layer(str, Enum):
    ACCESS = "接入层"
    PROTOCOL = "协议层"
    DISPATCH = "业务分发层"
    RULE_AGGREGATION = "规则与聚合层"
    SENDING = "发送层"
    LOCAL_FRONTEND = "本地前端层"


class EndpointTier(str, Enum):
    CORE = "主流程必经"
    RUNTIME = "高频运行期"
    OPTIONAL = "可选扩展"


@dataclass(slots=True)
class Endpoint:
    name: str
    method: str
    url: str
    purpose: str
    tier: EndpointTier


@dataclass(slots=True)
class MessageRoute:
    cmd: str
    normalized_cmd: str
    handlers: list[str]
    output_queue: str | None = None


@dataclass(slots=True)
class AggregationModel:
    name: str
    container: str
    key_strategy: str
    window_strategy: str
    output_template_modes: list[str] = field(default_factory=list)


@dataclass(slots=True)
class ThreadModel:
    name: str
    trigger: str
    responsibility: str


@dataclass(slots=True)
class DanmujiInitialModel:
    model_name: str
    source_document: str
    layers: dict[Layer, list[str]]
    startup_flow: list[str]
    queue_contracts: dict[str, str]
    message_routes: list[MessageRoute]
    aggregations: list[AggregationModel]
    endpoints: list[Endpoint]
    threads: list[ThreadModel]
    sending_policy: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_pretty_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2)


def build_default_model() -> DanmujiInitialModel:
    return DanmujiInitialModel(
        model_name="Bilibili Danmuji Initial Domain Model",
        source_document="Bilibili_Danmuji_流程与接口整理 (1).docx",
        layers={
            Layer.ACCESS: [
                "Websocket",
                "WebSocketProxy",
                "HeartByteThread",
                "ReConnThread",
            ],
            Layer.PROTOCOL: ["HandleWebsocketPackage"],
            Layer.DISPATCH: ["ParseMessageThread"],
            Layer.RULE_AGGREGATION: [
                "BlackParseComponent",
                "ShieldGiftTools",
                "ParseThankGiftThread",
                "ParseThankFollowThread",
                "ParseThankWelcomeThread",
            ],
            Layer.SENDING: ["SendBarrageThread", "HttpUserData.httpPostSendBarrage"],
            Layer.LOCAL_FRONTEND: ["DanmuWebsocket(/danmu/sub)"],
        },
        startup_flow=[
            "读取配置并判断是否自动连接",
            "room_init/getInfoByRoom 拉取房间上下文",
            "getDanmuInfo 获取 websocket host_list 与 token",
            "构造首包认证并建立 websocket",
            "发送首次心跳并启动核心线程",
            "接收二进制包并进行协议解码",
            "按 cmd 分发到业务模块",
            "所有可发送文本统一进入 barrageString",
            "SendBarrageThread 按长度分片并限速发送",
            "异常关闭触发 ReConnThread 重连",
        ],
        queue_contracts={
            "resultStrs": "协议层解包后的 JSON 文本队列，供 ParseMessageThread 消费",
            "barrageString": "统一待发送弹幕队列，供 SendBarrageThread 独占消费",
        },
        message_routes=[
            MessageRoute(
                cmd="DANMU_MSG:*",
                normalized_cmd="DANMU_MSG",
                handlers=["弹幕展示", "日志落地", "自动回复匹配", "前端广播"],
                output_queue="barrageString",
            ),
            MessageRoute(
                cmd="SEND_GIFT/GUARD_BUY/SUPER_CHAT_MESSAGE/POPULARITY_RED_POCKET_NEW",
                normalized_cmd="GIFT_RELATED",
                handlers=["礼物过滤", "礼物延时聚合"],
                output_queue="barrageString",
            ),
            MessageRoute(
                cmd="INTERACT_WORD/INTERACT_WORD_V2",
                normalized_cmd="INTERACT",
                handlers=["关注感谢", "欢迎感谢"],
                output_queue="barrageString",
            ),
            MessageRoute(
                cmd="LIVE/PREPARING",
                normalized_cmd="LIVE_STATUS",
                handlers=["更新直播状态", "收敛后台线程"],
            ),
        ],
        aggregations=[
            AggregationModel(
                name="礼物感谢聚合",
                container="thankGiftConcurrentHashMap",
                key_strategy="用户名 + 礼物名",
                window_strategy="延时窗口内累加数量/总额，新事件刷新时间戳",
                output_template_modes=["单人单种", "单人多种", "多人多种"],
            ),
            AggregationModel(
                name="关注感谢聚合",
                container="interacts",
                key_strategy="顺序列表按批次切片",
                window_strategy="窗口结束后按 num 将多个用户合并成一条感谢",
            ),
            AggregationModel(
                name="欢迎感谢聚合",
                container="interactWelcome",
                key_strategy="顺序列表按批次切片",
                window_strategy="窗口结束后按 num 合并欢迎文本",
            ),
        ],
        endpoints=[
            Endpoint(
                "房间初始化",
                "GET",
                "https://api.live.bilibili.com/room/v1/Room/room_init?id={roomid}",
                "短号解析为真实 room_id，并读取直播状态等上下文",
                EndpointTier.CORE,
            ),
            Endpoint(
                "WS 配置",
                "GET",
                "https://api.live.bilibili.com/xlive/web-room/v1/index/getDanmuInfo",
                "获取 websocket host_list 与 token",
                EndpointTier.CORE,
            ),
            Endpoint(
                "发送弹幕",
                "POST",
                "https://api.live.bilibili.com/msg/send",
                "统一弹幕发送出口",
                EndpointTier.RUNTIME,
            ),
            Endpoint(
                "获取抽奖信息",
                "GET",
                "https://api.live.bilibili.com/xlive/lottery-interface/v1/lottery/getLotteryInfoWeb?roomid={roomid}",
                "供红包/天选屏蔽逻辑使用",
                EndpointTier.RUNTIME,
            ),
            Endpoint(
                "签到",
                "GET",
                "https://api.live.bilibili.com/xlive/web-ucenter/v1/sign/DoSign",
                "每日签到任务",
                EndpointTier.OPTIONAL,
            ),
            Endpoint(
                "本地订阅 WS",
                "WS",
                "/danmu/sub",
                "浏览器订阅处理结果并可回传文本代发弹幕",
                EndpointTier.CORE,
            ),
        ],
        threads=[
            ThreadModel("HeartByteThread", "建链成功后", "每 30 秒发送心跳包"),
            ThreadModel("ParseMessageThread", "建链成功后", "消费 resultStrs 并按 cmd 分发"),
            ThreadModel("SendBarrageThread", "存在待发文本时", "消费 barrageString 并做分条限速发送"),
            ThreadModel("ParseThankGiftThread", "礼物缓存有新数据", "在窗口结束后生成感谢弹幕"),
            ThreadModel("ParseThankFollowThread", "关注缓存有新数据", "按人数分组输出关注感谢"),
            ThreadModel("ParseThankWelcomeThread", "访客缓存有新数据", "按人数分组输出欢迎文本"),
            ThreadModel("AutoReplyThread", "收到匹配型弹幕后", "按关键词规则生成自动回复"),
            ThreadModel("AdvertThread", "广告功能开启后", "按固定/随机间隔投喂广告文案"),
            ThreadModel("ReConnThread", "连接关闭后", "按重试策略执行重连"),
        ],
        sending_policy={
            "single_writer_thread": "SendBarrageThread",
            "rate_limit_interval_ms": 1455,
            "split_strategy": "按当前用户可发送最大弹幕长度分段",
            "risk_control_recommendation": "后续若做全局风控，优先在发送层统一实现",
        },
    )


if __name__ == "__main__":
    model = build_default_model()
    print(model.to_pretty_json())

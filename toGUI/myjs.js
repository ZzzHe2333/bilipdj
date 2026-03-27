//版本：2024年5月18日08:49:18
//更新 排队上限
//设置内容
//------------------------------------------------------------------------------------------
var admins = ["迷糊的迷糊菇","一纸轻予梦","写一下你的名字哦"];
var ban_admins = ["黑名单成员","黑名单成员2","黑猫静止"];
var jianzhang = [""];

//权限组 在双引号里面写b站昵称"",""然后用逗号隔开，有名字视为管理员，否则为路人。
//黑名单 在双引号里面写b站昵称"",""然后用逗号隔开，有名字视为黑名单，无法排队.
//跟你骂起来的直接禁言就OK了,这个是让人无法排上.注意,必须开播前写好。
//哦对了，管理员如果是黑名单的话，将会没有权限哦~
//先 黑名单判定，再 普通人判定，最后 管理员判定~
//------------------------------------------------------------------------
//自己改成true就知道了,以下额外就算你觉得不好,也不影响,因为你可以不开鸭,默认关闭。
//普通 功能区
var fankui = false; //是否开启反馈，就是排上了要不要发弹幕(不建议开)
var guanli_fankui = false; //是否开启反馈，就是管理员操作了要不要发弹幕(不建议开)
var paidui_list_length_max = 100 ; //排队人数上限，默认100，直播中可通过指令修改。

var jianzhangchadui = false; //是否开启舰长插队
var jianzhang_cd_kind = 1; //舰长插队类型|1:默认(已实装);2:上舰插队(未实装);3:礼物插队(未实装);|
var jianzhang_cd_cishu = 1;//被舰长插队的普通人次数，比如最多被插队一次；//未实装

var fangguan_can_doing = false //房管默认为插件控制者(主播已默认)
var all_suoyourenbukepaidui = false //所有人禁止排队,当然仅主播管理员可使用

// 高级功能区，只用基础功能的无需观看~需求请移步myjs其他功能教学.txt
var YHbot_kaiguan = false; //如需使用改成true
var YHbotId = "";//云湖app
var YHbot_msg_type = ""; //云湖app
var YHbot_webhook_token = "";//云湖app
var ws_zbtool_kaiguan = false; //主播工具箱

var QYWX_kaiguan = false;//企业微信开关
var WX_webhook = ""; //企业微信
//未实装功能,画饼区
var only_myfuns_paidui = false //仅本直播间牌子可排队(需要设置,未实装)。
var liwu_chadui_kg = false;//礼物插队
var liwu_paidui_kg = false;//礼物排队
var liwu_chadui_kind = 50;//插队礼物
var liwu_paidui_kind = 50;//排队礼物
//-----------------------------------------------------------------------------
jianzhang.push("一纸轻予梦");//强制授予 一纸轻予梦的舰长身份，用于测试。
console.log("[基础信息]","管理员:",admins,"黑名单",ban_admins,"初始舰长权限",jianzhang);
//预写入-
super_xinxi_desu = [["一纸轻予梦","喜欢雨露的小青草儿"],[0,0],[0,0],[],[],[],[],[],[],[]]
// 0 名字
// 1 当天排队数
// 2 当天被插队数
// 3 
// 4 
// 5

//-----------------------------------------------------------------------------
//以下无需更改，也不能更改；除非你很懂代码，不然建议你别动！










//-----------------------------------------------------------------------------
//迷糊uid 629543291 迷糊房间号 26714219 一纸轻予梦的18461303
// main.js
var zroomid = 0;//你的直播间号（由配置面板注入）
var zuid = 0;//你的uid（由配置面板注入）
var persons =[];
var confirm_wait_seconds = 10; // del 1 的确认时长，默认10秒，可用“设置确认时间 30”调整
var pendingConfirmTask = null; // { person, originalItem, timerId, startedAt }
var ws = null;
var pdjConnected = false;

function PDJ_EmitStatus(status, detail = {}) {
    window.dispatchEvent(new CustomEvent("pdj:status", {
        detail: Object.assign({ status: status, roomid: zroomid, uid: zuid }, detail)
    }));
}

async function PDJ_LoadConfig() {
    try {
        var res = await fetch('/api/config');
        if (!res.ok) {
            throw new Error('读取配置失败: ' + res.status);
        }
        var cfg = await res.json();
        zroomid = Number(cfg.roomid || 0);
        zuid = Number(cfg.uid || 0);
        applyMyJSConfig(cfg.myjs || {});
        PDJ_EmitStatus('config_loaded', { config: cfg });
    } catch (err) {
        console.error('[PDJ] 配置读取失败', err);
        PDJ_EmitStatus('config_error', { error: String(err) });
    }
}

function applyMyJSConfig(mycfg) {
    if (Array.isArray(mycfg.admins)) admins = mycfg.admins;
    if (Array.isArray(mycfg.ban_admins)) ban_admins = mycfg.ban_admins;
    if (Array.isArray(mycfg.jianzhang)) jianzhang = mycfg.jianzhang;
    if (typeof mycfg.fankui === "boolean") fankui = mycfg.fankui;
    if (typeof mycfg.guanli_fankui === "boolean") guanli_fankui = mycfg.guanli_fankui;
    if (Number.isFinite(Number(mycfg.paidui_list_length_max))) paidui_list_length_max = Number(mycfg.paidui_list_length_max);
    if (typeof mycfg.jianzhangchadui === "boolean") jianzhangchadui = mycfg.jianzhangchadui;
    if (Number.isFinite(Number(mycfg.jianzhang_cd_kind))) jianzhang_cd_kind = Number(mycfg.jianzhang_cd_kind);
    if (Number.isFinite(Number(mycfg.jianzhang_cd_cishu))) jianzhang_cd_cishu = Number(mycfg.jianzhang_cd_cishu);
    if (typeof mycfg.fangguan_can_doing === "boolean") fangguan_can_doing = mycfg.fangguan_can_doing;
    if (typeof mycfg.all_suoyourenbukepaidui === "boolean") all_suoyourenbukepaidui = mycfg.all_suoyourenbukepaidui;
    if (typeof mycfg.yhbot_kaiguan === "boolean") YHbot_kaiguan = mycfg.yhbot_kaiguan;
    if (typeof mycfg.yhbotid === "string") YHbotId = mycfg.yhbotid;
    if (typeof mycfg.yhbot_msg_type === "string") YHbot_msg_type = mycfg.yhbot_msg_type;
    if (typeof mycfg.yhbot_webhook_token === "string") YHbot_webhook_token = mycfg.yhbot_webhook_token;
    if (typeof mycfg.ws_zbtool_kaiguan === "boolean") ws_zbtool_kaiguan = mycfg.ws_zbtool_kaiguan;
    if (typeof mycfg.qywx_kaiguan === "boolean") QYWX_kaiguan = mycfg.qywx_kaiguan;
    if (typeof mycfg.wx_webhook === "string") WX_webhook = mycfg.wx_webhook;
    if (typeof mycfg.only_myfuns_paidui === "boolean") only_myfuns_paidui = mycfg.only_myfuns_paidui;
    if (typeof mycfg.liwu_chadui_kg === "boolean") liwu_chadui_kg = mycfg.liwu_chadui_kg;
    if (typeof mycfg.liwu_paidui_kg === "boolean") liwu_paidui_kg = mycfg.liwu_paidui_kg;
    if (Number.isFinite(Number(mycfg.liwu_chadui_kind))) liwu_chadui_kind = Number(mycfg.liwu_chadui_kind);
    if (Number.isFinite(Number(mycfg.liwu_paidui_kind))) liwu_paidui_kind = Number(mycfg.liwu_paidui_kind);
    console.log("[配置覆盖]", { admins, ban_admins, jianzhang, paidui_list_length_max });
}

async function PDJ_Connect() {
    await PDJ_LoadConfig();

    if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) {
        return ws;
    }

    ws = new WebSocket(PDJ_GetWebSocketURL());
    ws.onopen = function () {
        pdjConnected = true;
        document.getElementById("status").textContent = "已连接";
        PDJ_EmitStatus('connected');
    };

    ws.onclose = function () {
        pdjConnected = false;
        PDJ_EmitStatus('disconnected');
        setTimeout(function () { PDJ_Connect(); }, 2000);
    };

    ws.onerror = function (err) {
        PDJ_EmitStatus('error', { error: String(err) });
    };

    ws.onmessage = async function (msgEvent) {
        console.log("[弹幕Json信息]")
        console.log(msgEvent.data)

        if (typeof msgEvent.data !== 'string') {
            return;
        }

        var maybeJson = null;
        try {
            maybeJson = JSON.parse(msgEvent.data);
        } catch (_e) {
            return;
        }

        if (maybeJson && maybeJson.type === 'PDJ_STATUS') {
            PDJ_EmitStatus(maybeJson.status || 'server', maybeJson);
            return;
        }

        shujubaobao = msgEvent.data;
        jsontoprint(maybeJson);//调用下面的函数
    };

    return ws;
}

PDJ_Connect();

function PDJ_GetWebSocketURL() {
    if (window && window.location && (window.location.protocol === "http:" || window.location.protocol === "https:")) {
        var protocol = window.location.protocol === "https:" ? "wss://" : "ws://";
        return protocol + window.location.host + "/danmu/sub";
    }
    return "ws://127.0.0.1:23333/danmu/sub";
}


function PDJ_Send(payload) {
    if (!ws || ws.readyState !== WebSocket.OPEN) {
        console.warn('[PDJ] WS未连接，消息已丢弃', payload);
        return;
    }
    ws.send(payload);
}

function stripHtml(text) {
    return (text || "").replace(/<[^>]*>/g, "");
}

function guessQueuePersonName(queueItem) {
    var plain = stripHtml(queueItem || "").trim();
    if (!plain) return "";
    plain = plain.replace(/^@/, "");
    var firstToken = plain.split(/\s+/)[0] || "";
    if (firstToken.indexOf("|") >= 0) {
        var parts = firstToken.split("|");
        return (parts[parts.length - 1] || "").trim();
    }
    return firstToken.trim();
}

function markItemAsPending(queueItem) {
    if (!queueItem) return queueItem;
    var marked = queueItem.replace("<span>", '<span class="pdj-confirm-pending">⏳待确认 ');
    if (marked === queueItem) {
        marked = '<span class="pdj-confirm-pending">⏳待确认 ' + queueItem + '</span>';
    }
    return marked;
}

function clearPendingConfirmTask() {
    if (!pendingConfirmTask) return;
    if (pendingConfirmTask.timerId) {
        clearTimeout(pendingConfirmTask.timerId);
    }
    pendingConfirmTask = null;
}

function removeFirstQueueWithPendingClear() {
    if (persons.length > 0) {
        persons.splice(0, 1);
    }
    clearPendingConfirmTask();
}

function startDelFirstPendingConfirm() {
    if (!persons.length) return;
    clearPendingConfirmTask();
    var originalItem = persons[0];
    var pendingPerson = guessQueuePersonName(originalItem);
    pendingConfirmTask = {
        person: pendingPerson,
        originalItem: originalItem,
        startedAt: Date.now(),
        timerId: setTimeout(function () {
            if (!pendingConfirmTask) return;
            var timeoutPerson = pendingConfirmTask.person || "该观众";
            removeFirstQueueWithPendingClear();
            PDJ_Send("@" + timeoutPerson + " 超时跳过");
            console.log("[叫号确认]超时自动跳过:", timeoutPerson);
        }, Math.max(1, Number(confirm_wait_seconds || 10)) * 1000)
    };
    persons[0] = markItemAsPending(originalItem);
    if (pendingPerson) {
        PDJ_Send("@" + pendingPerson + " 请在" + confirm_wait_seconds + "秒内回复“在/来了/好的”确认");
    } else {
        PDJ_Send("请第一位观众在" + confirm_wait_seconds + "秒内回复“在/来了/好的”确认");
    }
    console.log("[叫号确认]等待确认:", pendingPerson, "超时秒数:", confirm_wait_seconds);
}

function ws_zbtool_open(){
    const ws_zbtool_url = "http://127.0.0.1:23223"
    var ws_zbtool = new WebSocket(ws_zbtool_url);
    var ShuJu_bag; //别动，没有BUG就别动。
    ws_zbtool.onmessage = async function (msgEvent) {
        console.log("[弹幕Json信息]")
        console.log(msgEvent.data)
        ShuJu_bag = msgEvent.data 
        op_quanxian(ShuJu_bag);
        document.getElementById('danmu').innerHTML=persons.join("");
    };
}
if(ws_zbtool_kaiguan == true){
    ws_zbtool_open()
}


const textEncoder = new TextEncoder('utf-8');
const textDecoder = new TextDecoder('utf-8');
const readInt = function (buffer, start, len) {
    let result = 0
    for (let i = len - 1; i >= 0; i--) {
        result += Math.pow(256, len - i - 1) * buffer[start + i]
    }
    return result
};
const writeInt = function (buffer, start, len, value) {
    let i = 0
    while (i < len) {
        buffer[start + i] = value / Math.pow(256, len - i - 1)
        i++
    }
};
const encode = function (str, op) {
    let data = textEncoder.encode(str);
    let packetLen = 16 + data.byteLength;
    let header = [0, 0, 0, 0, 0, 16, 0, 1, 0, 0, 0, op, 0, 0, 0, 1]
    writeInt(header, 0, 4, packetLen)
    return (new Uint8Array(header.concat(...data))).buffer
};
//----------//----------//----------//----------//----------
//list转化为txt，方便发送，输入[],返回 str。
function list_to_enter_txt(list){
    //新增的功能
        var nowlist = list;
        console.log("输出");
        const ooo_list = nowlist.map(ooo =>
            ooo.replace(/<br><\/span>/g, "").replace(/<span>/g, ""));
        const ooo_str = ooo_list.join("\n");
        return ooo_str
};
//推送消息到云湖软件，QQ没接口，或者说我不会，可以去看看cqhttp。
function to_yunhu_group(list){
    if (YHbot_kaiguan == false){
        return
    }
    console.log("开始试图将排队列表推送到云湖")
    //新增的功能
    var nowlist = list;
    const ooo_list = nowlist.map(ooo =>
        ooo.replace(/<br><\/span>/g, "").replace(/<span>/g, ""));
    const ooo_str = ooo_list.join("\n");

    var myHeaders = new Headers();
    myHeaders.append("Content-Type", "application/json");
    console.log("尝试推送到云湖");
    if(YHbotId == ""||YHbot_msg_type==""||YHbot_webhook_token == ""){
        console.error('信息不完整')
        return
    }
    var raw = JSON.stringify({
    "recvId": YHbotId,
    "recvType": YHbot_msg_type,
    "contentType": "text",
    "content": {
        "text": ooo_str
    }
    });

    var requestOptions = {
    method: 'POST',
    headers: myHeaders,
    body: raw,
    redirect: 'follow'
    };
    var webhook_base = "https://chat-go.jwzhd.com/open-apis/v1/bot/send?token=";
    var token_is = YHbot_webhook_token
    var webhook_is = webhook_base + token_is 

    fetch(webhook_is, requestOptions)
    .then(response => response.text())
    .then(result => console.log(result))
    .catch(error => console.log('error', error));
};
//推送消息到企业微信，QQ没接口，或者说我不会，可以去看看cqhttp。
//好像有点问题，你们会修的自己修。
function QiYe_weixin(list) {
    if (QYWX_kaiguan == false){
        return
    }
    console.log("开始试图将排队列表推送到企业微信")
    var QYWX_text = list_to_enter_txt(list)
    var QYWX_webhook = WX_webhook; // webhook地址
    var QYWX_headers = {
        'Content-Type': 'application/json'
    };
    var QYWX_body = {
        msgtype: "markdown",
        markdown: {
            content: QYWX_text
        }
    };
    var QYWX_requestOptions = {
        method: 'POST',
        headers: QYWX_headers,
        body: JSON.stringify(QYWX_body)
    }
    fetch(QYWX_webhook,QYWX_requestOptions)
    .then(response => response.json())
    .then(data => console.log('Success:', data))
    .catch((error) => console.log('Error:', error));
};
//推送到弹幕流
function sendMessage(xz_orz){ 
    if (fankui == true){
        console.log("排队人员：",xz_orz);
        zspxz_message = xz_orz + ",排上啦~";
        PDJ_Send(zspxz_message);
    }
};
function sendMessage_diy(xz_orz,kind = ",排上啦~"){
    if (kind != ""){
        var massage_xzorz = kind}
    if (fankui == true){
        console.log("排队人员：",xz_orz);
        zspxz_message = xz_orz + massage_xzorz
        PDJ_Send(zspxz_message)}
};
//输入list，str，如果str包含了list的元素，就会返回布尔值。
//如果 str = assss，list = ['as','bb']
function userList_in_String(userList = [],UserStr = ""){
    //console.log(userList,UserStr)
    var asa_boo = false
    if(userList == []||UserStr == ""){
        return};
    for (let i = 0; i < userList.length; i++) {
        var UiS_index = UserStr.indexOf(userList[i]);
        if (UiS_index !== -1) {
          console.log(`Found '${userList[i]}' at index ${UiS_index}`);
          //console.error("[实际需要+1]舰长位置 i=",i)
          asa_boo = true
          break;
        }
    }
    return asa_boo          
}
//----------//----------//----------//----------//----------
function user_quanxian(message,index,person){
    if(message=="排队"){                
        if(index < 0){
            persons.push("<span>"+person+"<br></span>");
            sendMessage(person);
        }               
    }
    
    if(message=="官服排"||message=="排官服"||message=="官服排队"||message=="排队官服"){                
        if(index < 0){
            persons.push("<span>官|"+person+"<br></span>");                   
            sendMessage_diy(person,",排成功官服");
        }               
    }

    if(message=="B服排"||message=="b服排"||message=="排b服"||message=="排B服"||message=="B服排队"||message=="排队B服"||message=="b服排队"||message=="排队b服"){                
        if(index < 0){
            persons.push("<span>B|"+person+"<br></span>");
            sendMessage_diy(person,",排成功B服");
        }               
    }

    if(message=="超级排"||message=="超级排队"){                
        if(index < 0){
            persons.push("<span><"+person+"><br></span>");
            sendMessage(person);
        }               
    }
    if(message=="小米排"||message=="排小米"||message=="排米服"){                
        if(index < 0){
            persons.push("<span>米|"+person+"<br></span>");
            sendMessage(person);
        }               
    }

    var messageFlag =message.indexOf("排队 ");
    if(messageFlag != -1 && messageFlag <= 1){
        var messageIndex = message.indexOf(" ");                
        var massageSub =  message.slice(messageIndex);
        if(index < 0){
            persons.push("<span>"+person+massageSub+"<br></span>");
            sendMessage(person);
        }
    }

    var messageFlag_Super_A = message.indexOf("超级排 ");
    var messageFlag_Super_B = message.indexOf("超级排队 ");
    if(messageFlag_Super_A != -1 || messageFlag_Super_B != -1){
        var messageIndex_super = message.indexOf(" ");
        var massageSub_super =  message.slice(messageIndex_super);

        massageSub_super = ":" + massageSub_super + ""
        Superperson = "<" + person + ">"
        if(index < 0){
            persons.push("<span>"+Superperson+massageSub_super+"<br></span>");
            sendMessage(person);
        }
    }
    
    var messageFlag_guanfu_A = message.indexOf("官服排 ");
    var messageFlag_guanfu_B = message.indexOf("官服排队 ");
    if(messageFlag_guanfu_A != -1||messageFlag_guanfu_B != -1){
        var messageIndex_guanfu = message.indexOf(" ");
        var massageSub_guanfu =  message.slice(messageIndex_guanfu);
        massageSub_guanfu = "" + massageSub_guanfu + ""
        SuperpersonG = "官|" + person + ""
        if(index < 0){
            persons.push("<span>"+SuperpersonG+massageSub_guanfu+"<br></span>");
            sendMessage_diy(person,",排成功官服");
        }
    }

    var messageFlag_Bfu_A = message.indexOf("B服排 ");
    var messageFlag_Bfu_B = message.indexOf("B服排 ");
    if(messageFlag_Bfu_A != -1||messageFlag_Bfu_B != -1){
        var messageIndex_Bfu = message.indexOf(" ");//定位空格
        var massageSub_Bfu =  message.slice(messageIndex_Bfu);//截取空格之前
        massageSub_Bfu = "" + massageSub_Bfu + ""
        SuperpersonB = "B|" + person + ""
        if(index < 0){
            persons.push("<span>"+SuperpersonB+massageSub_Bfu+"<br></span>");
            sendMessage_diy(person,",排成功B服");
        }
        
    }

    var messageFlag_Mfu =message.indexOf("米服排 ");
    if(messageFlag_Mfu != -1){
        var messageIndex_Mfu = message.indexOf(" ");
        var massageSub_Mfu =  message.slice(messageIndex_Mfu);

        massageSub_Mfu = "" + massageSub_Mfu + ""
        SuperpersonM = "M|" + person + ""

        //console.log("messageIndex:"+messageIndex_Mfu);
        //console.log("massageSub:"+massageSub_Mfu);
        if(index < 0){
            persons.push("<span>"+SuperpersonM+massageSub_Mfu+"<br></span>");
            sendMessage(person);
        }
    }
}

function user_quanxian_yp(message,index,person){
    if(message=="取消排队"||message=="排队取消"||message=="我确认我取消排队"){
        persons.splice(index,1);
        sendMessage_diy(person,",取消了排");
    };
    if(message=="替换"||message=="修改"||message=="内容洗白"){
        var TiHuanMSG ="<span>"+person +"<br></span>"
        persons.splice(index,1,TiHuanMSG);
        sendMessage_diy(person,",清空排队内容。");
    }

    var TiHuanIdex = message.indexOf("替换 ");
    var XiuGaiIdex = message.indexOf("修改 ");
    //if(TiHuanIdex == 0||XiuGaiIdex == 0){
    if(TiHuanIdex >= 0&&TiHuanIdex<=4||XiuGaiIdex>= 0&&XiuGaiIdex<=4){
        var Message_change = "";
        if(TiHuanIdex >= 0){
            var Message_change=message.replace("替换","");};
        if(XiuGaiIdex >= 0){
            var Message_change=message.replace("修改","");};            
        var TiHuanMessage ="<span>"+person + Message_change+"<br></span>"
        persons.splice(index,1,TiHuanMessage);
        sendMessage_diy(person,",修改了内容");
    }
}
function op_quanxian(message){
    //管理员权限
    //.indexOf是查找字符的位置
                //找到了，返回字符所在位置

                //删列表系列
                var delIdex = message.indexOf("del");
                var ShanCIdex = message.indexOf("删除");
                var WanCIndex = message.indexOf("完成");
                if(delIdex >= 0|| ShanCIdex >= 0 ||WanCIndex >= 0){
                    var dindex=message.replace(/[^0-9]/ig,"");//除了数字 全部删除
                    //条件 长度大于0，删除数字以外后剩下非空值，序号的长度大于等于需要删除的位置。
                    var GuoLv_MSG = message.replace(/[\d\s]+/g,"");//删除数字和空格
                    if(GuoLv_MSG =="del"||GuoLv_MSG =="删除"||GuoLv_MSG =="完成"){
                        if(dindex!=""&&persons.length>=dindex&&persons.length>0){
                            if(Number(dindex) === 1){
                                startDelFirstPendingConfirm();
                            }else{
                                persons.splice(dindex-1,1);
                            }
                        };
                    };
                    if (guanli_fankui = true){
                        sendMessage_diy(dindex,"号,被管理员删除了");
                    }
                    
                };


                //新增模块：add/新增/添加，顺序添加
                var newIdex = message.indexOf("add ");
                var xinzengIdex = message.indexOf("新增 ");
                var tianjiaIdex = message.indexOf("添加 ");
                if(newIdex >= 0&&newIdex <= 4||xinzengIdex >= 0 && xinzengIdex <=4||tianjiaIdex >= 0 && tianjiaIdex <=4){
                    var add_message = message.replace("add ","");
                    add_message=add_message.replace("add ","");
                    add_message=add_message.replace("新增 ","");
                    add_message=add_message.replace("添加 ","");
                    if(add_message!=""){
                        persons.push("<span>"+add_message+"<br></span>");
                    } ;
                    if (guanli_fankui = true){
                        sendMessage_diy("添加了信息",add_message);
                    }
                };
                //拉黑 xxx
                var lahei_Idex = message.indexOf("拉黑 ");//0是第一个字，1是第二个字
                if(lahei_Idex >= 0 && lahei_Idex <= 1){
                    var lahei_message=message.replace("拉黑 ","");
                    if(lahei_message!=""){
                        ban_admins.push(lahei_message);
                        consolelogprint(2,message)
                    };
                };
                //取消拉黑 xxx
                var del_lahei_Idex = message.indexOf("取消拉黑 ");
                if(del_lahei_Idex >= 0 && del_lahei_Idex <= 1){
                    var del_lahei_message=message.replace("取消拉黑 ","");
                    if(del_lahei_message != ""){
                        var del_lahei_index = ban_admins.indexOf(del_lahei_message);
                        if (del_lahei_index > -1){
                            ban_admins.splice(del_lahei_index, 1);
                        };
                        if(del_lahei_index == -1){
                            console.error("找不到:" , del_lahei_message)
                        };
                        consolelogprint(2,message)
                    };
                }
                //添加管理员 xxx
                var add_op_Idex = message.indexOf("添加管理员 ");
                if(add_op_Idex >= 0){
                    var add_op_message=message.replace("添加管理员 ","");
                    if(add_op_message != ""){
                        admins.push(add_op_message);
                        //PDJ_Send("添加临时管理:" + add_op_message)
                        console.log("[本场管理员]:",admins)
                    };
                };
                //取消管理员 xxx
                var del_op_Idex = message.indexOf("取消管理员 ");
                if(del_op_Idex >= 0){
                    var del_op_message=message.replace("取消管理员 ","");
                    if(del_op_message != ""){
                        var del_op_index = admins.indexOf(del_op_message);
                        if (del_op_index > -1){
                            admins.splice(del_op_index, 1);
                            PDJ_Send("临时取消管理:" + del_op_message)
                            console.log("[本场管理员]:",admins)}
                        if(del_op_index == -1){
                            PDJ_Send("找不到:" + del_op_message)
                            console.error("找不到:" , del_op_message)
                            console.log("[本场管理员]:",admins)}    
                    };
                };
                //无影插队系列
                var WYCDIndex = message.indexOf("无影插");
                if (WYCDIndex >= 0 && WYCDIndex <= 4) {
                  // 使用正则表达式匹配数字部分，例如 "无影插 2 胡桃" 中的 "2"
                  var WYpositionMatch = message.match(/\d+/);
                  
                  // 如果找到了匹配的位置
                  if (WYpositionMatch) {
                    var position = parseInt(WYpositionMatch[0], 10); // 将匹配的位置字符串转换为整数
                    
                    if (position >= 1 && position <= 20) {
                      // 删除 "无影" 和数字部分，只保留内容
                      var WYchaduiText = message.replace(/无影插\s+\d+\s+/, '');
                      WYchaduiText = "<span>" + WYchaduiText + "<br></span>"
                      // 在指定位置插入新内容
                      persons.splice(position - 1, 0, WYchaduiText);
                    }
                    };
                    if (guanli_fankui = true){
                        sendMessage_diy("添加了信息",WYchaduiText);
                    }
                };

                //插队系列
                var chaduiIndex = message.indexOf("插队 ");
                if (chaduiIndex >= 0 && chaduiIndex <= 4) {
                  // 使用正则表达式匹配数字部分，例如 "插队 2 胡桃" 中的 "2"
                  var positionMatch = message.match(/\d+/);
                  // 如果找到了匹配的位置
                    if (positionMatch) {
                        var position = parseInt(positionMatch[0], 10); // 将匹配的位置字符串转换为整数
                        if (position >= 1 && position <= 30) {
                        // 删除 "插队" 和数字部分，只保留内容
                        var chaduiText = message.replace(/插队\s+\d+\s+/, '');
                        chaduiText = "<span>@" + chaduiText + "<br></span>"
                        // 在指定位置插入新内容
                        persons.splice(position - 1, 0, chaduiText);
                        }
                    };
                    if (guanli_fankui = true){
                        sendMessage_diy("添加了信息",chaduiText);
                    }
                };

                //临时开关系列
                //舰长插队开关
                if(message == "开启舰长插队"){
                    jianzhangchadui = true;
                    PDJ_Send("OPEN JZCD MOD");
                    console.log("[功能]舰长插队开启");
                };
                if(message == "关闭舰长插队"){
                    jianzhangchadui = false;
                    PDJ_Send("CLOSE JZCD MOD");
                    console.log("[功能]舰长插队关闭");
                };
                //停止排队开关
                if(message == "暂停排队功能"||message == "关闭自助排队"){
                    all_suoyourenbukepaidui = true;
                    PDJ_Send("[Msg]排队功能已暂停");
                    console.log("[功能]排队功能暂停");
                };
                if(message == "恢复排队功能"||message == "恢复自助排队"){
                    all_suoyourenbukepaidui = false;
                    PDJ_Send("[Msg]排队功能已恢复");
                    console.log("[功能]排队功能恢复");
                };
                //房管继承制度
                if(message == "允许房管成为插件管理员"){
                    fangguan_can_doing = true;
                    //PDJ_Send("[Msg]排队功能已暂停");
                    console.log("[功能]允许房管成为插件管理员");
                };
                if(message == "停止房管成为插件管理员"){
                    fangguan_can_doing = false;
                    //PDJ_Send("[Msg]排队功能已恢复");
                    console.log("[功能]停止房管成为插件管理员");
                };
                //未实装 仅粉丝可排队
                if(message == "开启仅粉丝可排队"){
                    only_myfuns_paidui = true;
                    PDJ_Send("OPEN OnlyFuns MOD");
                    console.log("[功能]仅粉丝可排队开启");
                };
                if(message == "关闭仅粉丝可排队"){
                    only_myfuns_paidui = false;
                    PDJ_Send("CLOSE OnlyFuns MOD");
                    console.log("[功能]仅粉丝可排队关闭");
                };

                //设置被插队次数
                var Set_beichadui_Index = message.indexOf("设置被插队次数");
                if(Set_beichadui_Index >= 0){
                    var Set_beichadui_QAQ = message.replace(/[^0-9]/ig,"");//除了数字 全部删除
                    var Set_beichadui_MSG = message.replace(/[\d\s]+/g,"");//删除数字和空格
                    if(Set_beichadui_MSG =="设置被插队次数"){
                        jianzhang_cd_cishu = Set_beichadui_QAQ
                        PDJ_Send("Set Beichadui =" + jianzhang_cd_cishu);
                        console.log("[功能]被插队次数调整为",jianzhang_cd_cishu);
                        };
                };
                //设置排队人数上限
                var Set_paidui_max1_Index = message.indexOf("设置排队人数");
                var Set_paidui_max2_Index = message.indexOf("设置排队上限");
                var Set_paidui_max3_Index = message.indexOf("设置排队人数上限");
                if(Set_paidui_max1_Index >= 0||Set_paidui_max2_Index >= 0||Set_paidui_max3_Index >= 0){
                    var Set_paidui_max_QAQ = message.replace(/[^0-9]/ig,"");//除了数字 全部删除
                    var Set_paidui_max_MSG = message.replace(/[\d\s]+/g,"");//删除数字和空格
                    if(Set_paidui_max_MSG =="设置排队人数"||Set_paidui_max_MSG =="设置排队人数上限"||Set_paidui_max_MSG =="设置排队上限"){
                        paidui_list_length_max = Set_paidui_max_QAQ
                        PDJ_Send("Set PaiDuimax =" + paidui_list_length_max);
                        console.log("[功能]排队上限调整为",paidui_list_length_max);
                        };
                };

                //设置叫号确认时间
                var Set_confirm_time_Index = message.indexOf("设置确认时间");
                if(Set_confirm_time_Index >= 0){
                    var Set_confirm_time_QAQ = message.replace(/[^0-9]/ig,"");//除了数字 全部删除
                    var Set_confirm_time_MSG = message.replace(/[\d\s]+/g,"");//删除数字和空格
                    if(Set_confirm_time_MSG == "设置确认时间" && Set_confirm_time_QAQ != ""){
                        confirm_wait_seconds = Math.max(3, Number(Set_confirm_time_QAQ));
                        PDJ_Send("Set ConfirmTime =" + confirm_wait_seconds);
                        console.log("[功能]叫号确认时间调整为",confirm_wait_seconds,"秒");
                    };
                };

}

function consolelogprint(kind = 1,person = "",message = ""){
    if (kind == 1){
        console.log("[接收信息]",person,"--->",message);
        console.log("[排队列表]",persons);
        console.log("-------------------------------------",);}
    if(kind == 2){
        console.log("[本场黑名单]:",ban_admins)
    }
    if(kind == 3){
        console.log("[本场管理员]:",admins)
    }
}
//main 函数
function jsontoprint(data) {  
    var indexBool_danmu = data.cmd.indexOf('danmu'); 
    var indexBool_DANMU_MSG = data.cmd.indexOf('DANMU_MSG');
    var indexBool_SUPER_CHAT_MESSAGE = data.cmd.indexOf('SUPER_CHAT_MESSAGE');
    var indexContent ;

    if(indexBool_danmu >= 0 ){
        indexContent = 'danmu';
    };
    if(indexBool_DANMU_MSG >= 0 ){
        indexContent = 'DANMU_MSG';
    };
    if(indexBool_SUPER_CHAT_MESSAGE >= 0 ){
        indexContent = 'SUPER_CHAT_MESSAGE';
    };
    switch (indexContent) {
        case 'danmu': //case 'DANMU_MSG':
            //var amminIdex = admins.indexOf(person);//查找用户是不是管理员 老的变量，需要统一架构

            var message =data.result.msg; //弹幕信息
            var person = data.result.uname; //弹幕用户名
            var is_jianzhang = data.result.svip; //是否 B站舰长
            var user_kind_room = data.result.manager; // 0用户,1房管,2主播

            var index =persons.findIndex((item,index) => { return item.indexOf(person) != -1 });//排队序号
            var ban_user_inIdex =ban_admins.indexOf(person);//查找用户是不是排队黑名单
            var op_user_inIdex = admins.indexOf(person);//查找用户是不是管理员
            var jianZ_inIdex = jianzhang.indexOf(person);//查找用户是不是 插件舰长

            var paidui_list_length = persons.length;//获取排队列表长度
            var confirmWords = ["在","来了","好的","到","在的","我在","在呢"];
            var normalizedMsg = (message || "").trim();

            // 叫号确认：等待被叫到的第一位观众回复
            if (pendingConfirmTask && pendingConfirmTask.person == person) {
                if (confirmWords.indexOf(normalizedMsg) !== -1) {
                    removeFirstQueueWithPendingClear();
                    PDJ_Send("@" + person + " 已确认，开始服务");
                    console.log("[叫号确认]确认成功:", person);
                    document.getElementById('danmu').innerHTML=persons.join("");
                    return;
                }
            }
            
            //找不到名字就是 -1,如果找到了,就直接return;
            if(ban_user_inIdex != -1){
                console.log("[排队权限]已禁止黑名单排队||排队无效--->",person);
                return
            };

            //禁止所有人排队开关打开，而且，不是管理员。就直接返回。管理员还是能操作
            // 找到管理员,aop_user_inIdex就大于0,找不到就-1,如果我禁止排队且不是管理员,就return。
            if(all_suoyourenbukepaidui == true && op_user_inIdex == -1){
                console.log("[排队权限]已禁止所有人排队||排队无效--->",person);
                return
            };
            //查看是不是已有舰长--->查看是不是B站舰长--->尝试添加为插件舰长--->更新舰长值
            if (jianZ_inIdex == -1 && is_jianzhang == 1){
                    console.log("[添加权限]舰长:",person);
                    console.log("[权限列表]:",jianzhang);
                    jianzhang.push(person);
                    jianZ_inIdex = jianzhang.indexOf(person);//更新用户舰长
            };            
            //查看是不是已有管理员--->查看是不是B站房管--->尝试添加为插件管理--->更新房管值
            if(op_user_inIdex == -1){        
                if(user_kind_room == 2){
                    console.log("[添加权限]主播:",person); 
                    console.log("[权限列表]:",admins);
                    admins.push(person);
                    op_user_inIdex = admins.indexOf(person);//更新用户管理员
                };                  
                if(user_kind_room == 1 && fangguan_can_doing == true){                    
                        console.log("[添加权限]房管:",person);
                        console.log("[权限列表]:",admins);
                        admins.push(person)
                        op_user_inIdex = admins.indexOf(person);//更新用户管理员
                    
                };
                
            };

            //排队长度大于等于上限。且不是管理员，且自己不是已经排上的（方便修改）就直接略过
            if(paidui_list_length >= paidui_list_length_max && index == -1 ){
                console.error("当前排队@"+paidui_list_length+" 排队上限#"+paidui_list_length_max);
                if(op_user_inIdex < 0){
                    console.log("[排队限制]到达排队上限||排队无效--->",person);
                    return;
                };
                if(op_user_inIdex >= 0){
                    console.log("[排队限制]到达排队上限||管理员操作有效--->",person);
                }
                
            }


            if(index < 0 && jianZ_inIdex !== -1 && message == "插队"){
                // 三个条件 没排队&是舰长&发了插队
                console.log("[舰长插队]当前排队长度",paidui_list_length)
                //如果是0或者舰长插队没开,直接排队。
                if(paidui_list_length == 0||jianzhangchadui == false){
                    persons.push("<span>"+person+"<br></span>");
                    message = "[舰长插队]已处理当做普通排队处理" ;
                };
                //如果排队人数>0,而且舰长插队开了，就查看被插队的那个人是不是舰长
                if(paidui_list_length > 0 && jianzhangchadui == true){
                    qianyiwei_index = paidui_list_length - 1 ;
                    get_user_qianyiwei = persons[qianyiwei_index]
                    //解释一下，比如现在长度是5，5人排队,那最后一名是 persons[4]
                    //console.log("[舰长插队]",get_user_qianyiwei)
                    chaduiText = "<span>" + person + "<br></span>"; //构建插队文本

                    //检测要插队的位置是不是其他舰长，从舰长的list里面查找前一名的信息
                    var qianyiwei_is_jianzhang = userList_in_String(jianzhang,get_user_qianyiwei)

                    //如果前一位不是舰长，那可以插队
                    //目前没加限制，理论上来说，可以无限插队平民
                    if(qianyiwei_is_jianzhang == false){                                
                        persons.splice(qianyiwei_index , 0, chaduiText);}
                    //如果前一位是舰长，那就只能普通的排队了，不然舰长无限互相插队会炸的。  
                    if(qianyiwei_is_jianzhang == true){ 
                        persons.push(chaduiText);}
                    message = "[舰长插队]已处理完无需调用其他" ;
                };
                        
            };
            user_quanxian(message,index,person);//正常排队

            if(index != -1){user_quanxian_yp(message,index,person)};//已经排上队的人（列表有名字）

            if(op_user_inIdex != -1){op_quanxian(message,index,person)};//管理员

            consolelogprint(1,person,message);
            //let para = document.createElement("p")
            //let node = document.createTextNode(data.info[2][1]+':'+data.info[1])
            //para.appendChild(node)
            document.getElementById('danmu').innerHTML=persons.join(""); 
            break; 
        case 'SUPER_CHAT_MESSAGE':
            document.getElementById('danmu').append(data.data.price+'$'+data.data.user_info.uname+':'+data.data.message+'\n')
            break
        case 'DANMU_MSG':
            break
        default:
            // console.log(data)
            //document.getElementById('danmu').append(data.cmd+'\n')
            break
    }

    
    to_yunhu_group(persons)
    //QiYe_weixin(persons)
}



//[大佬原著][位置编号 00001]
    //以下部分代码再无作用，但是感谢之前大佬写的，不删除！
    //read.onload = function (eve) {
    //    let buffer = new Uint8Array(eve.target.result)//将二进制包转换成8位无符号整型数组
    //    let ver = readInt(buffer, 6, 2)
    //    let op = readInt(buffer, 8, 4)
    //    switch (ver) {
    //        case 0:
    //            console.log("[case = 0]----------");
    //            let body = textDecoder.decode(buffer.slice(16))
    //            if (body)
    //                console.log(JSON.parse(body));
    //            break;
    //        case 1:
    //            console.log("[case = 1]----------");
    //            if (op == 8) {
    //                console.log('认证成功开始心跳')
    //                PDJ_Send(encode('[object Object]', 2));
    //                setInterval(function () {
    //                    PDJ_Send(encode('[object Object]', 2));
    //                }, 30000);
    //            }
    //            else {
    //                let packetLen = readInt(buffer, 0, 4) - 16;
    //                let pop = readInt(buffer, 16, packetLen)
    //                console.log('气人值：' + pop)
    //                document.getElementById("popularity").innerText='气人值：'+pop
    //            }
    //            break;
    //        case 2:
    //            console.log("[case = 2]----------");
    //            let data = 0
    //            try {
    //                data = pako.inflate(buffer.slice(16))
    //                let last = data.byteLength
    //                while (last > 0) {
    //                    let packetLen = readInt(data, 0, 4)
    //                    let body = textDecoder.decode(data.slice(16, packetLen))
    //                    if (body)
    //                        jsontoprint(JSON.parse(body));
    //                    data = data.slice(packetLen)
    //                    last = last - packetLen
    //                }
    //            } catch (error) {
//
    //            }
//
//
//                break;
//            default:
//                console.log('error')
////                break;
//        }
//
//
//    }
//

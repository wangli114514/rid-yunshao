# 实验室 RID 软件回环 与 空口发射

## 软件回环（RF-disabled）

这是一个 **RF-disabled** 的协议测试模式，适合在实验室验证完整 ASTM F3411 帧和网站解析链路。

### 行为边界

- 固件使用同一套 ASTM 编码器生成 Message Pack、Beacon 和 NAN Action bytes。
- 生成的 bytes 直接交给固件内存中的解码器，再进入设备数据仓库和 USB 快照。
- 回环路径本身不调用 `esp_wifi_80211_tx`；不会发射 Wi-Fi 帧。
- 快照带 `labLoopback=true`、`simulated=true`，网关传输标识为 `usb-serial-loopback`。
- 云端仍要求 `hardwareConnected=true`，独立脚本不能把回环数据伪装成网关数据。

## 真实空口发射（frame tx / 模拟模式）

固件另有两个**真实 802.11 发射**入口，帧字节与真实 RID 广播一致：

- **模拟模式**（`sim on`）：10 架目标按各自协议（ASTM Beacon+NAN / CN 46750 Beacon）持续上空口；主页 A 键长按停发/恢复，`sim tx CH` 换信道，`sim show` 查看 txOk/txFail 计数。
- **frame tx once|on|off|status [beacon|nan] [ms]**：按当前 profile 单发或连发（pack 无 802.11 头不支持空口）。

两者与软件回环互斥；串口快照新增 `"rfTx":true/false` 字段标识发射状态。

## 串口命令

```text
frame loopback status
frame loopback once beacon
frame loopback once nan
frame loopback once pack
frame loopback on beacon 1000
frame loopback off
```

`on` 的第三个参数是间隔毫秒，固件会按当前 `frame set` 配置和 `frame set count N` 生成一批目标。每次注入都会经过回读校验；失败不会写入目标仓库。

## Windows 控制器

在仓库根目录执行：

```powershell
python lab_replay.py --port COM5 status
python lab_replay.py --port COM5 once --kind beacon
python lab_replay.py --port COM5 start --kind all --interval 1000
```

`lab-enable` 只发送一次持续回环启用命令并立即释放串口，适合随后启动 `gateway-start`；`lab-start` 则持续占用串口查看诊断，在 Ctrl+C 时发送停止命令。启动网关后，网站会显示设备产生的实验室目标，并在目标详情/事件中标注“实验室软件回环”。停止回环或拔掉设备后，实时目标按网关超时规则归零。

## 验收清单

1. `frame loopback status` 输出 `RF_TX=disabled`。
2. `frame loopback once all` 输出 pack、beacon、nan 的 `roundtrip=ok` 和 `loopback=ok`。
3. 串口快照包含 `labLoopback:true`，每个回环目标也包含同名字段。
4. `gateway.py` 上传的 `sourceTransport` 为 `usb-serial-loopback`。
5. 空口抓包工具在附近看不到由本模式产生的 Beacon/NAN 帧；这是软件回环的预期结果。

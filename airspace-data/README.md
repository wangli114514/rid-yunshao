# 空域参考数据

本目录放可公开引用的参考面，**不是** UOM 正式授权矢量，也不是飞行审批结论。

- `beijing-uom-prohibited-reference.geojson`：北京全市禁飞规则的行政区参考边界，`authoritative=false`，用于风险提示。
- `uom-derived-suitable.geojson`：由经授权的 UOM WMS 适飞栅格多边形化得到的全国适飞参考面（约 5.5MB，1718 个要素）。
- `uom-derived-suitable.manifest.json`：派生参数、图层清单、哈希和来源说明；不包含 token。

导入前请阅读清单中的 `notice` / `referenceOnly` 字段。系统首次启动可把这两份参考面种入空域库，但会持续标记为非权威数据。

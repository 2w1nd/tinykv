### 1 metapb
主要存储元信息？

#### Cluster

| 字段         | 说明                  |
|------------|---------------------|
| Id       | Cluster Id            |
| MaxPeerCount   | 一个Region拥有的Peer最大数量 |

#### Store

| 字段         | 说明                           |
|------------|------------------------------|
| Id       | Store Id                     |
| Address   | 地址                           |
| State   | Store状态。up；offline；tombstone |

#### RegionEpoch

| 字段         | 说明                                       |
|------------|------------------------------------------|
| ConfVer       | 配置更新版本，当增加或删除peer时自动变化                   |
| Version   | Region更新版本，当split region或merge region时变化 |

#### Region

| 字段         | 说明               |
|------------|------------------|
| Id       | region Id        |
| StartKey   | ket起始位置          |
| EndKey   | key结束位置          |
| RegionEpoch   | 描述当前region相关信息版本 |
| Peers   | peer集合           |

#### Peer

| 字段         | 说明       |
|------------|----------|
| Id       | peer Id  |
| StoreId   | Store Id |

### 2 raft_server

#### RaftMessage

| 字段         | 说明                    |
|------------|-----------------------|
| region_id       | region_id             |
| from_peer   | 发送peer                |
| to_peer   | 接受peer                |
| message   | Raft message          |
| region_epoch   | Region更新版本            |
| is_tombstone   | 表示to_peer已经被删除，应该关闭自己 |
| start_key   | 起始key                 |
| end_key   | 结束key                 |

#### RaftLocalState

| 字段         | 说明      |
|------------|---------|
| hard_state       | 状态      |
| last_index   | 最后index |
| last_term   | 最后term  |

#### RaftApplyState
用于存储 **Raft 状态机**的持久状态。

| 字段         | 说明                                       |
|------------|------------------------------------------|
| applied_index       | 记录状态机应用的索引，确保重启后不会两次应用任何索引。                   |
| truncated_state   | 记录最后被截断的 raft 日志的索引和期限 |

#### RaftTruncatedState
Raft 日志压缩的截断状态。

| 字段         | 说明                                       |
|------------|------------------------------------------|
| index       | 配置更新版本，当增加或删除peer时自动变化                   |
| term   | Region更新版本，当split region或merge region时变化 |

#### RegionLocalState
用于在此 Store 上存储 Region 信息和对应的 Peer 状态。

| 字段         | 说明                                       |
|------------|------------------------------------------|
| state       | 配置更新版本，当增加或删除peer时自动变化                   |
| region   | Region更新版本，当split region或merge region时变化 |


#### StoreIdent

| 字段         | 说明                                       |
|------------|------------------------------------------|
| ClusterId       | 配置更新版本，当增加或删除peer时自动变化                   |
| StoreId   | Region更新版本，当split region或merge region时变化 |


### 3 raft_cmdpb

#### GetRequest

| 字段         | 说明    |
|------------|-------|
| Cf       | 对应列   |
| Key   | 对应key |

#### GetResponse

| 字段         | 说明  |
|------------|-----|
| Value       | 值   |

#### PutRequest

| 字段         | 说明    |
|------------|-------|
| Cf       | 对应列   |
| Key   | 对应key |
| Value       | 值   |

#### PutResponse

| 字段         | 说明  |
|------------|-----|

#### DeleteRequest

| 字段         | 说明    |
|------------|-------|
| Cf       | 对应列   |
| Key   | 对应key |

#### DeleteResponse

| 字段         | 说明  |
|------------|-----|

#### SnapRequest

| 字段         | 说明    |
|------------|-------|

#### SnapResponse

| 字段     | 说明         |
|--------|------------|
| Region | 对应region信息 |

#### Request

| 字段         | 说明                              |
|------------|---------------------------------|
| CmdType       | 消息类型，get，put，delete，snap，invaid |
| Get   | Get信息                           |
| Put   | Put信息                           |
| Delete   | Delete信息                        |
| Snap   | Snapshot信息                      |

#### Response

| 字段         | 说明                            |
|------------|-------------------------------|
| CmdType       | 消息类型，get，put，delete，snap，invaid |
| Get   | Get响应信息                       |
| Put   | Put响应信息                       |
| Delete   | Delete响应信息                    |
| Snap   | Snapshot响应信息                  |

#### RaftRequestHeader

| 字段         | 说明         |
|------------|------------|
| RegionId       | Region Id  |
| Peer   | Peer信息     |
| RegionEpoch   | Region信息版本 |
| Term   | 任期         |

#### RaftCmdRequest

| 字段         | 说明           |
|------------|--------------|
| Header       | Raft Cmd 请求头 |
| Requests   | 正常请求信息       |
| AdminRequest   | 管理员请求信息      |

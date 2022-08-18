### metapb
主要存储元信息？

#### Store

| 字段         | 说明                           |
|------------|------------------------------|
| Id       | Store Id                     |
| Address   | 地址                           |
| State   | Store状态。up；offline；tombstone |

#### Cluster 

| 字段         | 说明                  |
|------------|---------------------|
| Id       | Cluster Id            |
| MaxPeerCount   | 一个Region拥有的Peer最大数量 |


#### RegionEpoch

| 字段         | 说明                                       |
|------------|------------------------------------------|
| ConfVer       | 配置更新版本，当增加或删除peer时自动变化                   |
| Version   | Region更新版本，当split region或merge region时变化 |


### raft_server

#### StoreIdent

| 字段         | 说明                                       |
|------------|------------------------------------------|
| ClusterId       | 配置更新版本，当增加或删除peer时自动变化                   |
| StoreId   | Region更新版本，当split region或merge region时变化 |



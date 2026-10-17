# ros_http_bridge

## 作用

在 HTTP 接口和 ROS 服务之间进行转换：接收上位机任务控制请求，并将巡检报告、完成状态上传到控制中心。

## 运行

先安装 Python 依赖：

```bash
pip install -r requirements.txt
```

启动节点：

```bash
rosrun ros_http_bridge http_bridge_node.py
```

HTTP 服务默认监听 `0.0.0.0:6000`。日志目录可通过私有参数 `~log_path_prefix` 配置；控制中心地址当前在源码常量中配置。

## 接口

HTTP：

- `POST /api/robot/start`：转发任务开始请求。
- `POST /api/robot/terminate`：转发任务终止请求。

ROS 服务：

- 调用 `/robot/start`、`/robot/terminate`：向巡检节点转发任务控制。
- 提供 `/robot/report/data` (`moma_service/RobotReportData`)：接收检测结果并上传。
- 提供 `/robot/complete` (`moma_service/RobotCompleted`)：接收任务汇总并上传。

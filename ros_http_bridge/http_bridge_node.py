#! /usr/bin/env python3
import datetime
import multiprocessing as mp
import signal
import sys
import json
import os
import time
import threading
import queue
import logging

from flask import Flask, request, jsonify
import requests

import rospy
from moma_service.srv import RobotStart, RobotStartRequest, RobotStartResponse
from moma_service.srv import RobotTerminate, RobotTerminateRequest, RobotTerminateResponse
from moma_service.srv import RobotReportData, RobotReportDataRequest, RobotReportDataResponse
from moma_service.srv import RobotCompleted, RobotCompletedRequest, RobotCompletedResponse

CONTROL_CENTER_IP = "10.161.202.151"
CONTROL_CENTER_PORT = 81
CONTROL_CENTER_BASE_URL = f"http://{CONTROL_CENTER_IP}:{CONTROL_CENTER_PORT}/api/third/robot"

CONTROL_CENTER_REPORT_DATA_URL = f"{CONTROL_CENTER_BASE_URL}/reportData"
CONTROL_CENTER_COMPLETE_URL = f"{CONTROL_CENTER_BASE_URL}/completed"
CONTROL_CENTER_HEARTBEAT_URL = f"{CONTROL_CENTER_BASE_URL}/heartbeat"

MAX_RETRY_TIMES = 3
RETRY_INTERVAL_SECONDS = 1
UPLOAD_TIMEOUT_SECONDS = 40
REPORT_QUEUE_MAXSIZE = 50

# 日志目录：与 patrol 保持一致，放在 ~/moma_ws/logs/ 下
LOG_PATH_PREFIX = "/home/shansu/moma_ws/logs/ros_http_bridge"


def _setup_logger(name, start_ts):
    """初始化一个写入时间戳文件的 logger（同时输出到控制台）。

    由于 bridge 用 multiprocessing spawn 起多个进程，模块级代码会在每个
    子进程里重新执行，所以这里必须在每个子进程内部单独调用，避免 handler
    被重复添加；两个子进程各自写各自的文件（flask / worker）。
    """
    try:
        log_dir = rospy.get_param('~log_path_prefix', LOG_PATH_PREFIX)
    except Exception:
        log_dir = LOG_PATH_PREFIX
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"{start_ts}_{name}.log")

    logger = logging.getLogger(f"ros_http_bridge.{name}")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    if not logger.handlers:
        fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setFormatter(fmt)
        logger.addHandler(fh)
        sh = logging.StreamHandler()
        sh.setFormatter(fmt)
        logger.addHandler(sh)
    return logger


class httpNode(object):
    def __init__(self, node_name):
        self.node_name = node_name
        self.logger = None  # 由所在子进程初始化后注入
        self._report_queue = queue.Queue(maxsize=REPORT_QUEUE_MAXSIZE)
        self._report_uploader_thread = threading.Thread(
            target=self._process_report_queue,
            name="report-uploader",
            daemon=True
        )
        self._report_uploader_thread.start()

    def _log(self, level, msg, exc_info=False):
        """写日志：已注入 logger 时写文件+控制台，否则回退为 print（保持原行为）。"""
        if self.logger is not None:
            getattr(self.logger, level)(msg, exc_info=exc_info)
        else:
            print(msg)

    def _process_report_queue(self):
        while True:
            task = self._report_queue.get()
            try:
                self._upload_report_task(task)
            except Exception as e:
                self._log("error", f"后台上传任务异常: {str(e)}")
            finally:
                self._report_queue.task_done()

    def _upload_report_task(self, task):
        file_path = task["file_path"]
        vo_data = task["vo_data"]

        if not os.path.exists(file_path) or not os.path.isfile(file_path):
            self._log("error", f"后台上传失败: 文件不可用 - {file_path}")
            return

        success = False
        for attempt in range(1, MAX_RETRY_TIMES + 1):
            with open(file_path, "rb") as raw_file:
                form_data = {
                    "rawFile": (
                        os.path.basename(file_path),
                        raw_file,
                        "application/octet-stream"
                    ),
                    "vo": (None, vo_data, "application/json")
                }
                try:
                    http_response = requests.post(
                        url=CONTROL_CENTER_REPORT_DATA_URL,
                        files=form_data,
                        timeout=UPLOAD_TIMEOUT_SECONDS
                    )
                    response_data = http_response.json()
                    success = http_response.status_code == 200 and response_data.get("code") == '0'
                    if success:
                        self._log("info", f"后台上传成功: 状态码={http_response.status_code}, 响应={response_data}")
                        break
                    self._log(
                        "warning",
                        f"后台上传失败(第{attempt}/{MAX_RETRY_TIMES}次): "
                        f"状态码={http_response.status_code}, 响应={response_data}"
                    )
                except json.JSONDecodeError:
                    self._log("warning", f"后台上传错误: 无法解析响应为JSON(第{attempt}/{MAX_RETRY_TIMES}次)")
                except requests.exceptions.RequestException as e:
                    self._log("warning", f"后台上传请求错误(第{attempt}/{MAX_RETRY_TIMES}次): {str(e)}")

            if attempt < MAX_RETRY_TIMES:
                time.sleep(RETRY_INTERVAL_SECONDS)

        if not success:
            self._log("error", f"后台上传最终失败: 文件={file_path}, 已重试{MAX_RETRY_TIMES}次")

    def _post_json_with_retry(self, url, payload, timeout=5, max_retry=MAX_RETRY_TIMES):
        """POST JSON to control center with retry; returns (success, response_data)."""
        last_response_data = None
        for attempt in range(1, max_retry + 1):
            try:
                response = requests.post(url=url, json=payload, timeout=timeout)
                response_data = response.json()
                success = response.status_code == 200 and response_data.get("code") == '0'
                if success:
                    return True, response_data
                last_response_data = response_data
                self._log(
                    "warning",
                    f"请求失败(第{attempt}/{max_retry}次): 状态码={response.status_code}, 响应={response_data}"
                )
            except json.JSONDecodeError:
                self._log("warning", f"错误: 响应不是有效JSON(第{attempt}/{max_retry}次)")
            except requests.exceptions.RequestException as e:
                self._log("warning", f"请求发生错误(第{attempt}/{max_retry}次): {str(e)}")

            if attempt < max_retry:
                time.sleep(RETRY_INTERVAL_SECONDS)

        return False, last_response_data

    def robot_report_data(self, request):
        response = RobotReportDataResponse()
        # 验证文件是否存在
        file_path = request.raw_file_path
        if not os.path.exists(file_path):
            self._log("error", f"错误: 文件不存在 - {file_path}")
            return response
        if not os.path.isfile(file_path):
            self._log("error", f"错误: 不是有效文件 - {file_path}")
            return response
        try:
            objectList = []
            self._log("info", f"object_list: {request.object_list}")
            for i in range(len(request.object_list)):
                obj = {
                    "abnormalObj": request.object_list[i].abnormal_obj,
                    "typeCode": request.object_list[i].type_code,
                    "typeName": request.object_list[i].type_name,
                    "confidence": request.object_list[i].confidence,
                    "offsetX": request.object_list[i].offset_x,
                    "offsetY": request.object_list[i].offset_y,
                    "width": request.object_list[i].width,
                    "height": request.object_list[i].height,
                    "status": request.object_list[i].status,
                    "thickness": request.object_list[i].thickness #这个是多加的
                }
                objectList.append(obj)
            vo_data = json.dumps({
                "id": request.id,
                "pointId": request.point_id,
                "analyseResult": request.analyse_result,
                "pointName": request.point_name,
                "pointType": request.point_type,
                "message": request.message,
                "timestamp": request.timestamp,
                "robotId": request.robot_id,
                "objectCount": request.object_count,
                "objectList": objectList
            })

            task = {
                "file_path": file_path,
                "vo_data": vo_data
            }
            try:
                self._report_queue.put_nowait(task)
                self._log("info", f"报告任务已入队: 文件={file_path}, 当前队列长度={self._report_queue.qsize()}")
            except queue.Full:
                self._log("warning", "报告任务入队失败: 队列已满，请检查网络或提升队列容量")
            return response
        except Exception as e:
            self._log("error", f"报告异常: {str(e)}")
            return response

    def robot_send_complete(self, request):
        response = RobotCompletedResponse()
        try:
            # 构造FormData请求体
            form_data = {
                "id": request.task_id,
                "timestamp": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "patrolResult": {
                    "totalSummary": {
                        "totalCount": request.total_summary.total_count,
                        "inspectedCount": request.total_summary.inspected_count,
                        "abnormalCount": request.total_summary.abnormal_count
                    },
                    "subcategoryList": {
                        "countVerify": {
                            "targetInspectionCount": request.count_verify.total_count,
                            "inspectedCount": request.count_verify.inspected_count,
                            "abnormalCount": request.count_verify.abnormal_count
                        },
                        "installationPosition": {
                            "targetInspectionCount": request.installation_position.total_count,
                            "inspectedCount": request.installation_position.inspected_count,
                            "abnormalCount": request.installation_position.abnormal_count
                        },
                        "physicalMeasurement": {
                            "targetInspectionCount": request.physical_measurement.total_count,
                            "inspectedCount": request.physical_measurement.inspected_count,
                            "abnormalCount": request.physical_measurement.abnormal_count
                        }
                    }
                }
            }

            self._log("info", f"complete form_data: {form_data}")

            success, response_data = self._post_json_with_retry(
                url=CONTROL_CENTER_COMPLETE_URL,
                payload=form_data,
                timeout=5,
                max_retry=MAX_RETRY_TIMES
            )

            if not success:
                self._log("error", f"报告最终失败: 已重试{MAX_RETRY_TIMES}次, 响应={response_data}")
            else:
                self._log("info", f"报告成功: 响应={response_data}")
            # return success
        except Exception as e:
            self._log("error", f"报告异常: {str(e)}")
        finally:
            return response

    def robot_send_heartbeat(self):
        try:
            form_data = {
                "id": 1,
                "timestamp": "2025-10-02 09:37:47"
            }
            success, response_data = self._post_json_with_retry(
                url=CONTROL_CENTER_HEARTBEAT_URL,
                payload=form_data,
                timeout=5,
                max_retry=MAX_RETRY_TIMES
            )

            if not success:
                self._log("error", f"报告最终失败: 已重试{MAX_RETRY_TIMES}次, 响应={response_data}")
            else:
                self._log("info", f"报告成功: 响应={response_data}")
            return success
        except Exception as e:
            self._log("error", f"请求异常: {str(e)}")
            return False


# rclpy.init()
node = httpNode('http_ros_node')
print("flask process started") # 替换了 node.get_logger().info

def run_flask(start_ts):
    rospy.init_node('http_ros_flask_node', disable_signals=True)
    logger = _setup_logger("flask", start_ts)
    logger.info("flask process started")
    app = Flask(__name__)
    # rclpy.init()
    # node = httpNode('http_node')
    # node.get_logger().info("flask process started")

    # 创建ROS服务客户端 (ROS1方式)
    clinet_start = rospy.ServiceProxy('/robot/start', RobotStart)
    client_terminate = rospy.ServiceProxy('/robot/terminate', RobotTerminate)

    @app.route('/api/robot/start', methods=['POST'])
    def robot_start():
        try:
            data = request.json
            logger.info(f"Edge server received initial info for: {data}")

            # 构建ROS服务请求
            req = RobotStartRequest()
            req.id = data.get('id')
            req.train_type_name = data.get('trainTypeName')
            req.train_number = data.get('trainNumber')
            req.verb_type = data.get('verbType')
            req.verb_type_str = data.get('verbTypeStr')
            req.repair_class_name = data.get('repairClassName')
            req.timestamp = data.get('timestamp')
            req.robot_id = data.get('robotId')
            if not req.robot_id:
                req.robot_id = 3
            req.work_mode = data.get('workMode')
            task_id = req.id

            # 等待服务可用
            try:
                rospy.wait_for_service('/robot/start', timeout=1.0)
            except rospy.ROSException:
                logger.warning('/robot/start 服务不可用，等待中...')

            # 发送ROS服务请求 (ROS1 服务调用是阻塞的)
            res = clinet_start(req)

            # 处理服务响应
            if res is not None:
                logger.info(f"/robot/start 服务响应: code={getattr(res, 'code', '-1')}, message={getattr(res, 'message', '')}")
                return jsonify({'code': getattr(res, 'code', '-1'), 'message': getattr(res, 'message', '')})
            else:
                logger.error("/robot/start 服务调用失败 (res is None)")
                return jsonify({'code': '-1', 'message': '/robot/start服务调用失败'})
        except Exception as e:
            logger.exception(f"/robot/start 处理异常: {e}")
            return jsonify({'code': '1', 'message': str(e)})

    @app.route('/api/robot/terminate', methods=['POST'])
    def robot_terminate():
        try:
            data = request.json
            logger.info(f"Edge server received initial info for session: {data}")

            # 构建ROS服务请求
            req = RobotTerminateRequest()
            req.id = data.get('id')
            req.timestamp = data.get('timestamp')

            # 等待服务可用
            try:
                rospy.wait_for_service('/robot/terminate', timeout=1.0)
            except rospy.ROSException:
                logger.warning('/robot/terminate 服务不可用，等待中...')

            # 发送ROS服务请求
            res = client_terminate(req)

            # 处理服务响应
            if res is not None:
                logger.info(f"/robot/terminate 服务响应: code={getattr(res, 'code', '-1')}, message={getattr(res, 'message', '')}")
                return jsonify({'code': getattr(res, 'code', '-1'), 'message': getattr(res, 'message', '')})
            else:
                logger.error("/robot/terminate 服务调用失败 (res is None)")
                return jsonify({'code': '-1', 'message': '/robot/terminate服务调用失败'})
        except Exception as e:
            logger.exception(f"/robot/terminate 处理异常: {e}")
            return jsonify({'code': '1', 'message': str(e)})

    # 开发环境使用内置服务器；生产建议 gunicorn/uwsgi
    app.run(host="0.0.0.0", port=6000, debug=False)


def run_ros_worker(start_ts):
    rospy.init_node('http_ros_worker_node', disable_signals=True)
    logger = _setup_logger("worker", start_ts)
    node.logger = logger
    logger.info("ros worker process started")
    # rclpy.init()
    # node = httpNode('ros_node')
    # node.get_logger().info("ros process started")
    # print("ros process started")
    # raw_path = '/home/shansu/moma_ws/records/HXD1C/item_11.png'

    server_robot_report_data = rospy.Service(
        '/robot/report/data',
        RobotReportData,
        node.robot_report_data
    )
    server_robot_complete = rospy.Service(
        '/robot/complete',
        RobotCompleted,
        node.robot_send_complete
    )

    # TODO 循环发送机器人heartbeat心跳包
    # node2.report_data(raw_path, process_path)
    # node2.get_logger().info("report_data finished!")
    # node2.send_heartbeat()
    # node2.get_logger().info("heartbeat_data sent finished!")
    # node2.send_complete()
    # node2.get_logger().info("complete_data sent finished!")

    try:
        rospy.spin()
    finally:
        pass

def main():
    mp.set_start_method("spawn", force=True)

    # 统一生成一个时间戳，两个子进程共用，保证日志文件名一致、便于对照
    start_ts = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

    http_proc = mp.Process(target=run_flask, args=(start_ts,), daemon=True)
    ros_proc = mp.Process(target=run_ros_worker, args=(start_ts,), daemon=True)

    http_proc.start()
    ros_proc.start()

    def shutdown(signum, frame):
        print("Shutting down...")
        if http_proc.is_alive():
            http_proc.terminate()
        if ros_proc.is_alive():
            ros_proc.terminate()
        http_proc.join(timeout=5)
        ros_proc.join(timeout=5)
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # 阻塞等待子进程
    try:
        http_proc.join()
        ros_proc.join()
    except KeyboardInterrupt:
        shutdown()


if __name__ == "__main__":
    main()

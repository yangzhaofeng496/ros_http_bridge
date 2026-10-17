cmake_minimum_required(VERSION 3.0.2)
project(ros_http_bridge)

find_package(catkin REQUIRED COMPONENTS
  rospy
  std_msgs
  moma_service
)

catkin_package()

# catkin_install_python(PROGRAMS
#   ros_http_bridge/http_bridge_node.py
#   DESTINATION ${CATKIN_PACKAGE_BIN_DESTINATION}
# )

install(DIRECTORY launch/
  DESTINATION ${CATKIN_PACKAGE_SHARE_DESTINATION}/launch
  FILES_MATCHING PATTERN "*.launch"
)
